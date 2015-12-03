package com.kakao.ml.recommendation

import com.kakao.ml.Data
import com.kakao.ml.io._
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

case class ItemBasedEvaluatorData(
    sourceDF: DataFrame,
    lastTsFrom: Long,
    lastTsTo: Long,
    similarItemDF: DataFrame) extends Data

class ContinuousItemBasedEvaluator(params: ContinuousEvaluatorParams)
    extends ContinuousEvaluator[ItemBasedEvaluatorData](params) {

  /** compute predicted results
    *
    *  for evaluating using item-to-item similarity,
    *
    *   (p_1) (p_2) (p_3)
    *          /|\         <-- dense, in similarity (the second step)
    *   (p_1) (p_2) (p_3)
    *           |          <-- sparse, in history (the first step)
    *          (u)
    */
  override def evaluateItemsForUsers(
      sqlContext: SQLContext, input: ItemBasedEvaluatorData, query: GraphLikeQuery): Map[String, ResultAggregator] = {

    val sc = sqlContext.sparkContext
    import sqlContext.implicits._

    val allTrainingSet = input.sourceDF.where(tsCol <= input.lastTsTo)
    val allTestSet = input.sourceDF.where(tsCol > input.lastTsTo)

    allTrainingSet.persist(StorageLevel.MEMORY_AND_DISK)
    allTestSet.persist(StorageLevel.MEMORY_AND_DISK)

    val maxEvalN = this.maxEvalN

    val usersInTrainingSet = allTrainingSet.select(userCol).distinct().map(_.getString(0)).collect()
    val usersInTestSet = allTestSet.select(userCol).distinct().collect().map(_.getString(0))
    val testableUsers = usersInTrainingSet.intersect(usersInTestSet)

    var testableUserDF = sc.parallelize(testableUsers).toDF("user_s")
    if (testableUsers.length <= 5e5)
      testableUserDF = broadcast(testableUserDF)

    val trainingSet = allTrainingSet.join(testableUserDF, userCol === $"user_s").drop("user_s")
    val testSet = allTestSet.join(testableUserDF, userCol === $"user_s").drop("user_s")

    val similarItems = input.similarItemDF

    /** materialize */
    trainingSet.persist(StorageLevel.MEMORY_AND_DISK)
    testSet.persist(StorageLevel.MEMORY_AND_DISK)
    similarItems.persist(StorageLevel.MEMORY_AND_DISK)
    val numTrainingData = trainingSet.count()
    val numTestData = testSet.count()
    similarItems.count()

    /** free never used */
    allTrainingSet.unpersist()
    allTestSet.unpersist()

    show(s"number of users in all the training set: ${usersInTrainingSet.length}")
    show(s"number of users in all the test set: ${usersInTestSet.length}")
    show(s"number of testable users: ${testableUsers.length}")
    show(s"number of training data: $numTrainingData")
    show(s"number of test data: $numTestData")

    val firstStepLimit = query.firstStepLimit
    val secondStepLimit = query.secondStepLimit

    /** take the latest activities by user */
    val firstStep = trainingSet
        .select(tsCol, userCol, itemCol)
        .map { case Row(ts: Long, user: String, item: String) =>
          user -> (item, ts)
        }
        .topByKey(firstStepLimit)(Ordering.by(_._2))
        .mapValues(_.map(_._1).distinct)

    firstStep.persist(StorageLevel.MEMORY_AND_DISK)

    /** key by item to join */
    val firstStepKeyByItem = firstStep
        .flatMap { case (user, items) =>
          items.map(_ -> user)
        }

    val secondStep = similarItems
        .map { case Row(from: String, to: String, score: Double) =>
          from -> (to, score)
        }
        .topByKey(secondStepLimit)(Ordering.by(_._2))
        .mapValues(_.toSeq)

    val predictedResults = firstStepKeyByItem.join(secondStep)
        .flatMap { case (_, (user, itemAndScore)) =>
          itemAndScore.map { case (item, score) =>
            (user, item) -> score
          }
        }
        .reduceByKey(_ + _)
        .map { case ((user, item), score) =>
          user -> (item, score)
        }
        .groupByKey()
        .mapValues(_.toSeq.sortBy(-_._2).map(_._1))

    val predictedResultsWithoutAlreadySeen = predictedResults.join(firstStep)
        .map { case (user, (predicted, seen)) =>
          user -> predicted.diff(seen).take(maxEvalN).toArray
        }

    val trainedItems = similarItems.select(fromCol).distinct().map(_.getString(0)).collect()
    val coldStartItems = testSet.select(itemCol).distinct().map(_.getString(0)).collect().diff(trainedItems)

    val recommendableItems = trainedItems ++ coldStartItems
    val recommendableItemsWithoutColdStart = trainedItems

    val trueResults = testSet.select(userCol, itemCol)
        .map { case Row(user: String, item: String) =>
          user -> item
        }
        .groupByKey()
        .mapValues(_.toArray.distinct)

    val bcColdStartItems = sqlContext.sparkContext.broadcast(coldStartItems)
    val trueResultsWithoutColdStart = trueResults.mapValues(_.diff(bcColdStartItems.value))

    summarize(
      trueResults,
      trueResultsWithoutColdStart,
      predictedResultsWithoutAlreadySeen,
      recommendableItems,
      recommendableItemsWithoutColdStart)
  }

}
