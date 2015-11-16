package com.kakao.ml.recommendation

import com.kakao.ml.Data
import com.kakao.ml.io._
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

case class UserBasedEvaluatorData(
    sourceDF: DataFrame,
    lastTsFrom: Long,
    lastTsTo: Long,
    similarUserDF: DataFrame) extends Data

class ContinuousUserBasedEvaluator(params: ContinuousEvaluatorParams)
    extends ContinuousEvaluator[UserBasedEvaluatorData](params) {

  /** compute predicted results
    *
    * for evaluating using user-to-user similarity,
    *
    *     (p_1) (p_2) (p_3)
    *         \        /            <-- sparse, in history
    *  (u_1) (u_2) (u_3) (u_4)
    *            \ /                <-- dense, in similarity
    *            (u)
    */
  override def evaluateItemsForUsers(
      sqlContext: SQLContext, input: UserBasedEvaluatorData, query: GraphLikeQuery): Map[String, ResultAggregator] = {

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

    val similarUsers = input.similarUserDF

    /** materialize */
    trainingSet.persist(StorageLevel.MEMORY_AND_DISK)
    testSet.persist(StorageLevel.MEMORY_AND_DISK)
    similarUsers.persist(StorageLevel.MEMORY_AND_DISK)
    val numTrainingData = trainingSet.count()
    val numTestData = testSet.count()
    similarUsers.count()

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

    val firstStep = similarUsers
        .map { case Row(from: String, to: String, score: Double) =>
          to -> (from, score)
        }
        .topByKey(firstStepLimit)(Ordering.by(_._2))

    /** take the latest activities by user */
    val secondStep = trainingSet
        .select(tsCol, userCol, itemCol)
        .map { case Row(ts: Long, user: String, item: String) =>
          user -> (item, ts)
        }
        .topByKey(secondStepLimit)(Ordering.by(_._2))
        .flatMap { case (user, iterable) =>
          iterable.map { case (item, ts) =>
            user -> item
          }
        }

    secondStep.persist(StorageLevel.MEMORY_AND_DISK)

    val predictedResults = firstStep.join(secondStep)
        .flatMap { case (_, (userWithScore, item)) =>
          userWithScore.map { case (user, score) =>
            (user, item) -> score
          }
        }
        .reduceByKey(_ + _)
        .map { case ((user, item), score) =>
          user -> (item, score)
        }
        .groupByKey()
        .mapValues(_.toSeq.sortBy(-_._2).map(_._1))

    val predictedResultsWithoutAlreadySeen = predictedResults.join(secondStep.groupByKey())
        .map { case (user, (predicted, seen)) =>
          user -> predicted.diff(seen.toSeq).take(maxEvalN).toArray
        }

    val trainedItems = secondStep.map(_._2).collect()
    val coldStartItems = Array.empty[String]

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
