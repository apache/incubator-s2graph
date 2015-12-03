package com.kakao.ml.recommendation

import com.kakao.ml.util.AsciiTable
import com.kakao.ml.{BaseDataProcessor, Data, Params}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Ascii

import scala.reflect.ClassTag

case class GraphLikeQuery(firstStepLimit: Int, secondStepLimit: Int)

case class ContinuousEvaluatorData(
    itemsForUserResults: Option[Map[String, ResultAggregator]],
    usersForItemResults: Option[Map[String, ResultAggregator]],
    itemsForItemResults: Option[Map[String, ResultAggregator]],
    usersForUserResults: Option[Map[String, ResultAggregator]]) extends Data

case class ContinuousEvaluatorParams(
    itemsForUsersQuery: Option[GraphLikeQuery],
    usersForItemsQuery: Option[GraphLikeQuery],
    itemsForItemsQuery: Option[GraphLikeQuery],
    usersForUsersQuery: Option[GraphLikeQuery]) extends Params

abstract class ContinuousEvaluator[I <: Data: ClassTag](params: ContinuousEvaluatorParams)
    extends BaseDataProcessor[I, ContinuousEvaluatorData](params) {

  val defaultEvalNs = 1 to 10
  val maxEvalN = 10

  def evaluateItemsForUsers(sqlContext: SQLContext, input: I, query: GraphLikeQuery): Map[String, ResultAggregator]

  def evaluateUsersForItems(sqlContext: SQLContext, input: I, query: GraphLikeQuery): Map[String, ResultAggregator] = ???

  def evaluateItemsForItems(sqlContext: SQLContext, input: I, query: GraphLikeQuery): Map[String, ResultAggregator] = ???

  def evaluateUsersForUsers(sqlContext: SQLContext, input: I, query: GraphLikeQuery): Map[String, ResultAggregator] = ???

  def summarize(
      trueResults: RDD[(String, Array[String])],
      trueResultsExceptColdStart: RDD[(String, Array[String])],
      predictedResults: RDD[(String, Array[String])],
      recommendableItems: Seq[String],
      recommendableItemsExceptColdStart: Seq[String]): Map[String, ResultAggregator] = {

    val maxEvalN = this.maxEvalN
    val defaultEvalNs = this.defaultEvalNs

    val performance = predictedResults.join(trueResults).join(trueResultsExceptColdStart)
        .flatMap { case (_, ((pred, trueAll), trueExceptColdStart)) =>
          /** generate random recommendation results */
          val randAll = Seq.tabulate(maxEvalN) { _ => recommendableItems(scala.util.Random.nextInt(recommendableItems.length)) }
          val randExceptColdStart = Seq.tabulate(maxEvalN) { _ =>
            recommendableItemsExceptColdStart(scala.util.Random.nextInt(recommendableItemsExceptColdStart.length))}

          defaultEvalNs
              .flatMap { n =>
                val subPred = pred.take(n)
                val subRandAll = randAll.take(n)
                val subRandExceptColdStart = randExceptColdStart.take(n)

                val averagePrecisionAll = ContinuousEvaluator.getAveragePrecision(subPred, trueAll)
                val averagePrecisionExceptColdStart = ContinuousEvaluator.getAveragePrecision(subPred, trueExceptColdStart)

                Seq(
                  s"trueAll_$n" ->
                      ResultAggregator(
                        trueAll.length,
                        subPred.length,
                        Map("predicted" -> trueAll.intersect(subPred).length,
                          "randAll" -> trueAll.intersect(subRandAll).length,
                          "randExceptColdStart" ->trueAll.intersect(subRandExceptColdStart).length),
                        averagePrecisionAll,
                        1L),
                  s"trueExceptColdStart_$n" ->
                      ResultAggregator(
                        trueExceptColdStart.length,
                        subPred.length,
                        Map("predicted" -> trueExceptColdStart.intersect(subPred).length,
                          "randAll" -> trueExceptColdStart.intersect(subRandAll).length,
                          "randExceptColdStart" -> trueExceptColdStart.intersect(subRandExceptColdStart).length),
                        averagePrecisionExceptColdStart,
                        1L)
                )
              }
        }
        .reduceByKey(_ + _)

    val results = performance.collect().sortBy(_._1).toMap

    val s = results.toSeq.sortBy(_._1).map(x => (x._1, x._2.getHeader, x._2.getRows))

    if (s.nonEmpty) {
      val header = Seq("key") ++ s.map(_._2).head
      val rows = s.flatMap(x => x._3.map(Seq(x._1) ++ _))
      AsciiTable(rows, header).show(rows.length, false)
    }

    results
  }

  final override protected def processBlock(sqlContext: SQLContext, input: I): ContinuousEvaluatorData = {
    val itemsForUsers = params.itemsForUsersQuery.map(query => evaluateItemsForUsers(sqlContext, input, query))
    val usersForItems = params.usersForItemsQuery.map(query => evaluateUsersForItems(sqlContext, input, query))
    val itemsForItems = params.itemsForItemsQuery.map(query => evaluateItemsForItems(sqlContext, input, query))
    val usersForUsers = params.usersForUsersQuery.map(query => evaluateUsersForUsers(sqlContext, input, query))
    ContinuousEvaluatorData(itemsForUsers, usersForItems, itemsForItems, usersForUsers)
  }

}

object ContinuousEvaluator {

  def getAveragePrecision(predResults: Array[String], trueResults: Array[String]): Double = {
    /** Mean Average Precision */
    var numHits = 0.0
    var averagePrecision = 0.0
    var i = 0
    while(i < predResults.length) {
      if(trueResults.contains(predResults(i))) {
        numHits += 1.0
        averagePrecision += numHits / (i + 1.0)
      }
      i += 1
    }

    if(trueResults.isEmpty)
      averagePrecision = 1.0
    else if(averagePrecision != 0.0)
      averagePrecision /= math.min(trueResults.length, predResults.length)

    averagePrecision
  }

}

