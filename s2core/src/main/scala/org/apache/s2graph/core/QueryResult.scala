/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core

import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs}
import org.apache.s2graph.core.utils.logger

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

object QueryResult {
  def fromVertices(query: Query): StepInnerResult = {
    if (query.steps.isEmpty || query.steps.head.queryParams.isEmpty) {
      StepInnerResult.Empty
    } else {
      val queryParam = query.steps.head.queryParams.head
      val label = queryParam.label
      val currentTs = System.currentTimeMillis()
      val propsWithTs = Map(LabelMeta.timeStampSeq ->
        InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs))
      val edgeWithScoreLs = for {
        vertex <- query.vertices
      } yield {
          val edge = Edge(vertex, vertex, queryParam.labelWithDir, propsWithTs = propsWithTs)
          val edgeWithScore = EdgeWithScore(edge, Graph.DefaultScore)
          edgeWithScore
        }
      StepInnerResult(edgesWithScoreLs = edgeWithScoreLs, Nil, false)
    }
  }
}
/** inner traverse */
object StepInnerResult {
  val Failure = StepInnerResult(Nil, Nil, true)
  val Empty = StepInnerResult(Nil, Nil, false)
}
case class StepInnerResult(edgesWithScoreLs: Seq[EdgeWithScore],
                           degreeEdges: Seq[EdgeWithScore],
                           isFailure: Boolean = false) {
  val isEmpty = edgesWithScoreLs.isEmpty
}

case class QueryRequest(query: Query,
                        stepIdx: Int,
                        vertex: Vertex,
                        queryParam: QueryParam)


case class QueryResult(edgeWithScoreLs: Seq[EdgeWithScore] = Nil,
                       tailCursor: Array[Byte] = Array.empty,
                       timestamp: Long = System.currentTimeMillis(),
                       isFailure: Boolean = false)

case class EdgeWithScore(edge: Edge, score: Double)





/** result */

object StepResult {

  type Values = Seq[S2EdgeWithScore]
  type GroupByKey = Seq[Option[Any]]
  val EmptyOrderByValues = (None, None, None, None)
  val Empty = StepResult(Nil, Nil, Nil)


  def mergeOrdered(left: StepResult.Values,
                   right: StepResult.Values,
                   queryOption: QueryOption): (Double, StepResult.Values) = {
    val merged = (left ++ right)
    val scoreSum = merged.foldLeft(0.0) { case (prev, current) => prev + current.score }
    if (scoreSum < queryOption.scoreThreshold) (0.0, Nil)
    else {
      val ordered = orderBy(queryOption, merged)
      val filtered = ordered.take(queryOption.groupBy.limit)
      val newScoreSum = filtered.foldLeft(0.0) { case (prev, current) => prev + current.score }
      (newScoreSum, filtered)
    }
  }

  def orderBy(queryOption: QueryOption, notOrdered: Values): Values = {
    import OrderingUtil._

    if (queryOption.withScore && queryOption.orderByColumns.nonEmpty) {
      notOrdered.sortBy(_.orderByValues)(TupleMultiOrdering[Any](queryOption.ascendingVals))
    } else {
      notOrdered
    }
  }
  def toOrderByValues(s2Edge: Edge,
                      score: Double,
                      orderByKeys: Seq[String]): (Any, Any, Any, Any) = {
    def toValue(propertyKey: String): Any = {
      propertyKey match {
        case "score" => score
        case "timestamp" | "_timestamp" => s2Edge.ts
        case _ => s2Edge.properties.get(propertyKey)
      }
    }
    if (orderByKeys.isEmpty) (None, None, None, None)
    else {
      orderByKeys.length match {
        case 1 =>
          (toValue(orderByKeys(0)), None, None, None)
        case 2 =>
          (toValue(orderByKeys(0)), toValue(orderByKeys(1)), None, None)
        case 3 =>
          (toValue(orderByKeys(0)), toValue(orderByKeys(1)), toValue(orderByKeys(2)), None)
        case _ =>
          (toValue(orderByKeys(0)), toValue(orderByKeys(1)), toValue(orderByKeys(2)), toValue(orderByKeys(3)))
      }
    }
  }
  /**
   * merge multiple StepResult into one StepResult.
   * @param queryOption
   * @param multiStepResults
   * @return
   */
  def merges(queryOption: QueryOption,
             multiStepResults: Seq[StepResult],
             weights: Seq[Double] = Nil): StepResult = {
    val degrees = multiStepResults.flatMap(_.degreeEdges)
    val ls = new mutable.ListBuffer[S2EdgeWithScore]()
    val agg= new mutable.HashMap[GroupByKey, ListBuffer[S2EdgeWithScore]]()
    val sums = new mutable.HashMap[GroupByKey, Double]()

    for {
      (weight, eachStepResult) <- weights.zip(multiStepResults)
      (ordered, grouped) = (eachStepResult.results, eachStepResult.grouped)
    } {
      ordered.foreach { t =>
        val newScore = t.score * weight
        ls += t.copy(score = newScore)
      }

      // process each query's stepResult's grouped
      for {
        (groupByKey, (scoreSum, values)) <- grouped
      } {
        val buffer = agg.getOrElseUpdate(groupByKey, ListBuffer.empty[S2EdgeWithScore])
        var scoreSum = 0.0
        values.foreach { t =>
          val newScore = t.score * weight
          buffer += t.copy(score = newScore)
          scoreSum += newScore
        }
        sums += (groupByKey -> scoreSum)
      }
    }

    // process global groupBy
    if (queryOption.groupBy.keys.nonEmpty) {
      for {
        s2EdgeWithScore <- ls
        groupByKey = s2EdgeWithScore.s2Edge.selectValues(queryOption.groupBy.keys)
      } {
        val buffer = agg.getOrElseUpdate(groupByKey, ListBuffer.empty[S2EdgeWithScore])
        buffer += s2EdgeWithScore
        val newScore = sums.getOrElse(groupByKey, 0.0) + s2EdgeWithScore.score
        sums += (groupByKey -> newScore)
      }
    }


    val ordered = orderBy(queryOption, ls)
    val grouped = for {
      (groupByKey, scoreSum) <- sums.toSeq.sortBy(_._2 * -1)
      aggregated = agg(groupByKey) if aggregated.nonEmpty
    } yield groupByKey -> (scoreSum, aggregated)

    StepResult(results = ordered, grouped = grouped, degrees)
  }

  //TODO: Optimize this.
  def filterOut(graph: Graph,
                queryOption: QueryOption,
                baseStepResult: StepResult,
                filterOutStepInnerResult: StepInnerResult): StepResult = {

    val fields = if (queryOption.filterOutFields.isEmpty) Seq("to") else Seq("to")
    //    else queryOption.filterOutFields
    val filterOutSet = filterOutStepInnerResult.edgesWithScoreLs.map { t =>
      t.edge.selectValues(fields)
    }.toSet

    val filteredResults = baseStepResult.results.filter { t =>
      val filterOutKey = t.s2Edge.selectValues(fields)
      !filterOutSet.contains(filterOutKey)
    }

    val grouped = for {
      (key, (scoreSum, values)) <- baseStepResult.grouped
      (out, in) = values.partition(v => filterOutSet.contains(v.s2Edge.selectValues(fields)))
      newScoreSum = scoreSum - out.foldLeft(0.0) { case (prev, current) => prev + current.score } if in.nonEmpty
    } yield key -> (newScoreSum, in)


    StepResult(results = filteredResults, grouped = grouped, baseStepResult.degreeEdges)
  }
  def apply(graph: Graph,
            queryOption: QueryOption,
            stepInnerResult: StepInnerResult): StepResult = {
        logger.debug(s"[BeforePostProcess]: ${stepInnerResult.edgesWithScoreLs.size}")

    val results = for {
      edgeWithScore <- stepInnerResult.edgesWithScoreLs
    } yield {
        val edge = edgeWithScore.edge
        val orderByValues =
          if (queryOption.orderByColumns.isEmpty) (edgeWithScore.score, None, None, None)
          else toOrderByValues(edge, edgeWithScore.score, queryOption.orderByKeys)

        S2EdgeWithScore(edge, edgeWithScore.score, orderByValues, edgeWithScore.edge.parentEdges)
      }
    /** ordered flatten result */
    val ordered = orderBy(queryOption, results)

    /** ordered grouped result */
    val grouped =
      if (queryOption.groupBy.keys.isEmpty) Nil
      else {
        val agg = new mutable.HashMap[GroupByKey, (Double, Values)]()
        results.groupBy { s2EdgeWithScore =>
          s2EdgeWithScore.s2Edge.selectValues(queryOption.groupBy.keys, useToString = true)
        }.map { case (k, ls) =>
          val (scoreSum, merged) = mergeOrdered(ls, Nil, queryOption)
          /**
           * watch out here. by calling toString on Any, we lose type information which will be used
           * later for toJson.
           */
          if (merged.nonEmpty) {
            val newKey = merged.head.s2Edge.selectValues(queryOption.groupBy.keys, useToString = false)
            agg += (newKey -> (scoreSum, merged))
          }
        }
        agg.toSeq.sortBy(_._2._1 * -1)
      }

    val degrees = stepInnerResult.degreeEdges.map(t => S2EdgeWithScore(t.edge, t.score))
    StepResult(results = ordered, grouped = grouped, degreeEdges = degrees)
  }
}

case class S2EdgeWithScore(s2Edge: Edge,
                           score: Double,
                           orderByValues: (Any, Any, Any, Any) = StepResult.EmptyOrderByValues,
                           parentEdges: Seq[EdgeWithScore] = Nil)

case class StepResult(results: StepResult.Values,
                      grouped: Seq[(StepResult.GroupByKey, (Double, StepResult.Values))],
                      degreeEdges: StepResult.Values) {
  val isEmpty = results.isEmpty
}