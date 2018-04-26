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

import org.apache.s2graph.core.schema.{Label, LabelMeta}
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs, VertexId}
import org.apache.s2graph.core.utils.logger

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Seq, mutable}

object QueryResult {
  def fromVertices(graph: S2GraphLike, vertices: Seq[S2VertexLike], queryParams: Seq[QueryParam]): StepResult = {
    val edgeWithScores = vertices.flatMap { vertex =>
      queryParams.map { queryParam =>
        val label = queryParam.label
        val currentTs = System.currentTimeMillis()
        val propsWithTs = Map(LabelMeta.timestamp ->
          InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs))

        val edge = graph.elementBuilder.newEdge(vertex, vertex, label, queryParam.labelWithDir.dir, propsWithTs = propsWithTs)
        val edgeWithScore = EdgeWithScore(edge, S2Graph.DefaultScore, queryParam.label)
        edgeWithScore

      }
    }
    StepResult(edgeWithScores = edgeWithScores, grouped = Nil, degreeEdges = Nil, false)
  }

  def fromVertices(graph: S2GraphLike,
                   query: Query): StepResult = {
    if (query.steps.isEmpty || query.steps.head.queryParams.isEmpty) {
      StepResult.Empty
    } else {
      fromVertices(graph, query.vertices, query.steps.head.queryParams)
//      val queryParam = query.steps.head.queryParams.head
//      val label = queryParam.label
//      val currentTs = System.currentTimeMillis()
//      val propsWithTs = Map(LabelMeta.timestamp ->
//        InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs))
//      val edgeWithScores = for {
//        vertex <- query.vertices
//      } yield {
//          val edge = graph.newEdge(vertex, vertex, label, queryParam.labelWithDir.dir, propsWithTs = propsWithTs)
//          val edgeWithScore = EdgeWithScore(edge, S2Graph.DefaultScore, queryParam.label)
//          edgeWithScore
//        }
//      StepResult(edgeWithScores = edgeWithScores, grouped = Nil, degreeEdges = Nil, false)
    }
  }
}

case class QueryRequest(query: Query,
                        stepIdx: Int,
                        vertex: S2VertexLike,
                        queryParam: QueryParam,
                        prevStepScore: Double = 1.0,
                        labelWeight: Double = 1.0) {
  val nextStepOpt =
    if (stepIdx < query.steps.size - 1) Option(query.steps(stepIdx + 1))
    else None
}

trait WithScore[T] {
  def score(me: T): Double
  def withNewScore(me: T, newScore: Double): T
}

object WithScore {
  implicit val impEdgeWithScore = new WithScore[EdgeWithScore] {
    override def score(me: EdgeWithScore): Double = me.score

    override def withNewScore(me: EdgeWithScore, newScore: Double): EdgeWithScore = me.copy(score = newScore)
  }
}

case class EdgeWithScore(edge: S2EdgeLike,
                         score: Double,
                         label: Label,
                         orderByValues: (Any, Any, Any, Any) = StepResult.EmptyOrderByValues,
                         groupByValues: Seq[Option[Any]] = Nil,
                         stepGroupByValues: Seq[Option[Any]] = Nil,
                         filterOutValues: Seq[Option[Any]] = Nil,
                         accumulatedScores: Map[String, Double] = Map.empty) {

  def toValue(keyName: String): Option[Any] = keyName match {
    case "score" => Option(score)
    case _ => edge.propertyValue(keyName).map(_.innerVal.value)
  }

  def toValues(keyNames: Seq[String]): Seq[Option[Any]] = for {
    keyName <- keyNames
  } yield toValue(keyName)

}

/** result */
case class StepResult(edgeWithScores: Seq[EdgeWithScore],
                      grouped: Seq[(StepResult.GroupByKey, (Double, StepResult.Values))],
                      degreeEdges: Seq[EdgeWithScore],
                      isFailure: Boolean = false,
                      accumulatedCursors: Seq[Seq[Array[Byte]]] = Nil,
                      cursors: Seq[Array[Byte]] = Nil,
                      failCount: Int = 0) {
  //  val isInnerEmpty = innerResults.isEmpty
  val isEmpty = edgeWithScores.isEmpty
}

object StepResult {

  type Values = Seq[EdgeWithScore]
  type GroupByKey = Seq[Option[Any]]
  val EmptyOrderByValues = (None, None, None, None)
  val Empty = StepResult(Nil, Nil, Nil)
  val Failure = StepResult(Nil, Nil, Nil, true, failCount = 1)

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

  def filterOutStepGroupBy(edgesWithScores: Seq[EdgeWithScore],
                                   groupBy: GroupBy): Seq[EdgeWithScore] =
    if (groupBy == GroupBy.Empty) edgesWithScores
    else {
      groupBy.minShouldMatch match {
        case None => edgesWithScores
        case Some(minShouldMatch) =>
          val MinShouldMatchParam(propKey, count, terms) = minShouldMatch

          val grouped = edgesWithScores.groupBy { edgeWithScore =>
            edgeWithScore.stepGroupByValues
          }.filter { case (key, edges) =>
            val filtered = edges.toStream.filter{ e =>
              e.toValue(propKey) match {
                case None => false
                case Some(v) => terms.contains(v)
              }
            }.take(count)

            filtered.lengthCompare(count) >= 0
          }

          grouped.values.flatten.toSeq
      }
    }

  def orderBy(queryOption: QueryOption, notOrdered: Values): Values = {
    import OrderingUtil._
    if (queryOption.withScore) {
      val ascendingVals = if (queryOption.ascendingVals.isEmpty) QueryOption.DefaultAscendingVals else queryOption.ascendingVals
      notOrdered.sortBy(_.orderByValues)(TupleMultiOrdering[Any](ascendingVals))
    } else {
      notOrdered
    }
  }

  def updateScoreOnOrderByValues(scoreFieldIndex: Int,
                                 orderByValues: (Any, Any, Any, Any),
                                 newScore: Double): (Any, Any, Any, Any) = {
    scoreFieldIndex match {
      case 0 => (newScore, orderByValues._2, orderByValues._3, orderByValues._4)
      case 1 => (orderByValues._1, newScore, orderByValues._3, orderByValues._4)
      case 2 => (orderByValues._1, orderByValues._2, newScore, orderByValues._4)
      case 3 => (orderByValues._1, orderByValues._2, orderByValues._3, newScore)
      case _ => orderByValues
    }

  }

  def toTuple4(values: Seq[Option[Any]]): (Any, Any, Any, Any) = {
    values.length match {
      case 1 => (values(0).getOrElse(None), None, None, None)
      case 2 => (values(0).getOrElse(None), values(1).getOrElse(None), None, None)
      case 3 => (values(0).getOrElse(None), values(1).getOrElse(None), values(2).getOrElse(None), None)
      case _ => (values(0).getOrElse(None), values(1).getOrElse(None), values(2).getOrElse(None), values(3).getOrElse(None))
    }
  }

  /**
   * merge multiple StepResult into one StepResult.
   * @param globalQueryOption
   * @param multiStepResults
   * @param weights
   * @param filterOutStepResult
   * @return
   */
  def merges(globalQueryOption: QueryOption,
             multiStepResults: Seq[StepResult],
             weights: Seq[Double] = Nil,
             filterOutStepResult: StepResult): StepResult = {
    val degrees = multiStepResults.flatMap(_.degreeEdges)
    val ls = new mutable.ListBuffer[EdgeWithScore]()
    val agg= new mutable.HashMap[GroupByKey, ListBuffer[EdgeWithScore]]()
    val sums = new mutable.HashMap[GroupByKey, Double]()


    val filterOutSet = filterOutStepResult.edgeWithScores.foldLeft(Set.empty[Seq[Option[Any]]]) { case (prev, t) =>
      prev + t.filterOutValues
    }

    for {
      (weight, eachStepResult) <- weights.zip(multiStepResults)
      (ordered, grouped) = (eachStepResult.edgeWithScores, eachStepResult.grouped)
    } {
      ordered.foreach { t =>
        val filterOutKey = t.filterOutValues
        if (!filterOutSet.contains(filterOutKey)) {
          val newScore = t.score * weight
          val newT = t.copy(score = newScore)

          //          val newOrderByValues = updateScoreOnOrderByValues(globalQueryOption.scoreFieldIdx, t.orderByValues, newScore)
          val newOrderByValues =
            if (globalQueryOption.orderByKeys.isEmpty) (newScore, t.edge.getTs(), None, None)
            else toTuple4(newT.toValues(globalQueryOption.orderByKeys))

          val newGroupByValues = newT.toValues(globalQueryOption.groupBy.keys)

          ls += t.copy(score = newScore, orderByValues = newOrderByValues, groupByValues = newGroupByValues)
        }
      }

      // process each query's stepResult's grouped
      for {
        (groupByKey, (scoreSum, values)) <- grouped
      } {
        val buffer = agg.getOrElseUpdate(groupByKey, ListBuffer.empty[EdgeWithScore])
        var scoreSum = 0.0
        var isEmpty = true
        values.foreach { t =>
          val filterOutKey = t.filterOutValues
          if (!filterOutSet.contains(filterOutKey)) {
            isEmpty = false
            val newScore = t.score * weight
            val newT = t.copy(score = newScore)
//            val newOrderByValues = updateScoreOnOrderByValues(globalQueryOption.scoreFieldIdx, t.orderByValues, newScore)

            val newOrderByValues =
              if (globalQueryOption.orderByKeys.isEmpty) (newScore, t.edge.getTs(), None, None)
              else toTuple4(newT.toValues(globalQueryOption.orderByKeys))

            val newGroupByValues = newT.toValues(globalQueryOption.groupBy.keys)

            buffer += t.copy(score = newScore, orderByValues = newOrderByValues, groupByValues = newGroupByValues)
            scoreSum += newScore
          }
        }
        if (!isEmpty) sums += (groupByKey -> scoreSum)
      }
    }

    // process global groupBy
    val (ordered, grouped) = if (globalQueryOption.groupBy.keys.nonEmpty) {
      for {
        edgeWithScore <- ls
        groupByKey = edgeWithScore.groupByValues
      } {
        val buffer = agg.getOrElseUpdate(groupByKey, ListBuffer.empty[EdgeWithScore])
        buffer += edgeWithScore
        val newScore = sums.getOrElse(groupByKey, 0.0) + edgeWithScore.score
        sums += (groupByKey -> newScore)
      }
      val grouped = for {
        (groupByKey, scoreSum) <- sums.toSeq.sortBy(_._2 * -1)
        aggregated = agg(groupByKey) if aggregated.nonEmpty
        sorted = orderBy(globalQueryOption, aggregated)
      } yield (groupByKey, (scoreSum, sorted))
      (Nil, grouped)
    } else {
      val ordered = orderBy(globalQueryOption, ls)
      (ordered, Nil)
    }

    StepResult(edgeWithScores = ordered, grouped = grouped, degrees, failCount = multiStepResults.map(_.failCount).sum)
  }

  //TODO: Optimize this.
  def filterOut(graph: S2GraphLike,
                queryOption: QueryOption,
                baseStepResult: StepResult,
                filterOutStepResult: StepResult): StepResult = {

    val filterOutSet = filterOutStepResult.edgeWithScores.foldLeft(Set.empty[Seq[Option[Any]]]) { case (prev, t) =>
      prev + t.filterOutValues
    }

    val filteredResults = baseStepResult.edgeWithScores.filter { t =>
      val filterOutKey = t.filterOutValues
      !filterOutSet.contains(filterOutKey)
    }

    val grouped = for {
      (key, (scoreSum, values)) <- baseStepResult.grouped
      (out, in) = values.partition(v => filterOutSet.contains(v.filterOutValues))
      newScoreSum = scoreSum - out.foldLeft(0.0) { case (prev, current) => prev + current.score } if in.nonEmpty
    } yield (key, (newScoreSum, in))

    StepResult(edgeWithScores = filteredResults, grouped = grouped, baseStepResult.degreeEdges, cursors = baseStepResult.cursors, failCount = baseStepResult.failCount + filterOutStepResult.failCount)
  }
}
