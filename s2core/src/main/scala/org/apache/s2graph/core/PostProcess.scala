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

import java.util.Base64

import com.google.protobuf.ByteString
import org.apache.s2graph.core.GraphExceptions.{BadQueryException, LabelNotExistException}
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.S2Graph.{FilterHashKey, HashKey}
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.{Json, _}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.Future
import scala.util.Random

object PostProcess {


  type EDGE_VALUES = Map[String, JsValue]
  type ORDER_BY_VALUES =  (Any, Any, Any, Any)
  type RAW_EDGE = (EDGE_VALUES, Double, ORDER_BY_VALUES)
  type GROUP_BY_KEY = Map[String, JsValue]

  /**
   * Result Entity score field name
   */
  val emptyDegrees = Seq.empty[JsValue]
  val emptyResults = Json.obj("size" -> 0, "degrees" -> Json.arr(), "results" -> Json.arr(), "isEmpty" -> true, "rpcFail" -> 0)

  def badRequestResults(ex: => Exception) = ex match {
    case ex: BadQueryException => Json.obj("message" -> ex.msg)
    case _ => Json.obj("message" -> ex.getMessage)
  }

  def s2EdgeParent(graph: S2Graph,
                   queryOption: QueryOption,
                   parentEdges: Seq[EdgeWithScore]): JsValue = {
    if (parentEdges.isEmpty) JsNull
    else {
      val ancestors = for {
        current <- parentEdges
        parents = s2EdgeParent(graph, queryOption, current.edge.parentEdges) if parents != JsNull
      } yield {
          val s2Edge = current.edge.originalEdgeOpt.getOrElse(current.edge)
          s2EdgeToJsValue(queryOption, current.copy(edge = s2Edge), false, parents = parents, checkSelectColumns = true)
        }
      Json.toJson(ancestors)
    }
  }

  def s2EdgeToJsValue(queryOption: QueryOption,
                      edgeWithScore: EdgeWithScore,
                      isDegree: Boolean = false,
                      parents: JsValue = JsNull,
                      checkSelectColumns: Boolean = false): JsValue = {
    //    val builder = immutable.Map.newBuilder[String, JsValue]
    val builder = ArrayBuffer.empty[(String, JsValue)]
    val s2Edge = edgeWithScore.edge
    val score = edgeWithScore.score
    val label = edgeWithScore.label
    if (isDegree) {
      builder += ("from" -> anyValToJsValue(s2Edge.srcId).get)
      builder += ("label" -> anyValToJsValue(label.label).get)
      builder += ("direction" -> anyValToJsValue(s2Edge.direction).get)
      builder += (LabelMeta.degree.name -> anyValToJsValue(s2Edge.propertyValueInner(LabelMeta.degree).innerVal.value).get)
      JsObject(builder)
    } else {
      if (queryOption.withScore) builder += ("score" -> anyValToJsValue(score).get)

      if (queryOption.selectColumns.isEmpty) {
        builder += ("from" -> anyValToJsValue(s2Edge.srcId).get)
        builder += ("to" -> anyValToJsValue(s2Edge.tgtId).get)
        builder += ("label" -> anyValToJsValue(label.label).get)

        val innerProps = ArrayBuffer.empty[(String, JsValue)]
        for {
          (labelMeta, v) <- edgeWithScore.edge.propertyValues()
          jsValue <- anyValToJsValue(v.innerVal.value)
        } {
          innerProps += (labelMeta.name -> jsValue)
        }


        builder += ("props" -> JsObject(innerProps))
        builder += ("direction" -> anyValToJsValue(s2Edge.direction).get)
        builder += ("timestamp" -> anyValToJsValue(s2Edge.tsInnerVal).get)
        builder += ("_timestamp" -> anyValToJsValue(s2Edge.tsInnerVal).get) // backward compatibility
        if (parents != JsNull) builder += ("parents" -> parents)
        //          Json.toJson(builder.result())
        JsObject(builder)
      } else {
        queryOption.selectColumnsMap.foreach { case (columnName, _) =>
          columnName match {
            case "from" => builder += ("from" -> anyValToJsValue(s2Edge.srcId).get)
            case "_from" => builder += ("_from" -> anyValToJsValue(s2Edge.srcId).get)
            case "to" => builder += ("to" -> anyValToJsValue(s2Edge.tgtId).get)
            case "_to" => builder += ("_to" -> anyValToJsValue(s2Edge.tgtId).get)
            case "label" => builder += ("label" -> anyValToJsValue(label.label).get)
            case "direction" => builder += ("direction" -> anyValToJsValue(s2Edge.direction).get)
            case "timestamp" => builder += ("timestamp" -> anyValToJsValue(s2Edge.tsInnerVal).get)
            case "_timestamp" => builder += ("_timestamp" -> anyValToJsValue(s2Edge.tsInnerVal).get)
            case _ => // should not happen

          }
        }
        val innerProps = ArrayBuffer.empty[(String, JsValue)]
        for {
          (selectColumnName, _) <- queryOption.selectColumnsMap
          labelMeta <- label.metaPropsInvMap.get(selectColumnName)
          innerValWithTs = edgeWithScore.edge.propertyValueInner(labelMeta)
          jsValue <- anyValToJsValue(innerValWithTs.innerVal.value)
        } {
          innerProps += (labelMeta.name -> jsValue)
        }

        builder += ("props" -> JsObject(innerProps))
        if (parents != JsNull) builder += ("parents" -> parents)
        JsObject(builder)
      }
    }
  }

  def s2VertexToJson(s2Vertex: S2VertexLike): Option[JsValue] = {
    val props = for {
      (k, v) <- s2Vertex.properties
      jsVal <- anyValToJsValue(v)
    } yield k -> jsVal

    for {
      id <- anyValToJsValue(s2Vertex.innerIdVal)
    } yield {
      Json.obj(
        "serviceName" -> s2Vertex.serviceName,
        "columnName" -> s2Vertex.columnName,
        "id" -> id,
        "props" -> Json.toJson(props),
        "timestamp" -> s2Vertex.ts
      )
    }
  }

  def verticesToJson(s2Vertices: Seq[S2VertexLike]): JsValue =
    Json.toJson(s2Vertices.flatMap(s2VertexToJson(_)))

  def withOptionalFields(queryOption: QueryOption,
                         size: Int,
                         degrees: Seq[JsValue],
                         results: Seq[JsValue],
                         failCount: Int = 0,
                         cursors: => JsValue,
                         nextQuery: => Option[JsValue]): JsValue = {

    val kvs = new ArrayBuffer[(String, JsValue)]()


    kvs.append("size" -> JsNumber(size))
    kvs.append("degrees" -> JsArray(degrees))
    kvs.append("results" -> JsArray(results))

    if (queryOption.impIdOpt.isDefined) kvs.append(Experiment.ImpressionKey -> JsString(queryOption.impIdOpt.get))

    JsObject(kvs)
  }

  def buildJsonWith(js: JsValue)(implicit fn: (String, JsValue) => JsValue): JsValue = js match {
    case JsObject(obj) => JsObject(obj.map { case (k, v) => k -> buildJsonWith(fn(k, v)) })
    case JsArray(arr) => JsArray(arr.map(buildJsonWith(_)))
    case _ => js
  }

  def toJson(orgQuery: Option[JsValue])(graph: S2Graph,
                                        queryOption: QueryOption,
                                        stepResult: StepResult): JsValue = {

    // [[cursor, cursor], [cursor]]
    lazy val cursors: Seq[Seq[String]] = stepResult.accumulatedCursors.map { stepCursors =>
      stepCursors.map { cursor => new String(Base64.getEncoder.encode(cursor)) }
    }

    lazy val cursorJson: JsValue = Json.toJson(cursors)

    // build nextQuery with (original query + cursors)
    lazy val nextQuery: Option[JsValue] = {
      if (cursors.exists { stepCursors => stepCursors.exists(_ != "") }) {
        val cursorIter = cursors.iterator

        orgQuery.map { query =>
          buildJsonWith(query) { (key, js) =>
            if (key == "step") {
              val currentCursor = cursorIter.next
              val res = js.as[Seq[JsObject]].toStream.zip(currentCursor).filterNot(_._2 == "").map { case (obj, cursor) =>
                val label = (obj \ "label").as[String]
                if (Label.findByName(label).get.schemaVersion == "v4") obj + ("cursor" -> JsString(cursor))
                else {
                  val limit = (obj \ "limit").asOpt[Int].getOrElse(RequestParser.defaultLimit)
                  val offset = (obj \ "offset").asOpt[Int].getOrElse(0)
                  obj + ("offset" -> JsNumber(offset + limit))
                }
              }

              JsArray(res)
            } else js
          }
        }
      } else Option(JsNull)
    }

    val limitOpt = queryOption.limitOpt
    val selectColumns = queryOption.selectColumnsMap
    val degrees =
      if (queryOption.returnDegree) stepResult.degreeEdges.map(t => s2EdgeToJsValue(queryOption, t, true, JsNull))
      else emptyDegrees

    if (queryOption.groupBy.keys.isEmpty) {
      // no group by specified on query.
      val results = if (limitOpt.isDefined) stepResult.edgeWithScores.take(limitOpt.get) else stepResult.edgeWithScores
      val ls = results.map { t =>
        val parents = if (queryOption.returnTree) s2EdgeParent(graph, queryOption, t.edge.parentEdges) else JsNull

        s2EdgeToJsValue(queryOption, t, false, parents)
      }

      withOptionalFields(queryOption, ls.size, degrees, ls, stepResult.failCount, cursorJson, nextQuery)
    } else {

      val grouped = if (limitOpt.isDefined) stepResult.grouped.take(limitOpt.get) else stepResult.grouped
      val results =
        for {
          (groupByValues, (scoreSum, edges)) <- grouped
        } yield {
          val groupByKeyValues = queryOption.groupBy.keys.zip(groupByValues).map { case (k, valueOpt) =>
            k -> valueOpt.flatMap(anyValToJsValue).getOrElse(JsNull)
          }
          val groupByValuesJson = Json.toJson(groupByKeyValues.toMap)

          if (!queryOption.returnAgg) {
            Json.obj(
              "groupBy" -> groupByValuesJson,
              "scoreSum" -> scoreSum,
              "agg" -> Json.arr()
            )
          } else {
            val agg = edges.map { t =>
              val parents = if (queryOption.returnTree) s2EdgeParent(graph, queryOption, t.edge.parentEdges) else JsNull
              s2EdgeToJsValue(queryOption, t, false, parents)
            }
            val aggJson = Json.toJson(agg)
            Json.obj(
              "groupBy" -> groupByValuesJson,
              "scoreSum" -> scoreSum,
              "agg" -> aggJson
            )
          }
        }

      withOptionalFields(queryOption, results.size, degrees, results, stepResult.failCount, cursorJson, nextQuery)
    }
  }

  /** Global helper functions */
  @tailrec
  final def randomInt(sampleNumber: Int, range: Int, set: Set[Int] = Set.empty[Int]): Set[Int] = {
    if (range < sampleNumber || set.size == sampleNumber) set
    else randomInt(sampleNumber, range, set + Random.nextInt(range))
  }

  def sample(queryRequest: QueryRequest, edges: Seq[EdgeWithScore], n: Int): Seq[EdgeWithScore] = {
    if (edges.size <= n) {
      edges
    } else {
      val plainEdges = if (queryRequest.queryParam.offset == 0) {
        edges.tail
      } else edges

      val randoms = randomInt(n, plainEdges.size)
      var samples = List.empty[EdgeWithScore]
      var idx = 0
      plainEdges.foreach { e =>
        if (randoms.contains(idx)) samples = e :: samples
        idx += 1
      }
      samples
    }
  }

  def normalize(edgeWithScores: Seq[EdgeWithScore]): Seq[EdgeWithScore] = {
    val sum = edgeWithScores.foldLeft(0.0) { case (acc, cur) => acc + cur.score }
    edgeWithScores.map { edgeWithScore =>
      edgeWithScore.copy(score = edgeWithScore.score / sum)
    }
  }

  def alreadyVisitedVertices(edgeWithScoreLs: Seq[EdgeWithScore]): Map[(LabelWithDirection, S2VertexLike), Boolean] = {
    val vertices = for {
      edgeWithScore <- edgeWithScoreLs
      edge = edgeWithScore.edge
      vertex = if (edge.labelWithDir.dir == GraphUtil.directions("out")) edge.tgtVertex else edge.srcVertex
    } yield (edge.labelWithDir, vertex) -> true

    vertices.toMap
  }

  /** common methods for filter out, transform, aggregate queryResult */
  def convertEdges(queryParam: QueryParam, edge: S2EdgeLike, nextStepOpt: Option[Step]): Seq[S2EdgeLike] = {
    for {
      convertedEdge <- queryParam.edgeTransformer.transform(queryParam, edge, nextStepOpt) if !edge.isDegree
    } yield convertedEdge
  }

  def processTimeDecay(queryParam: QueryParam, edge: S2EdgeLike) = {
    /* process time decay */
    val tsVal = queryParam.timeDecay match {
      case None => 1.0
      case Some(timeDecay) =>
        val tsVal = try {
          val innerValWithTsOpt = edge.propertyValue(timeDecay.labelMeta.name)
          innerValWithTsOpt.map { innerValWithTs =>
            val innerVal = innerValWithTs.innerVal
            timeDecay.labelMeta.dataType match {
              case InnerVal.LONG => innerVal.value match {
                case n: BigDecimal => n.bigDecimal.longValue()
                case _ => innerVal.toString().toLong
              }
              case _ => innerVal.toString().toLong
            }
          } getOrElse (edge.ts)
        } catch {
          case e: Exception =>
            logger.error(s"processTimeDecay error. ${edge.toLogString}", e)
            edge.ts
        }
        val timeDiff = queryParam.timestamp - tsVal
        timeDecay.decay(timeDiff)
    }

    tsVal
  }

  def processDuplicates[R](queryParam: QueryParam,
                           duplicates: Seq[(FilterHashKey, R)])(implicit ev: WithScore[R]): Seq[(FilterHashKey, R)] = {

    if (queryParam.label.consistencyLevel != "strong") {
      //TODO:
      queryParam.duplicatePolicy match {
        case DuplicatePolicy.First => Seq(duplicates.head)
        case DuplicatePolicy.Raw => duplicates
        case DuplicatePolicy.CountSum =>
          val countSum = duplicates.size
          val (headFilterHashKey, headEdgeWithScore) = duplicates.head
          Seq(headFilterHashKey -> ev.withNewScore(headEdgeWithScore, countSum))
        case _ =>
          val scoreSum = duplicates.foldLeft(0.0) { case (prev, current) => prev + ev.score(current._2) }
          val (headFilterHashKey, headEdgeWithScore) = duplicates.head
          Seq(headFilterHashKey -> ev.withNewScore(headEdgeWithScore, scoreSum))
      }
    } else {
      duplicates
    }
  }

  def toHashKey(queryParam: QueryParam, edge: S2EdgeLike, isDegree: Boolean): (HashKey, FilterHashKey) = {
    val src = edge.srcVertex.innerId.hashCode()
    val tgt = edge.tgtVertex.innerId.hashCode()
    val hashKey = (src, edge.labelWithDir.labelId, edge.labelWithDir.dir, tgt, isDegree)
    val filterHashKey = (src, tgt)

    (hashKey, filterHashKey)
  }

  def filterEdges(q: Query,
                  stepIdx: Int,
                  queryRequests: Seq[QueryRequest],
                  queryResultLsFuture: Future[Seq[StepResult]],
                  queryParams: Seq[QueryParam],
                  alreadyVisited: Map[(LabelWithDirection, S2VertexLike), Boolean] = Map.empty,
                  buildLastStepInnerResult: Boolean = true,
                  parentEdges: Map[VertexId, Seq[EdgeWithScore]])
                 (implicit ec: scala.concurrent.ExecutionContext): Future[StepResult] = {

    queryResultLsFuture.map { queryRequestWithResultLs =>
      val (cursors, failCount) = {
        val _cursors = ArrayBuffer.empty[Array[Byte]]
        var _failCount = 0

        queryRequestWithResultLs.foreach { stepResult =>
          _cursors.append(stepResult.cursors: _*)
          _failCount += stepResult.failCount
        }

        _cursors -> _failCount
      }


      if (queryRequestWithResultLs.isEmpty) StepResult.Empty.copy(failCount = failCount)
      else {
        val isLastStep = stepIdx == q.steps.size - 1
        val queryOption = q.queryOption
        val step = q.steps(stepIdx)

        val currentStepResults = queryRequests.view.zip(queryRequestWithResultLs)
        val shouldBuildInnerResults = !isLastStep || buildLastStepInnerResult
        val degrees = queryRequestWithResultLs.flatMap(_.degreeEdges)

        if (shouldBuildInnerResults) {
          val _results = buildResult(q, stepIdx, currentStepResults, parentEdges) { (edgeWithScore, propsSelectColumns) =>
            edgeWithScore
          }

          /* process step group by */
          val results = StepResult.filterOutStepGroupBy(_results, step.groupBy)
          StepResult(edgeWithScores = results, grouped = Nil, degreeEdges = degrees, cursors = cursors, failCount = failCount)

        } else {
          val _results = buildResult(q, stepIdx, currentStepResults, parentEdges) { (edgeWithScore, propsSelectColumns) =>
            val edge = edgeWithScore.edge
            val score = edgeWithScore.score
            val label = edgeWithScore.label

            /* Select */
            val mergedPropsWithTs = edge.propertyValuesInner(propsSelectColumns)

            //            val newEdge = edge.copy(propsWithTs = mergedPropsWithTs)
            val newEdge = edge.copyEdgeWithState(mergedPropsWithTs)

            val newEdgeWithScore = edgeWithScore.copy(edge = newEdge)
            /* OrderBy */
            val orderByValues =
              if (queryOption.orderByKeys.isEmpty) (score, edge.tsInnerVal, None, None)
              else StepResult.toTuple4(newEdgeWithScore.toValues(queryOption.orderByKeys))

            /* StepGroupBy */
            val stepGroupByValues = newEdgeWithScore.toValues(step.groupBy.keys)

            /* GroupBy */
            val groupByValues = newEdgeWithScore.toValues(queryOption.groupBy.keys)

            /* FilterOut */
            val filterOutValues = newEdgeWithScore.toValues(queryOption.filterOutFields)

            newEdgeWithScore.copy(orderByValues = orderByValues,
              stepGroupByValues = stepGroupByValues,
              groupByValues = groupByValues,
              filterOutValues = filterOutValues)
          }

          /* process step group by */
          val results = StepResult.filterOutStepGroupBy(_results, step.groupBy)

          /* process ordered list */
          val ordered = if (queryOption.groupBy.keys.isEmpty) StepResult.orderBy(queryOption, results) else Nil

          /* process grouped list */
          val grouped =
            if (queryOption.groupBy.keys.isEmpty) Nil
            else {
              val agg = new scala.collection.mutable.HashMap[StepResult.GroupByKey, (Double, StepResult.Values)]()
              results.groupBy { edgeWithScore =>
                //                edgeWithScore.groupByValues.map(_.map(_.toString))
                edgeWithScore.groupByValues
              }.foreach { case (k, ls) =>
                val (scoreSum, merged) = StepResult.mergeOrdered(ls, Nil, queryOption)

                val newScoreSum = scoreSum

                /*
                  * watch out here. by calling toString on Any, we lose type information which will be used
                  * later for toJson.
                  */
                if (merged.nonEmpty) {
                  val newKey = merged.head.groupByValues
                  agg += ((newKey, (newScoreSum, merged)))
                }
              }
              agg.toSeq.sortBy(_._2._1 * -1)
            }

          StepResult(edgeWithScores = ordered, grouped = grouped, degreeEdges = degrees, cursors = cursors, failCount = failCount)
        }
      }
    }
  }

  def toEdgeWithScores(queryRequest: QueryRequest,
                               stepResult: StepResult,
                               parentEdges: Map[VertexId, Seq[EdgeWithScore]]): Seq[EdgeWithScore] = {
    val queryOption = queryRequest.query.queryOption
    val queryParam = queryRequest.queryParam
    val prevScore = queryRequest.prevStepScore
    val labelWeight = queryRequest.labelWeight
    val edgeWithScores = stepResult.edgeWithScores

    val shouldBuildParents = queryOption.returnTree || queryParam.whereHasParent
    val parents = if (shouldBuildParents) {
      parentEdges.getOrElse(queryRequest.vertex.id, Nil).map { edgeWithScore =>
        val edge = edgeWithScore.edge
        val score = edgeWithScore.score
        val label = edgeWithScore.label

        /* Select */
        val mergedPropsWithTs =
          if (queryOption.selectColumns.isEmpty) {
            edge.propertyValuesInner()
          } else {
            val initial = Map(LabelMeta.timestamp -> edge.propertyValueInner(LabelMeta.timestamp))
            edge.propertyValues(queryOption.selectColumns) ++ initial
          }

        val newEdge = edge.copyEdgeWithState(mergedPropsWithTs)
        edgeWithScore.copy(edge = newEdge)
      }
    } else Nil

    // skip
    if (queryOption.ignorePrevStepCache) stepResult.edgeWithScores
    else {
      val degreeScore = 0.0

      val sampled =
        if (queryRequest.queryParam.sample >= 0) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
        else edgeWithScores

      val withScores = for {
        edgeWithScore <- sampled
      } yield {
        val edge = edgeWithScore.edge
        val edgeScore = edgeWithScore.score
        val score = queryParam.scorePropagateOp match {
          case "plus" => edgeScore + prevScore
          case "divide" =>
            if ((prevScore + queryParam.scorePropagateShrinkage) == 0) 0
            else edgeScore / (prevScore + queryParam.scorePropagateShrinkage)
          case _ => edgeScore * prevScore
        }

        val tsVal = processTimeDecay(queryParam, edge)
        val newScore = degreeScore + score
        //          val newEdge = if (queryOption.returnTree) edge.copy(parentEdges = parents) else edge
        val newEdge = edge.copyParentEdges(parents)
        edgeWithScore.copy(edge = newEdge, score = newScore * labelWeight * tsVal)
      }

      val normalized =
        if (queryParam.shouldNormalize) normalize(withScores)
        else withScores

      normalized
    }
  }

  def buildResult[R](query: Query,
                             stepIdx: Int,
                             stepResultLs: Seq[(QueryRequest, StepResult)],
                             parentEdges: Map[VertexId, Seq[EdgeWithScore]])
                            (createFunc: (EdgeWithScore, Seq[LabelMeta]) => R)
                            (implicit ev: WithScore[R]): ListBuffer[R] = {
    import scala.collection._

    val results = ListBuffer.empty[R]
    val sequentialLs: ListBuffer[(HashKey, FilterHashKey, R, QueryParam)] = ListBuffer.empty
    val duplicates: mutable.HashMap[HashKey, ListBuffer[(FilterHashKey, R)]] = mutable.HashMap.empty
    val edgesToExclude: mutable.HashSet[FilterHashKey] = mutable.HashSet.empty
    val edgesToInclude: mutable.HashSet[FilterHashKey] = mutable.HashSet.empty

    var numOfDuplicates = 0
    val queryOption = query.queryOption
    val step = query.steps(stepIdx)
    val excludeLabelWithDirSet = step.queryParams.filter(_.exclude).map(l => l.labelWithDir).toSet
    val includeLabelWithDirSet = step.queryParams.filter(_.include).map(l => l.labelWithDir).toSet

    stepResultLs.foreach { case (queryRequest, stepInnerResult) =>
      val queryParam = queryRequest.queryParam
      val label = queryParam.label
      val shouldBeExcluded = excludeLabelWithDirSet.contains(queryParam.labelWithDir)
      val shouldBeIncluded = includeLabelWithDirSet.contains(queryParam.labelWithDir)

      val propsSelectColumns = (for {
        column <- queryOption.propsSelectColumns
        labelMeta <- label.metaPropsInvMap.get(column)
      } yield labelMeta)

      for {
        edgeWithScore <- toEdgeWithScores(queryRequest, stepInnerResult, parentEdges)
      } {
        val edge = edgeWithScore.edge
        val (hashKey, filterHashKey) = toHashKey(queryParam, edge, isDegree = false)
        //        params += (hashKey -> queryParam) //

        /* check if this edge should be exlcuded. */
        if (shouldBeExcluded) {
          edgesToExclude.add(filterHashKey)
        } else {
          if (shouldBeIncluded) {
            edgesToInclude.add(filterHashKey)
          }
          val newEdgeWithScore = createFunc(edgeWithScore, propsSelectColumns)

          sequentialLs += ((hashKey, filterHashKey, newEdgeWithScore, queryParam))
          duplicates.get(hashKey) match {
            case None =>
              val newLs = ListBuffer.empty[(FilterHashKey, R)]
              newLs += (filterHashKey -> newEdgeWithScore)
              duplicates += (hashKey -> newLs) //
            case Some(old) =>
              numOfDuplicates += 1
              old += (filterHashKey -> newEdgeWithScore) //
          }
        }
      }
    }


    if (numOfDuplicates == 0) {
      // no duplicates at all.
      for {
        (hashKey, filterHashKey, edgeWithScore, _) <- sequentialLs
        if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
      } {
        results += edgeWithScore
      }
    } else {
      // need to resolve duplicates.
      val seen = new mutable.HashSet[HashKey]()
      for {
        (hashKey, filterHashKey, edgeWithScore, queryParam) <- sequentialLs
        if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
        if !seen.contains(hashKey)
      } {
        //        val queryParam = params(hashKey)
        processDuplicates(queryParam, duplicates(hashKey)).foreach { case (_, duplicate) =>
          if (ev.score(duplicate) >= queryParam.threshold) {
            seen += hashKey
            results += duplicate
          }
        }
      }
    }
    results
  }
}