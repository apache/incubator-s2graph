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

  def s2EdgeParent(graph: S2GraphLike,
                   queryOption: QueryOption,
                   parentEdges: Seq[EdgeWithScore]): JsValue = {
    if (parentEdges.isEmpty) JsNull
    else {
      val ancestors = for {
        current <- parentEdges
        parents = s2EdgeParent(graph, queryOption, current.edge.getParentEdges()) if parents != JsNull
      } yield {
          val s2Edge = current.edge.getOriginalEdgeOpt().getOrElse(current.edge)
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
      builder += ("from" -> anyValToJsValue(s2Edge.srcVertex.innerIdVal).get)
      builder += ("label" -> anyValToJsValue(label.label).get)
      builder += ("direction" -> anyValToJsValue(s2Edge.getDirection()).get)
      builder += (LabelMeta.degree.name -> anyValToJsValue(s2Edge.propertyValueInner(LabelMeta.degree).innerVal.value).get)
      JsObject(builder)
    } else {
      if (queryOption.withScore) builder += ("score" -> anyValToJsValue(score).get)

      if (queryOption.selectColumns.isEmpty) {
        builder += ("from" -> anyValToJsValue(s2Edge.srcVertex.innerIdVal).get)
        builder += ("to" -> anyValToJsValue(s2Edge.tgtVertex.innerIdVal).get)
        builder += ("label" -> anyValToJsValue(label.label).get)

        val innerProps = ArrayBuffer.empty[(String, JsValue)]
        for {
          (labelMeta, v) <- edgeWithScore.edge.propertyValues()
          jsValue <- anyValToJsValue(v.innerVal.value)
        } {
          innerProps += (labelMeta.name -> jsValue)
        }


        builder += ("props" -> JsObject(innerProps))
        builder += ("direction" -> anyValToJsValue(s2Edge.getDirection()).get)
        builder += ("timestamp" -> anyValToJsValue(s2Edge.getTsInnerValValue()).get)
        builder += ("_timestamp" -> anyValToJsValue(s2Edge.getTsInnerValValue()).get) // backward compatibility
        if (parents != JsNull) builder += ("parents" -> parents)
        //          Json.toJson(builder.result())
        JsObject(builder)
      } else {
        queryOption.selectColumnsMap.foreach { case (columnName, _) =>
          columnName match {
            case "from" => builder += ("from" -> anyValToJsValue(s2Edge.srcVertex.innerIdVal).get)
            case "_from" => builder += ("_from" -> anyValToJsValue(s2Edge.srcVertex.innerIdVal).get)
            case "to" => builder += ("to" -> anyValToJsValue(s2Edge.tgtVertex.innerIdVal).get)
            case "_to" => builder += ("_to" -> anyValToJsValue(s2Edge.tgtVertex.innerIdVal).get)
            case "label" => builder += ("label" -> anyValToJsValue(label.label).get)
            case "direction" => builder += ("direction" -> anyValToJsValue(s2Edge.getDirection()).get)
            case "timestamp" => builder += ("timestamp" -> anyValToJsValue(s2Edge.getTsInnerValValue()).get)
            case "_timestamp" => builder += ("_timestamp" -> anyValToJsValue(s2Edge.getTsInnerValValue()).get)
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
      (_, property) <- s2Vertex.props
      jsVal <- anyValToJsValue(property.value)
    } yield property.columnMeta.name -> jsVal

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

  def toJson(orgQuery: Option[JsValue])(graph: S2GraphLike,
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
        val parents = if (queryOption.returnTree) s2EdgeParent(graph, queryOption, t.edge.getParentEdges()) else JsNull

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
              val parents = if (queryOption.returnTree) s2EdgeParent(graph, queryOption, t.edge.getParentEdges()) else JsNull
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

  def s2EdgePropsJsonString(edge: S2EdgeLike): String =
    Json.toJson(s2EdgePropsJson(edge)).toString()

  def s2VertexPropsJsonString(vertex: S2VertexLike): String =
    Json.toJson(s2VertexPropsJson(vertex)).toString()

  def s2EdgePropsJson(edge: S2EdgeLike): Map[String, JsValue] = {
    import scala.collection.JavaConverters._
    for {
      (k, v) <- edge.getPropsWithTs().asScala.toMap
      jsValue <- JSONParser.anyValToJsValue(v.innerVal.value)
    } yield (v.labelMeta.name -> jsValue)
  }

  def s2VertexPropsJson(vertex: S2VertexLike): Map[String, JsValue] = {
    import scala.collection.JavaConverters._
    for {
      (k, v) <- vertex.props.asScala.toMap
      jsValue <- JSONParser.anyValToJsValue(v.innerVal.value)
    } yield (v.columnMeta.name -> jsValue)
  }
}