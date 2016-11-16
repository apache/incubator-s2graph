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

import org.apache.s2graph.core.GraphExceptions.BadQueryException
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.{InnerVal, InnerValLike}
import org.apache.s2graph.core.JSONParser._
import play.api.libs.json.{Json, _}

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object PostProcess {


  type EDGE_VALUES = Map[String, JsValue]
  type ORDER_BY_VALUES =  (Any, Any, Any, Any)
  type RAW_EDGE = (EDGE_VALUES, Double, ORDER_BY_VALUES)
  type GROUP_BY_KEY = Map[String, JsValue]

  /**
   * Result Entity score field name
   */
  val emptyDegrees = Seq.empty[JsValue]
  val timeoutResults = Json.obj("size" -> 0, "degrees" -> Json.arr(), "results" -> Json.arr(), "isTimeout" -> true)
  val emptyResults = Json.obj("size" -> 0, "degrees" -> Json.arr(), "results" -> Json.arr(), "isEmpty" -> true)
  def badRequestResults(ex: => Exception) = ex match {
    case ex: BadQueryException => Json.obj("message" -> ex.msg)
    case _ => Json.obj("message" -> ex.getMessage)
  }

  val SCORE_FIELD_NAME = "scoreSum"
  val reservedColumns = Set("cacheRemain", "from", "to", "label", "direction", "_timestamp", "timestamp", "score", "props")


  def s2EdgeParent(graph: Graph,
                   parentEdges: Seq[EdgeWithScore]): JsValue = {
    if (parentEdges.isEmpty) JsNull
    else {
      val ancestors = for {
        current <- parentEdges
        parents = s2EdgeParent(graph, current.edge.parentEdges) if parents != JsNull
      } yield {
          val s2Edge = current.edge.originalEdgeOpt.getOrElse(current.edge)
          s2EdgeToJsValue(s2Edge, current.score, false, parents = parents)
        }
      Json.toJson(ancestors)
    }
  }

  def s2EdgeToJsValue(s2Edge: Edge,
                      score: Double,
                      isDegree: Boolean = false,
                      parents: JsValue = JsNull): JsValue = {
    if (isDegree) {
      Json.obj(
        "from" -> anyValToJsValue(s2Edge.srcId),
        "label" -> s2Edge.labelName,
        LabelMeta.degree.name -> anyValToJsValue(s2Edge.propsWithTs(LabelMeta.degreeSeq).innerVal.value)
      )
    } else {
      Json.obj("from" -> anyValToJsValue(s2Edge.srcId),
        "to" -> anyValToJsValue(s2Edge.tgtId),
        "label" -> s2Edge.labelName,
        "score" -> score,
        "props" -> JSONParser.propertiesToJson(s2Edge.properties),
        "direction" -> s2Edge.direction,
        "timestamp" -> anyValToJsValue(s2Edge.ts),
        "parents" -> parents
      )
    }
  }

  def withImpressionId(queryOption: QueryOption,
                        size: Int,
                        degrees: Seq[JsValue],
                        results: Seq[JsValue]): JsValue = {
    queryOption.impIdOpt match {
      case None => Json.obj(
        "size" -> size,
        "degrees" -> degrees,
        "results" -> results
      )
      case Some(impId) =>
        Json.obj(
          "size" -> size,
          "degrees" -> degrees,
          "results" -> results,
          Experiment.ImpressionKey -> impId
        )
    }
  }
  def s2VertexToJson(s2Vertex: Vertex): Option[JsValue] = {
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

  def verticesToJson(s2Vertices: Seq[Vertex]): JsValue =
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

  def toJson(graph: Graph,
             queryOption: QueryOption,
             stepResult: StepResult): JsValue = {


    val degrees =
      if (queryOption.returnDegree) stepResult.degreeEdges.map(t => s2EdgeToJsValue(t.s2Edge, t.score, true))
      else emptyDegrees

    if (queryOption.groupBy.keys.isEmpty) {
      // no group by specified on query.

      val ls = stepResult.results.map { t =>
        val parents = if (queryOption.returnTree) s2EdgeParent(graph, t.parentEdges) else JsNull
        s2EdgeToJsValue(t.s2Edge, t.score, false, parents)
      }
      withImpressionId(queryOption, ls.size, degrees, ls)
    } else {

      val results =
        for {
          (groupByValues, (scoreSum, edges)) <- stepResult.grouped
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
              val parents = if (queryOption.returnTree) s2EdgeParent(graph, t.parentEdges) else JsNull
              s2EdgeToJsValue(t.s2Edge, t.score, false, parents)
            }
            val aggJson = Json.toJson(agg)
            Json.obj(
              "groupBy" -> groupByValuesJson,
              "scoreSum" -> scoreSum,
              "agg" -> aggJson
            )
          }
        }
      withImpressionId(queryOption, results.size, degrees, results)
    }
  }
}
