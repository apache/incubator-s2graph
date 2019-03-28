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

package org.apache.s2graph.core.Integrate

import com.typesafe.config._
import org.apache.s2graph.core.schema.{Label, Schema}
import org.apache.s2graph.core.rest.{RequestParser, RestHandler}
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core._
import org.scalatest._
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

trait IntegrateCommon extends FunSuite with Matchers with BeforeAndAfterAll {

  import TestUtil._

  var graph: S2Graph = _
  var parser: RequestParser = _
  var management: Management = _
  var config: Config = _

  override def beforeAll = {
    config = ConfigFactory.load()
    graph = new S2Graph(config)(ExecutionContext.Implicits.global)
    management = new Management(graph)
    parser = new RequestParser(graph)
    initTestData()
  }

  override def afterAll(): Unit = {
    graph.shutdown()
  }

  /**
   * Make Service, Label, Vertex for integrate test
   */
  def initTestData() = {
    logger.info("[init start]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    Management.deleteService(testServiceName)

    // 1. createService
    val jsValue = Json.parse(createService)
    val (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) =
      parser.toServiceElements(jsValue)

    val tryRes =
      management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
    logger.info(s">> Service created : $createService, $tryRes")

    val labelNames = Map(testLabelName -> testLabelNameCreate,
      testLabelName2 -> testLabelName2Create,
      testLabelNameV1 -> testLabelNameV1Create,
      testLabelNameWeak -> testLabelNameWeakCreate,
      testLabelNameLabelIndex -> testLabelNameLabelIndexCreate)

    for {
      (labelName, create) <- labelNames
    } {
      Management.deleteLabel(labelName)
      Label.findByName(labelName, useCache = false) match {
        case None =>
          val json = Json.parse(create)
          logger.info(s">> Create Label")
          logger.info(create)
          val tryRes = for {
            label <- parser.toLabelElements(json)
          } yield label

          tryRes.get
        case Some(label) =>
          logger.info(s">> Label already exist: $create, $label")
      }
    }

    val vertexPropsKeys = List(
      ("age", "integer", "0"),
      ("im", "string", "-")
    )
    Seq(testColumnName, testTgtColumnName).foreach { columnName =>
      vertexPropsKeys.map { case (key, keyType, defaultValue) =>
        Management.addVertexProp(testServiceName, columnName, key, keyType, defaultValue, storeInGlobalIndex = true)
      }
    }
    // vertex type global index.
//    val globalVertexIndex = management.buildGlobalIndex(GlobalIndex.VertexType, "test_age_index", Seq("age"))

    // edge type global index.
//    val globalEdgeIndex = management.buildGlobalIndex(GlobalIndex.EdgeType, "test_weight_time_edge", Seq("weight", "time"))

    logger.info("[init end]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  }


  /**
   * Test Helpers
   */
  object TestUtil {
    implicit def ec = scala.concurrent.ExecutionContext.global

    //    def checkEdgeQueryJson(params: Seq[(String, String, String, String)]) = {
    //      val arr = for {
    //        (label, dir, from, to) <- params
    //      } yield {
    //        Json.obj("label" -> label, "direction" -> dir, "from" -> from, "to" -> to)
    //      }
    //
    //      val s = Json.toJson(arr)
    //      s
    //    }

    def deleteAllSync(jsValue: JsValue) = {
      val future = Future.sequence(jsValue.as[Seq[JsValue]] map { json =>
        val (labels, direction, ids, ts, vertices) = parser.toDeleteParam(json)
        val srcVertices = vertices
        val future = graph.deleteAllAdjacentEdges(srcVertices.toList, labels, GraphUtil.directions(direction), ts)

        future
      })

      Await.result(future, HttpRequestWaitingTime)
    }

    def getEdgesSync(s2Query: Query): JsValue = {
      logger.info(s2Query.toString)
      val stepResult = Await.result(graph.getEdges(s2Query), HttpRequestWaitingTime)
      val result = PostProcess.toJson(Option(s2Query.jsonQuery))(graph, s2Query.queryOption, stepResult)
      //      val result = Await.result(graph.getEdges(s2Query).(PostProcess.toJson), HttpRequestWaitingTime)
      logger.debug(s"${Json.prettyPrint(result)}")
      result
    }

    def getEdgesSync(queryJson: JsValue): JsValue = {
      logger.info(Json.prettyPrint(queryJson))
      val restHandler = new RestHandler(graph)
      val result = Await.result(restHandler.getEdgesAsync(queryJson)(PostProcess.toJson(Option(queryJson))), HttpRequestWaitingTime)
      logger.debug(s"${Json.prettyPrint(result)}")
      result
    }

    def insertEdgesSync(bulkEdges: String*) = {
      logger.debug(s"${bulkEdges.mkString("\n")}")
      val req = graph.mutateElements(parser.toGraphElements(bulkEdges.mkString("\n")), withWait = true)
      val jsResult = Await.result(req, HttpRequestWaitingTime)

      jsResult
    }

    def insertEdgesAsync(bulkEdges: String*) = {
      val req = graph.mutateElements(parser.toGraphElements(bulkEdges.mkString("\n")), withWait = true)
      req
    }

    def toEdge(elems: Any*): String = elems.mkString("\t")

    // common tables
    val testServiceName = "s2graph"
    val testLabelName = "s2graph_label_test"
    val testLabelName2 = "s2graph_label_test_2"
    val testLabelNameV1 = "s2graph_label_test_v1"
    val testLabelNameWeak = "s2graph_label_test_weak"
    val testLabelNameLabelIndex = "s2graph_label_test_index"
    val testColumnName = "user_id_test"
    val testColumnType = "long"
    val testTgtColumnName = "item_id_test"
    val testHTableName = "test-htable"
    val newHTableName = "new-htable"
    val index1 = "idx_1"
    val index2 = "idx_2"
    val idxStoreInDropDegree = "idx_drop_In"
    val idxStoreOutDropDegree = "idx_drop_out"
    val idxStoreIn = "idx_store_In"
    val idxStoreOut = "idx_store_out"
    val idxDropInStoreDegree = "idx_drop_in_store_degree"
    val idxDropOutStoreDegree = "idx_drop_out_store_degree"

    val NumOfEachTest = 3
    val HttpRequestWaitingTime = Duration("600 seconds")

    val createService = s"""{"serviceName" : "$testServiceName"}"""

    val testLabelNameCreate =
      s"""
  {
    "label": "$testLabelName",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "$testColumnName",
    "tgtColumnType": "long",
    "indices": [
      {"name": "$index1", "propNames": ["weight", "time", "is_hidden", "is_blocked"]},
      {"name": "$index2", "propNames": ["_timestamp"]}
    ],
    "props": [
    {
      "name": "time",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "weight",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "is_hidden",
      "dataType": "boolean",
      "defaultValue": false
    },
    {
      "name": "is_blocked",
      "dataType": "boolean",
      "defaultValue": false
    }
    ],
    "consistencyLevel": "strong",
    "schemaVersion": "v4",
    "compressionAlgorithm": "gz",
    "hTableName": "$testHTableName"
  }"""

    val testLabelName2Create =
      s"""
  {
    "label": "$testLabelName2",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "$testTgtColumnName",
    "tgtColumnType": "string",
    "indices": [{"name": "$index1", "propNames": ["time", "weight", "is_hidden", "is_blocked"]}],
    "props": [
    {
      "name": "time",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "weight",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "is_hidden",
      "dataType": "boolean",
      "defaultValue": false
    },
    {
      "name": "is_blocked",
      "dataType": "boolean",
      "defaultValue": false
    }
    ],
    "consistencyLevel": "strong",
    "isDirected": false,
    "schemaVersion": "v3",
    "compressionAlgorithm": "gz"
  }"""

    val testLabelNameV1Create =
      s"""
  {
    "label": "$testLabelNameV1",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "${testTgtColumnName}_v1",
    "tgtColumnType": "string",
    "indices": [{"name": "$index1", "propNames": ["time", "weight", "is_hidden", "is_blocked"]}],
    "props": [
    {
      "name": "time",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "weight",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "is_hidden",
      "dataType": "boolean",
      "defaultValue": false
    },
    {
      "name": "is_blocked",
      "dataType": "boolean",
      "defaultValue": false
    }
    ],
    "consistencyLevel": "strong",
    "isDirected": true,
    "schemaVersion": "v2",
    "compressionAlgorithm": "gz"
  }"""

    val testLabelNameWeakCreate =
      s"""
  {
    "label": "$testLabelNameWeak",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "$testTgtColumnName",
    "tgtColumnType": "string",
    "indices": [
      {"name": "$index1", "propNames": ["time", "weight", "is_hidden", "is_blocked"]},
      {"name": "$index2", "propNames": ["time"]}
    ],
    "props": [
    {
      "name": "time",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "weight",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "is_hidden",
      "dataType": "boolean",
      "defaultValue": false
    },
    {
      "name": "is_blocked",
      "dataType": "boolean",
      "defaultValue": false
    }
    ],
    "consistencyLevel": "weak",
    "isDirected": true,
    "compressionAlgorithm": "gz"
  }"""

    val testLabelNameLabelIndexCreate =
      s"""
  {
    "label": "$testLabelNameLabelIndex",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "$testColumnName",
    "tgtColumnType": "long",
    "indices": [
       {"name": "$index1", "propNames": ["weight", "time", "is_hidden", "is_blocked"]},
       {"name": "$idxStoreInDropDegree", "propNames": ["time"], "options": { "in": {"storeDegree": false }, "out": {"method": "drop", "storeDegree": false }}},
       {"name": "$idxStoreOutDropDegree", "propNames": ["weight"], "options": { "out": {"storeDegree": false}, "in": { "method": "drop", "storeDegree": false }}},
       {"name": "$idxStoreIn", "propNames": ["is_hidden"], "options": { "out": {"method": "drop", "storeDegree": false }}},
       {"name": "$idxStoreOut", "propNames": ["weight", "is_blocked"], "options": { "in": {"method": "drop", "storeDegree": false }, "out": {"method": "normal" }}},
       {"name": "$idxDropInStoreDegree", "propNames": ["is_blocked"], "options": { "in": {"method": "drop" }, "out": {"method": "drop", "storeDegree": false }}},
       {"name": "$idxDropOutStoreDegree", "propNames": ["weight", "is_blocked", "_timestamp"], "options": { "in": {"method": "drop", "storeDegree": false }, "out": {"method": "drop"}}}
    ],
    "props": [
    {
      "name": "time",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "weight",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "is_hidden",
      "dataType": "boolean",
      "defaultValue": false
    },
    {
      "name": "is_blocked",
      "dataType": "boolean",
      "defaultValue": false
    }
    ],
    "consistencyLevel": "strong",
    "schemaVersion": "v4",
    "compressionAlgorithm": "gz",
    "hTableName": "$testHTableName"
  }"""
  }
}
