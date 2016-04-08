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
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.Label
import org.apache.s2graph.core.rest.{RequestParser, RestHandler}
import org.apache.s2graph.core.utils.logger
import org.scalatest._
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

trait IntegrateCommon extends FunSuite with Matchers with BeforeAndAfterAll with TestCommon{

  import TestUtil._


  override def beforeAll = {
    config = ConfigFactory.load()
    graph = new Graph(config)(ExecutionContext.Implicits.global)
    management = new Management(graph)
    parser = new RequestParser(graph.config)
    initTestData()
  }

  override def afterAll(): Unit = {
    graph.shutdown()
  }

  def getLabelName(ver: String, consistency: String = "strong") = s"s2graph_label_test_${ver}_${consistency}"
  def getLabelName2(ver: String, consistency: String = "strong") = s"s2graph_label_test_${ver}_${consistency}_2"

  /**
   * Make Service, Label, Vertex for integrate test
   */
  def initTestData() = {
    println("[init start]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    Management.deleteService(testServiceName)

    // 1. createService
    val jsValue = Json.parse(createService)
    val (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) =
      parser.toServiceElements(jsValue)

    val tryRes =
      management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
    println(s">> Service created : $createService, $tryRes")

    //    val labelNames = Map(testLabelNameV4 -> testLabelNameCreate(ver),
    //      testLabelNameV3 -> testLabelNameCreate(ver),
    //      testLabelNameV1 -> testLabelNameCreate(ver),
    //      testLabelNameWeak -> testLabelNameWeakCreate(ver))

    // Create test labels by versions + consistency.
    for (n <- versions; consistency <- Seq("strong", "weak")) yield {
      val ver = s"v$n"
      val labelName = getLabelName(ver, consistency)
      val create = testLabelCreate(ver, consistency)
      Management.deleteLabel(labelName)
      Label.findByName(labelName, useCache = false) match {
        case None =>
          val json = Json.parse(create)
          val tryRes = for {
            labelArgs <- parser.toLabelElements(json)
            label <- (management.createLabel _).tupled(labelArgs)
          } yield label

          tryRes.get
        case Some(label) =>
          println(s">> Label already exist: $create, $label")
      }
    }

    val vertexPropsKeys = List("age" -> "int")

    vertexPropsKeys.map { case (key, keyType) =>
      Management.addVertexProp(testServiceName, testColumnName, key, keyType)
    }

    println("[init end]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
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

    def vertexQueryJson(serviceName: String, columnName: String, ids: Seq[Int]) = {
      Json.parse(
        s"""
           |[
           |{"serviceName": "$serviceName", "columnName": "$columnName", "ids": [${ids.mkString(",")}]}
                                                                                                       |]
       """.stripMargin)
    }

    def vertexInsertsPayload(serviceName: String, columnName: String, ids: Seq[Int]): Seq[JsValue] = {
      ids.map { id =>
        Json.obj("id" -> id, "props" -> randomProps, "timestamp" -> System.currentTimeMillis())
      }
    }

    val vertexPropsKeys = List(
      ("age", "int")
    )

    def randomProps() = {
      (for {
        (propKey, propType) <- vertexPropsKeys
      } yield {
          propKey -> Random.nextInt(100)
        }).toMap
    }

    def getVerticesSync(queryJson: JsValue): JsValue = {
      val restHandler = new RestHandler(graph)
      logger.info(Json.prettyPrint(queryJson))
      val f = restHandler.getVertices(queryJson)
      Await.result(f, HttpRequestWaitingTime)
    }

    def deleteAllSync(jsValue: JsValue) = {
      val future = Future.sequence(jsValue.as[Seq[JsValue]] map { json =>
        val (labels, direction, ids, ts, vertices) = parser.toDeleteParam(json)
        val future = graph.deleteAllAdjacentEdges(vertices.toList, labels, GraphUtil.directions(direction), ts)

        future
      })

      Await.result(future, HttpRequestWaitingTime)
    }


    def getEdgesSync(queryJson: JsValue): JsValue = {
      logger.info(Json.prettyPrint(queryJson))
      val restHandler = new RestHandler(graph)
      Await.result(restHandler.getEdgesAsync(queryJson)(PostProcess.toSimpleVertexArrJson), HttpRequestWaitingTime)
    }

    def checkEdgesSync(checkEdgeJson: JsValue): JsValue = {
      logger.info(Json.prettyPrint(checkEdgeJson))

      val ret = parser.toCheckEdgeParam(checkEdgeJson) match {
        case (e, _) => graph.checkEdges(e)
      }
      val result = Await.result(ret, HttpRequestWaitingTime)
      val jsResult = PostProcess.toSimpleVertexArrJson(result)

      logger.info(jsResult.toString)
      jsResult
    }

    def mutateEdgesSync(bulkEdges: String*) = {
      val req = graph.mutateElements(parser.toGraphElements(bulkEdges.mkString("\n")), withWait = true)
      val jsResult = Await.result(req, HttpRequestWaitingTime)

      jsResult
    }

    def insertEdgesSync(bulkEdges: String*) = {
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
    val testLabelNameV4 = "s2graph_label_test_v4_strong"
    val testLabelNameV3 = "s2graph_label_test_v3_strong"
    val testLabelNameV2 = "s2graph_label_test_v2_strong"
    val testLabelNameV1 = "s2graph_label_test_v1_strong"
    val testLabelNameWeakV4 = "s2graph_label_test_v4_weak"
    val testLabelNameWeakV3 = "s2graph_label_test_v3_weak"
    val testLabelNameWeakV2 = "s2graph_label_test_v2_weak"
    val testLabelNameWeakV1 = "s2graph_label_test_v1_weak"
    val testColumnName = "user_id_test"
    val testColumnType = "long"
    val testTgtColumnName = "item_id_test"
    val testHTableName = "test-htable"
    val newHTableName = "new-htable"
    val index1 = "idx_1"
    val index2 = "idx_2"

    val NumOfEachTest = 30
    val HttpRequestWaitingTime = Duration("60 seconds")

    val createService = s"""{"serviceName" : "$testServiceName"}"""

    def testLabelCreate(ver: String, consistency: String = "strong") = {
      val label = getLabelName(ver, consistency)
      s"""
  {
    "label": "$label",
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
    "consistencyLevel": "$consistency",
    "schemaVersion": "$ver",
    "compressionAlgorithm": "gz"
  }"""
    }

    def testLabel2Create(ver: String, consistency: String = "strong") = {
      val label = getLabelName2(ver, consistency)
      s"""
  {
    "label": "$label",
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
    "consistencyLevel": "$consistency",
    "schemaVersion": "$ver",
    "compressionAlgorithm": "gz"
  }"""
    }

    def querySingle(id: Int, label: String, offset: Int = 0, limit: Int = 100) = Json.parse(
      s"""
      { "srcVertices": [
        { "serviceName": "$testServiceName",
          "columnName": "$testColumnName",
          "id": $id
         }],
        "steps": [
        [ {
            "label": "$label",
            "direction": "out",
            "offset": $offset,
            "limit": $limit
          }
        ]]
      }
    """)

    def queryWithInterval(label: String, id: Int, index: String, prop: String, fromVal: Int, toVal: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
          [ {
              "label": "$label",
              "index": "$index",
              "interval": {
                  "from": [ { "$prop": $fromVal } ],
                  "to": [ { "$prop": $toVal } ]
              }
            }
          ]]
        }
    """)

    def queryWhere(id: Int, label: String, where: String) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${label}",
              "direction": "out",
              "offset": 0,
              "limit": 100,
              "where": "${where}"
            }
          ]]
        }""")

    def queryExclude(id: Int, label: String) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${label}",
              "direction": "out",
              "offset": 0,
              "limit": 2
            },
            {
              "label": "${label}",
              "direction": "in",
              "offset": 0,
              "limit": 2,
              "exclude": true
            }
          ]]
        }""")

    def queryGroupBy(id: Int, label: String, props: Seq[String]): JsValue = {
      Json.obj(
        "groupBy" -> props,
        "srcVertices" -> Json.arr(
          Json.obj("serviceName" -> testServiceName, "columnName" -> testColumnName, "id" -> id)
        ),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj(
                "label" -> label
              )
            )
          )
        )
      )
    }

    def queryTransform(id: Int, label: String, transforms: String) = Json.parse(
      s"""
      { "srcVertices": [
        { "serviceName": "${testServiceName}",
          "columnName": "${testColumnName}",
          "id": ${id}
         }],
        "steps": [
        [ {
            "label": "${label}",
            "direction": "out",
            "offset": 0,
            "transform": $transforms
          }
        ]]
      }""")

    def queryIndex(ids: Seq[Int], label: String, indexName: String) = {
      val $from = Json.arr(
        Json.obj("serviceName" -> testServiceName,
          "columnName" -> testColumnName,
          "ids" -> ids))

      val $step = Json.arr(Json.obj("label" -> label, "index" -> indexName))
      val $steps = Json.arr(Json.obj("step" -> $step))

      val js = Json.obj("withScore" -> false, "srcVertices" -> $from, "steps" -> $steps)
      js
    }

    def queryParents(id: Long) = Json.parse(
      s"""
          {
            "returnTree": true,
            "srcVertices": [
            { "serviceName": "$testServiceName",
              "columnName": "$testColumnName",
              "id": $id
             }],
            "steps": [
            [ {
                "label": "$testLabelNameV3",
                "direction": "out",
                "offset": 0,
                "limit": 2
              }
            ],[{
                "label": "$testLabelNameV3",
                "direction": "in",
                "offset": 0,
                "limit": -1
              }
            ]]
          }""".stripMargin)

    def querySingleWithTo(id: Int, label: String, offset: Int = 0, limit: Int = 100, to: Int) = Json.parse(
      s"""
          { "srcVertices": [
            { "serviceName": "${testServiceName}",
              "columnName": "${testColumnName}",
              "id": ${id}
             }],
            "steps": [
            [ {
                "label": "${label}",
                "direction": "out",
                "offset": $offset,
                "limit": $limit,
                "_to": $to
              }
            ]]
          }
          """)

    def queryScore(id: Int, label: String, scoring: Map[String, Int]): JsValue = Json.obj(
      "srcVertices" -> Json.arr(
        Json.obj(
          "serviceName" -> testServiceName,
          "columnName" -> testColumnName,
          "id" -> id
        )
      ),
      "steps" -> Json.arr(
        Json.obj(
          "step" -> Json.arr(
            Json.obj(
              "label" -> label,
              "scoring" -> scoring
            )
          )
        )
      )
    )

    def queryOrderBy(id: Int, label: String, scoring: Map[String, Int], props: Seq[Map[String, String]]): JsValue = Json.obj(
      "orderBy" -> props,
      "srcVertices" -> Json.arr(
        Json.obj("serviceName" -> testServiceName, "columnName" -> testColumnName, "id" -> id)
      ),
      "steps" -> Json.arr(
        Json.obj(
          "step" -> Json.arr(
            Json.obj(
              "label" -> label,
              "scoring" -> scoring
            )
          )
        )
      )
    )

    def queryWithSampling(id: Int, label: String, sample: Int) = Json.parse(
      s"""
          { "srcVertices": [
            { "serviceName": "$testServiceName",
              "columnName": "$testColumnName",
              "id": $id
             }],
            "steps": [
              {
                "step": [{
                  "label": "$label",
                  "direction": "out",
                  "offset": 0,
                  "limit": 100,
                  "sample": $sample
                  }]
              }
            ]
          }""")

    def twoStepQueryWithSampling(id: Int, label: String, sample: Int) = Json.parse(
      s"""
          { "srcVertices": [
            { "serviceName": "$testServiceName",
              "columnName": "$testColumnName",
              "id": $id
             }],
            "steps": [
              {
                "step": [{
                  "label": "$label",
                  "direction": "out",
                  "offset": 0,
                  "limit": 100,
                  "sample": $sample
                  }]
              },
              {
                 "step": [{
                   "label": "$label",
                   "direction": "out",
                   "offset": 0,
                   "limit": 100,
                   "sample": $sample
                 }]
              }
            ]
          }""")

    def twoQueryWithSampling(id: Int, label: String, label2: String, sample: Int) = Json.parse(
      s"""
          { "srcVertices": [
            { "serviceName": "$testServiceName",
              "columnName": "$testColumnName",
              "id": $id
             }],
            "steps": [
              {
                "step": [{
                  "label": "$label",
                  "direction": "out",
                  "offset": 0,
                  "limit": 50,
                  "sample": $sample
                },
                {
                  "label": "$label",
                  "direction": "out",
                  "offset": 0,
                  "limit": 50
                }]
              }
            ]
          }""")
  }

}