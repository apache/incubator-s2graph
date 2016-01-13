package com.kakao.s2graph.core.Integrate

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core.rest.RequestParser
import com.kakao.s2graph.core.utils.logger
import com.typesafe.config._
import org.scalatest._
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

trait IntegrateCommon extends FunSuite with Matchers with BeforeAndAfterAll {

  import TestUtil._

  var graph: Graph = _
  var parser: RequestParser = _
  var config: Config = _

  override def beforeAll = {
    config = ConfigFactory.load()
    graph = new Graph(config)(ExecutionContext.Implicits.global)
    parser = new RequestParser(graph.config)
    initTestData()
  }

  override def afterAll(): Unit = {
    graph.shutdown()
  }

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
      Management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
    println(s">> Service created : $createService, $tryRes")

    val labelNames = Map(testLabelName -> testLabelNameCreate,
      testLabelName2 -> testLabelName2Create,
      testLabelNameV1 -> testLabelNameV1Create,
      testLabelNameWeak -> testLabelNameWeakCreate)

    for {
      (labelName, create) <- labelNames
    } {
      Management.deleteLabel(labelName)
      Label.findByName(labelName, useCache = false) match {
        case None =>
          val json = Json.parse(create)
          val tryRes = for {
            labelArgs <- parser.toLabelElements(json)
            label <- (Management.createLabel _).tupled(labelArgs)
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

      val ret = graph.getEdges(parser.toQuery(queryJson))
      val result = Await.result(ret, HttpRequestWaitingTime)
      val jsResult = PostProcess.toSimpleVertexArrJson(result)

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
    val testLabelName = "s2graph_label_test"
    val testLabelName2 = "s2graph_label_test_2"
    val testLabelNameV1 = "s2graph_label_test_v1"
    val testLabelNameWeak = "s2graph_label_test_weak"
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
    "schemaVersion": "v2",
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
    "schemaVersion": "v1",
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
    "consistencyLevel": "weak",
    "isDirected": true,
    "compressionAlgorithm": "gz"
  }"""
  }
}
