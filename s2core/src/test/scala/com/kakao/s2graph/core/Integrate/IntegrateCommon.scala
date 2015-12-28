package com.kakao.s2graph.core.Integrate

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core.rest.RequestParser
import com.typesafe.config._
import org.scalatest._
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

trait IntegrateCommon extends FunSuite with Matchers with BeforeAndAfterAll {
  var graph: Graph = _
  var parser: RequestParser = _
  var config: Config = _

  override def beforeAll(): Unit = {
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
  object TestHelper {
    def getEdges(queryJson: JsValue): JsValue = {
      val ret = graph.getEdges(parser.toQuery(queryJson))
      val result = Await.result(ret, HttpRequestWaitingTime)
      val jsResult = PostProcess.toSimpleVertexArrJson(result)

      jsResult
    }

    def insertEdges(bulkEdges: String*) = {
      val req = graph.mutateElements(parser.toGraphElements(bulkEdges.mkString("\n")), withWait = true)
      val jsResult = Await.result(req, HttpRequestWaitingTime)
    }

    def toEdge(elems: Any*): String = elems.mkString("\t")
  }

  // common tables
  protected val testServiceName = "s2graph"
  protected val testLabelName = "s2graph_label_test"
  protected val testLabelName2 = "s2graph_label_test_2"
  protected val testLabelNameV1 = "s2graph_label_test_v1"
  protected val testLabelNameWeak = "s2graph_label_test_weak"
  protected val testColumnName = "user_id_test"
  protected val testColumnType = "long"
  protected val testTgtColumnName = "item_id_test"
  protected val testHTableName = "test-htable"
  protected val newHTableName = "new-htable"
  protected val index1 = "idx_1"
  protected val index2 = "idx_2"

  val NumOfEachTest = 30
  val HttpRequestWaitingTime = Duration("Inf")

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
