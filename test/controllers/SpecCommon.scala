package test.controllers

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls._
import controllers.AdminController
import org.specs2.mutable.Specification
import play.api.libs.json._
import play.api.test.FakeApplication
import play.api.test.Helpers._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random


trait SpecCommon extends Specification {

  object Helper {

    import org.json4s.native.Serialization
    type KV = Map[String, Any]

    import scala.language.dynamics

    def $aa[T](args: T*) = List($a(args: _ *))

    def $a[T](args: T*) = args.toList

    object $ extends Dynamic {
      def applyDynamicNamed(name: String)(args: (String, Any)*): Map[String, Any] = args.toMap
    }

    implicit class anyMapOps(map: Map[String, Any]) {
      def toJson: JsValue = {
        val js = Serialization.write(map)(org.json4s.DefaultFormats)
        Json.parse(js)
      }
    }

    implicit class S2Context(val sc : StringContext) {
      def edge(args : Any*)(implicit map: Map[String, Any] = Map.empty) : String = {
        val parts = sc.s(args: _*).split("\\s")
        assert(parts.length == 6)
        (parts.toList :+  map.toJson.toString).mkString("\t")
      }
    }
  }

  val curTime = System.currentTimeMillis
  val t1 = curTime + 0
  val t2 = curTime + 1
  val t3 = curTime + 2
  val t4 = curTime + 3
  val t5 = curTime + 4

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

  val NUM_OF_EACH_TEST = 3
  val HTTP_REQ_WAITING_TIME = Duration(5000, MILLISECONDS)
  val asyncFlushInterval = 100

  val createService = s"""{"serviceName" : "$testServiceName"}"""
  val testLabelNameCreate = s"""
  {
    "label": "$testLabelName",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "$testColumnName",
    "tgtColumnType": "long",
    "indices": [
      {"name": "idx_1", "propNames": ["weight", "time", "is_hidden", "is_blocked"]},
      {"name": "idx_2", "propNames": ["_timestamp"]}
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

  val testLabelName2Create = s"""
  {
    "label": "$testLabelName2",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "$testTgtColumnName",
    "tgtColumnType": "string",
    "indices": [{"name": "idx_1", "propNames": ["time", "weight", "is_hidden", "is_blocked"]}],
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
    "schemaVersion": "v2",
    "compressionAlgorithm": "gz"
  }"""

  val testLabelNameV1Create = s"""
  {
    "label": "$testLabelNameV1",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "${testTgtColumnName}_v1",
    "tgtColumnType": "string",
    "indices": [{"name": "idx_1", "propNames": ["time", "weight", "is_hidden", "is_blocked"]}],
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
  val testLabelNameWeakCreate = s"""
  {
    "label": "$testLabelNameWeak",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "$testTgtColumnName",
    "tgtColumnType": "string",
    "indices": [{"name": "idx_1", "propNames": ["time", "weight", "is_hidden", "is_blocked"]}],
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

  val vertexPropsKeys = List(
    ("age", "int")
  )

  val createVertex = s"""{
    "serviceName": "$testServiceName",
    "columnName": "$testColumnName",
    "columnType": "$testColumnType",
    "props": [
        {"name": "is_active", "dataType": "boolean", "defaultValue": true},
        {"name": "phone_number", "dataType": "string", "defaultValue": "-"},
        {"name": "nickname", "dataType": "string", "defaultValue": ".."},
        {"name": "activity_score", "dataType": "float", "defaultValue": 0.0},
        {"name": "age", "dataType": "integer", "defaultValue": 0}
    ]
    }"""


  val TS = System.currentTimeMillis()

  def queryJson(serviceName: String, columnName: String, labelName: String, id: String, dir: String, cacheTTL: Long = -1L) = {
    val s = s"""{
      "srcVertices": [
      {
        "serviceName": "$serviceName",
        "columnName": "$columnName",
        "id": $id
      }
      ],
      "steps": [
      [
      {
        "label": "$labelName",
        "direction": "$dir",
        "offset": 0,
        "limit": 10,
        "cacheTTL": $cacheTTL
      }
      ]
      ]
    }"""
    println(s)
    Json.parse(s)
  }

  def checkEdgeQueryJson(params: Seq[(String, String, String, String)]) = {
    val arr = for {
      (label, dir, from, to) <- params
    } yield {
        Json.obj("label" -> label, "direction" -> dir, "from" -> from, "to" -> to)
      }

    val s = Json.toJson(arr)
    println(s)
    s
  }

  def vertexQueryJson(serviceName: String, columnName: String, ids: Seq[Int]) = {
    Json.parse(
      s"""
         |[
         |{"serviceName": "$serviceName", "columnName": "$columnName", "ids": [${ids.mkString(",")}
         ]}
         |]
       """.stripMargin)
  }

  def randomProps() = {
    (for {
      (propKey, propType) <- vertexPropsKeys
    } yield {
        propKey -> Random.nextInt(100)
      }).toMap
  }

  def vertexInsertsPayload(serviceName: String, columnName: String, ids: Seq[Int]): Seq[JsValue] = {
    ids.map { id =>
      Json.obj("id" -> id, "props" -> randomProps, "timestamp" -> System.currentTimeMillis())
    }
  }

  def commonCheck(rslt: Future[play.api.mvc.Result]): JsValue = {
    status(rslt) must equalTo(OK)
    contentType(rslt) must beSome.which(_ == "application/json")
    val jsRslt = contentAsJson(rslt)
    println("======")
    println(jsRslt)
    println("======")
    jsRslt.as[JsObject].keys.contains("size") must equalTo(true)
    (jsRslt \ "size").as[Int] must greaterThan(0)
    jsRslt.as[JsObject].keys.contains("results") must equalTo(true)
    val jsRsltsObj = jsRslt \ "results"
    jsRsltsObj.as[JsArray].value(0).as[JsObject].keys.contains("from") must equalTo(true)
    jsRsltsObj.as[JsArray].value(0).as[JsObject].keys.contains("to") must equalTo(true)
    jsRsltsObj.as[JsArray].value(0).as[JsObject].keys.contains("_timestamp") must equalTo(true)
    jsRsltsObj.as[JsArray].value(0).as[JsObject].keys.contains("props") must equalTo(true)
    jsRslt
  }

  def init() = {
    running(FakeApplication()) {
      println("[init start]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      Management.deleteService(testServiceName)

      // 1. createService
      val result = AdminController.createServiceInner(Json.parse(createService))
      println(s">> Service created : $createService, $result")

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
            AdminController.createLabelInner(Json.parse(create))
          case Some(label) =>
            println(s">> Label already exist: $create, $label")
        }
      }

      // 5. create vertex
      vertexPropsKeys.map { case (key, keyType) =>
        Management.addVertexProp(testServiceName, testColumnName, key, keyType)
      }

      println("[init end]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

      Thread.sleep(asyncFlushInterval)
    }
  }
}

