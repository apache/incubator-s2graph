package test.controllers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.mysqls._
import config.Config

import scala.util.Random

//import com.daumkakao.s2graph.core.models._

import controllers.{EdgeController, AdminController}
import org.specs2.mutable.Specification
import play.api.Logger
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

trait S2graphSpecCommon extends Specification {
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
  lazy val TC_WAITING_TIME = 1200
  val NUM_OF_EACH_TEST = 3
  lazy val HTTP_REQ_WAITING_TIME = Duration(5000, MILLISECONDS)
  val asyncFlushInterval = 1000

  val createService =
    s"""
       |{
       |"serviceName" : "$testServiceName"
                                           |}
    """.stripMargin

  val testLabelNameCreate = s"""
  {
    "label": "$testLabelName",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "$testColumnName",
    "tgtColumnType": "long",
    "indexProps": [
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
    "props": [],
    "consistencyLevel": "strong",
    "schemaVersion": "v2"
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
    "indexProps": [
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
    "props": [],
    "consistencyLevel": "strong",
    "isDirected": false,
    "schemaVersion": "v2"
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
    "indexProps": [
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
    "props": [],
    "consistencyLevel": "strong",
    "isDirected": true,
    "schemaVersion": "v1"
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
    "indexProps": [
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
    "props": [],
    "consistencyLevel": "weak",
    "isDirected": true
  }"""

  val vertexPropsKeys = List(
    ("age", "int")
  )

  val createVertex = s"""{
    "serviceName": "$testServiceName",
    "columnName": "$testColumnName",
    "columnType": "long",
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
      Graph(Config.conf.underlying)(ExecutionContext.Implicits.global)
      Management.deleteService(testServiceName)
      //
      //      // 1. createService

      var result = AdminController.createServiceInner(Json.parse(createService))
      println(s">> Service created : $createService, $result")
      //
      ////      val labelNames = Map(testLabelName -> testLabelNameCreate)
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
            result = AdminController.createLabelInner(Json.parse(create))
            Logger.error(s">> Label created : $create, $result")
          case Some(label) =>
            Logger.error(s">> Label already exist: $create, $label")
        }
      }

      println("[init end]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      // 5. create vertex
      //      vertexPropsKeys.map { case (key, keyType) =>
      //        Management.addVertexProp(testServiceName, testColumnName, key, keyType)
      //      }

      Thread.sleep(asyncFlushInterval)
    }
  }

}

class BasicCRUDSpec extends S2graphSpecCommon {

  def runTC(tcNum: Int, tcString: String, opWithProps: List[(Long, String, String)], expected: Map[String, String]) = {
    for {
      labelName <- List(testLabelName, testLabelName2)
      //      labelName <- List(testLabelNameV1)
      i <- (0 to NUM_OF_EACH_TEST)
    } {
      val srcId = ((tcNum * 1000) + i).toString
      val tgtId = if (labelName == testLabelName) s"${srcId + 1000 + i}" else s"${(srcId + 1000 + i)}abc"

      val maxTs = opWithProps.map(t => t._1).max

      /** insert edges */
      println(s"---- TC${tcNum}_init ----")
      val bulkEdge = (for ((ts, op, props) <- opWithProps) yield {
        List(ts, op, "e", srcId, tgtId, labelName, props).mkString("\t")
      }).mkString("\n")

      val req = FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdge)
      println(s">> $req, $bulkEdge")
      val res = Await.result(route(req).get, HTTP_REQ_WAITING_TIME)

      res.header.status must equalTo(200)

      println(s"---- TC${tcNum}_init ----")

      Thread.sleep(asyncFlushInterval)

      for {
        label <- Label.findByName(labelName)
        direction <- List("out", "in")
        cacheTTL <- List(1000L, 2000L)
      } {
        val (serviceName, columnName, id, otherId) = direction match {
          case "out" => (label.srcService.serviceName, label.srcColumn.columnName, srcId, tgtId)
          case "in" => (label.tgtService.serviceName, label.tgtColumn.columnName, tgtId, srcId)
        }
        val qId = if (labelName == testLabelName) id else "\"" + id + "\""
        val query = queryJson(serviceName, columnName, labelName, qId, direction, cacheTTL)
        val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(query)).get
        val jsResult = commonCheck(ret)


        val results = jsResult \ "results"
        val deegrees = (jsResult \ "degrees").as[List[JsObject]]
        val propsLs = (results \\ "props").seq
        (deegrees.head \ LabelMeta.degree.name).as[Int] must equalTo(1)

        val from = (results \\ "from").seq.last.toString.replaceAll("\"", "")
        val to = (results \\ "to").seq.last.toString.replaceAll("\"", "")

        from must equalTo(id.toString)
        to must equalTo(otherId.toString)
        (results \\ "_timestamp").seq.last.as[Long] must equalTo(maxTs)
        for ((key, expectedVal) <- expected) {
          propsLs.last.as[JsObject].keys.contains(key) must equalTo(true)
          (propsLs.last \ key).toString must equalTo(expectedVal)
        }
        Await.result(ret, HTTP_REQ_WAITING_TIME)
      }
    }
  }

  init()
  "integritySpec " should {
    "tc1" in {
      running(FakeApplication()) {
        var tcNum = 0
        var tcString = ""
        var bulkQueries = List.empty[(Long, String, String)]
        var expected = Map.empty[String, String]

        tcNum = 7
        tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
        bulkQueries = List(
          (t1, "insert", "{\"time\": 10}"),
          (t2, "delete", ""),
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 8
        tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
        bulkQueries = List(
          (t1, "insert", "{\"time\": 10}"),
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 9
        tcString = "[t3 -> t2 -> t1 test case] insert(t3) delete(t2) insert(t1) test "
        bulkQueries = List(
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""),
          (t1, "insert", "{\"time\": 10}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 10
        tcString = "[t3 -> t1 -> t2 test case] insert(t3) insert(t1) delete(t2) test "
        bulkQueries = List(
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
          (t1, "insert", "{\"time\": 10}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 11
        tcString = "[t2 -> t1 -> t3 test case] delete(t2) insert(t1) insert(t3) test"
        bulkQueries = List(
          (t2, "delete", ""),
          (t1, "insert", "{\"time\": 10}"),
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 12
        tcString = "[t2 -> t3 -> t1 test case] delete(t2) insert(t3) insert(t1) test "
        bulkQueries = List(
          (t2, "delete", ""),
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
          (t1, "insert", "{\"time\": 10}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 13
        tcString = "[t1 -> t2 -> t3 test case] update(t1) delete(t2) update(t3) test "
        bulkQueries = List(
          (t1, "update", "{\"time\": 10}"),
          (t2, "delete", ""),
          (t3, "update", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 14
        tcString = "[t1 -> t3 -> t2 test case] update(t1) update(t3) delete(t2) test "
        bulkQueries = List(
          (t1, "update", "{\"time\": 10}"),
          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 15
        tcString = "[t2 -> t1 -> t3 test case] delete(t2) update(t1) update(t3) test "
        bulkQueries = List(
          (t2, "delete", ""),
          (t1, "update", "{\"time\": 10}"),
          (t3, "update", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 16
        tcString = "[t2 -> t3 -> t1 test case] delete(t2) update(t3) update(t1) test"
        bulkQueries = List(
          (t2, "delete", ""),
          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
          (t1, "update", "{\"time\": 10}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 17
        tcString = "[t3 -> t2 -> t1 test case] update(t3) delete(t2) update(t1) test "
        bulkQueries = List(
          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""),
          (t1, "update", "{\"time\": 10}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 18
        tcString = "[t3 -> t1 -> t2 test case] update(t3) update(t1) delete(t2) test "
        bulkQueries = List(
          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
          (t1, "update", "{\"time\": 10}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 19
        tcString = "[t5 -> t1 -> t3 -> t2 -> t4 test case] update(t5) insert(t1) insert(t3) delete(t2) update(t4) test "
        bulkQueries = List(
          (t5, "update", "{\"is_blocked\": true}"),
          (t1, "insert", "{\"is_hidden\": false}"),
          (t3, "insert", "{\"is_hidden\": false, \"weight\": 10}"),
          (t2, "delete", ""),
          (t4, "update", "{\"time\": 1, \"weight\": -10}"))
        expected = Map("time" -> "1", "weight" -> "-10", "is_hidden" -> "false", "is_blocked" -> "true")

        runTC(tcNum, tcString, bulkQueries, expected)
        true
      }
    }
  }
}

//    "vetex tc" should {
//      "tc1" in {
//        running(FakeApplication()) {
//          val ids = (0 until 3).toList
//          val (serviceName, columnName) = (testServiceName, testColumnName)
//
//          val data = vertexInsertsPayload(serviceName, columnName, ids)
//          val payload = Json.parse(Json.toJson(data).toString)
//
//          val req = FakeRequest(POST, s"/graphs/vertices/insert/$serviceName/$columnName").withBody(payload)
//          println(s">> $req, $payload")
//          val res = Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
//          println(res)
//          res.header.status must equalTo(200)
//          Thread.sleep(asyncFlushInterval)
//          println("---------------")
//
//          val query = vertexQueryJson(serviceName, columnName, ids)
//          val retFuture = route(FakeRequest(POST, "/graphs/getVertices").withJsonBody(query)).get
//
//          val ret = contentAsJson(retFuture)
//          println(">>>", ret)
//          val fetched = ret.as[Seq[JsValue]]
//          for {
//            (d, f) <- data.zip(fetched)
//          } yield {
//            (d \ "id") must beEqualTo((f \ "id"))
//            ((d \ "props") \ "age") must beEqualTo(((f \ "props") \ "age"))
//          }
//        }
//        true
//      }
//    }
//
//  "cache test" should {
//    def queryWithTTL(id: Int, cacheTTL: Long) = Json.parse( s"""
//        { "srcVertices": [
//          { "serviceName": "${testServiceName}",
//            "columnName": "${testColumnName}",
//            "id": ${id}
//           }],
//          "steps": [[ {
//            "label": "${testLabelName}",
//            "direction": "out",
//            "offset": 0,
//            "limit": 10,
//            "cacheTTL": ${cacheTTL},
//            "scoring": {"weight": 1} }]]
//          }""")
//
//    def getEdges(queryJson: JsValue): JsValue = {
//      var ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
//      contentAsJson(ret)
//    }
//
//    // init
//    running(FakeApplication()) {
//      // insert bulk and wait ..
//      var bulkEdges: String = Seq(
//        Seq("1", "insert", "e", "0", "2", "s2graph_label_test", "{}").mkString("\t"),
//        Seq("1", "insert", "e", "1", "2", "s2graph_label_test", "{}").mkString("\t")
//      ).mkString("\n")
//
//      val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges)).get
//      val jsRslt = contentAsJson(ret)
//      Thread.sleep(asyncFlushInterval)
//    }
//
//    "tc1: query with {id: 0, ttl: 1000}" in {
//      running(FakeApplication()) {
//        var jsRslt = getEdges(queryWithTTL(0, 1000))
//        var cacheRemain = (jsRslt \\ "cacheRemain").head
//        cacheRemain.as[Int] must greaterThan(500)
//
//        // get edges from cache after wait 500ms
//        Thread.sleep(500)
//        var ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryWithTTL(0, 1000))).get
//        jsRslt = contentAsJson(ret)
//        cacheRemain = (jsRslt \\ "cacheRemain").head
//        cacheRemain.as[Int] must lessThan(500)
//      }
//    }
//
//    "tc2: query with {id: 1, ttl: 3000}" in {
//      running(FakeApplication()) {
//        var jsRslt = getEdges(queryWithTTL(1, 3000))
//        var cacheRemain = (jsRslt \\ "cacheRemain").head
//        // before update: is_blocked is false
//        (jsRslt \\ "is_blocked").head must equalTo(JsBoolean(false))
//
//        var bulkEdges = Seq(
//          Seq("2", "update", "e", "0", "2", "s2graph_label_test", "{\"is_blocked\": true}").mkString("\t"),
//          Seq("2", "update", "e", "1", "2", "s2graph_label_test", "{\"is_blocked\": true}").mkString("\t")
//        ).mkString("\n")
//
//        // update edges with {is_blocked: true}
//        var ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges)).get
//        jsRslt = contentAsJson(ret)
//
//        Thread.sleep(asyncFlushInterval)
//
//        // prop 'is_blocked' still false, cause queryResult on cache
//        jsRslt = getEdges(queryWithTTL(1, 3000))
//        (jsRslt \\ "is_blocked").head must equalTo(JsBoolean(false))
//
//        // after wait 3000ms prop 'is_blocked' is updated to true, cache cleared
//        Thread.sleep(3000)
//        jsRslt = getEdges(queryWithTTL(1, 3000))
//        (jsRslt \\ "is_blocked").head must equalTo(JsBoolean(true))
//      }
//    }
//  }

class QuerySpec extends S2graphSpecCommon {
  init()

  "query test" should {
    running(FakeApplication()) {
      // insert bulk and wait ..
      val bulkEdges: String = Seq(
        Seq("1", "insert", "e", "0", "1", testLabelName, "{}").mkString("\t"),
        Seq("1", "insert", "e", "0", "2", testLabelName, "{}").mkString("\t"),
        Seq("1", "insert", "e", "2", "0", testLabelName, "{}").mkString("\t"),
        Seq("1", "insert", "e", "2", "1", testLabelName, "{}").mkString("\t")
      ).mkString("\n")

      val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges)).get
      val jsRslt = contentAsJson(ret)
      Thread.sleep(asyncFlushInterval)
    }

    def query(id: Int) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": 0,
              "limit": 2
            },
            {
              "label": "${testLabelName}",
              "direction": "in",
              "offset": 0,
              "limit": 2,
              "exclude": true
            }
          ]]
        }""")

    def getEdges(queryJson: JsValue): JsValue = {
      var ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
      contentAsJson(ret)
    }

    "get edge exclude" in {
      running(FakeApplication()) {
        val result = getEdges(query(0))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(1)

        true
      }
    }
  }
}

class WeakLabelDeleteSpec extends S2graphSpecCommon {
  init()

  "weak label delete test" should {
    running(FakeApplication()) {
      // insert bulk and wait ..
      val bulkEdges: String = Seq(
        Seq("1", "insert", "e", "0", "1", testLabelNameWeak, s"""{"time": 10}""").mkString("\t"),
        Seq("2", "insert", "e", "0", "1", testLabelNameWeak, s"""{"time": 11}""").mkString("\t"),
        Seq("3", "insert", "e", "0", "1", testLabelNameWeak, s"""{"time": 12}""").mkString("\t"),
        Seq("4", "insert", "e", "0", "2", testLabelNameWeak, s"""{"time": 10}""").mkString("\t")
      ).mkString("\n")

      val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges)).get
      val jsRslt = contentAsJson(ret)
      Thread.sleep(asyncFlushInterval)
    }

    def query(id: Int) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelNameWeak}",
              "direction": "out",
              "offset": 0,
              "limit": 10,
              "duplicate": "raw"
            }
          ]]
        }""")

    def getEdges(queryJson: JsValue): JsValue = {
      var ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
      contentAsJson(ret)
    }

    "test weak consistency select" in {
      running(FakeApplication()) {
        val result = getEdges(query(0))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(4)

        true
      }
    }

    "test weak consistency delete" in {
      running(FakeApplication()) {
        var result = getEdges(query(0))
        println(result)

        /** expect 4 edges */
        (result \ "results").as[List[JsValue]].size must equalTo(4)
        val edges = (result \ "results").as[List[JsObject]]
        EdgeController.tryMutates(Json.toJson(edges), "delete")

        Thread.sleep(asyncFlushInterval)

        /** expect noting */
        result = getEdges(query(0))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(0)

        /** insert should be ignored */
        EdgeController.tryMutates(Json.toJson(edges), "insert")

        Thread.sleep(asyncFlushInterval)

        result = getEdges(query(0))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(0)

        true
      }
    }
  }
}
