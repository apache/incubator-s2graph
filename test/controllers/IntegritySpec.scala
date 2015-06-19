package test.controllers

import com.daumkakao.s2graph.core._

import scala.util.Random

//import com.daumkakao.s2graph.core.mysqls._

import com.daumkakao.s2graph.core.models._
import play.api.Logger


import com.daumkakao.s2graph.rest.config.Config
import controllers.AdminController
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import org.specs2.execute.{AsResult, Result}
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest, Helpers, WithApplication}
import scala.concurrent.ExecutionContext


/**
 * Data Integrity Test specification
 * @author June.k( Junki Kim, june.k@kakao.com )
 * @since 15. 1. 5.
 */

class IntegritySpec extends Specification {
  val curTime = System.currentTimeMillis
  val t1 = curTime + 0
  val t2 = curTime + 1
  val t3 = curTime + 2
  val t4 = curTime + 3
  val t5 = curTime + 4

  protected val testServiceName = "s2graph"
  protected val testLabelName = "s2graph_label_test"
  protected val testLabelName2 = "s2graph_label_test_2"
  protected val testColumnName = "user_id_test"
  protected val testColumnType = "long"
  protected val testTgtColumnName = "item_id_test"
  lazy val TC_WAITING_TIME = 1200

  lazy val HTTP_REQ_WAITING_TIME = Duration(5000, MILLISECONDS)
  val asyncFlushInterval = 2000

  val createService =
    s"""
       |{
       |"serviceName" : "$testServiceName"
       |}
    """.stripMargin

  val createLabel = s"""
  {
    "label": "$testLabelName",
    "srcServiceName": "$testServiceName",
    "srcColumnName": "$testColumnName",
    "srcColumnType": "long",
    "tgtServiceName": "$testServiceName",
    "tgtColumnName": "$testTgtColumnName",
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
    "consistencyLevel": "strong"
  }"""

  val createLabel2 = s"""
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
    "isDirected": false
  }"""

  val vertexPropsKeys = List(
    ("age", "int")
  )

  val createVertex =s"""{
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

  val NUM_OF_EACH_TEST = 10
  val TS = System.currentTimeMillis()
  def queryJson(serviceName: String, columnName: String, labelName: String, id: String, dir: String) = {
    Json.parse( s"""{
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
        "limit": 10
      }
      ]
      ]
    }""")
  }
  def vertexQueryJson(serviceName: String, columnName: String, ids: Seq[Int]) = {
    Json.parse(
      s"""
         |[
         |    {"serviceName": "$serviceName", "columnName": "$columnName", "ids": [${ids.mkString(",")}]}
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
      Graph(Config.conf.underlying)(ExecutionContext.Implicits.global)
      Management.deleteService(testServiceName)
      Management.deleteLabel(testLabelName)
      Management.deleteLabel(testLabelName2)


      // 1. createService
      var result = AdminController.createServiceInner(Json.parse(createService))
      println(s">> Service created : $createService, $result")


      // 2. createLabel
      Label.findByName(testLabelName, useCache = false) match {
        case None =>
          result = AdminController.createLabelInner(Json.parse(createLabel))
          Logger.error(s">> Label created : $createLabel, $result")
        case Some(label) =>
          Logger.error(s">> Label already exist: $createLabel, $label")
      }

      // 3. create second label
      Label.findByName(testLabelName2, useCache = false) match {
        case None =>
          result = AdminController.createLabelInner(Json.parse(createLabel2))
          Logger.error(s">> Label created : $createLabel2, $result")
        case Some(label) =>
          Logger.error(s">> Label already exist: $createLabel2, $label")
      }
      // 4. create vertex
      vertexPropsKeys.map { case (key, keyType) =>
        Management.addVertexProp(testServiceName, testColumnName, key, keyType)
      }
    }
  }


  def runTC(tcNum: Int, tcString: String, opWithProps: List[(Long, String, String)], expected: Map[String, String]) = {
    for {
      labelName <- List(testLabelName, testLabelName2)
      i <- (1 to NUM_OF_EACH_TEST)
    } {
      val srcId = ((tcNum * 1000) + i).toString
      val tgtId = if (labelName == testLabelName) s"${srcId + 1000}" else s"${(srcId + 1000)}abc"

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
      Thread.sleep(asyncFlushInterval)
      println(s"---- TC${tcNum}_init ----")


      for {
        label <- Label.findByName(labelName)
        direction <- List("out", "in")
      } {
        val (serviceName, columnName, id, otherId) = direction match {
          case "out" => (label.srcService.serviceName, label.srcColumn.columnName, srcId, tgtId)
          case "in" => (label.tgtService.serviceName, label.tgtColumn.columnName, tgtId, srcId)
        }

        val query = queryJson(serviceName, columnName, labelName, id, direction)
        val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(query)).get
        val jsResult = commonCheck(ret)


        val results = jsResult \ "results"
        val deegrees = (jsResult \ "degrees").as[List[JsObject]]
        val propsLs = (results \\ "props").seq
        (deegrees.head \ LabelMeta.degree.name).as[Int] must equalTo(1)

        (results \\ "from").seq.last.toString must equalTo(id.toString)
        (results \\ "to").seq.last.toString must equalTo(otherId.toString)
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

        tcNum = 1

        tcString = "[t1 -> t3 -> t2 test case] incr(t0) incr(t2) delete(t1) test"
        bulkQueries = List(
          (t1, "increment", "{\"weight\": 10}"),
          (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 2

        tcString = "[t1 -> t2 -> t3 test case] incr(t1) delete(t2) incr(t3) test"
        bulkQueries = List(
          (t1, "increment", "{\"weight\": 10}"),
          (t2, "delete", ""),
          (t3, "increment", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 3
        tcString = "[t2 -> t1 -> t3 test case] delete(t2) incr(t1) incr(t3) test "
        bulkQueries = List(
          (t2, "delete", ""),
          (t1, "increment", "{\"weight\": 10}"),
          (t3, "increment", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 4
        tcString = "[t2 -> t3 -> t1 test case] delete(t2) incr(t3) incr(t1) test "
        bulkQueries = List(
          (t2, "delete", ""),
          (t3, "increment", "{\"weight\": 10}"),
          (t1, "increment", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "0", "weight" -> "10")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 5
        tcString = "[t3 -> t1 -> t2 test case] incr(t3) incr(t1) delete(t2) test "
        bulkQueries = List(
          (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
          (t1, "increment", "{\"time\": 10}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 6
        tcString = "[t3 -> t2 -> t1 test case] incr(t3) delete(t2) incr(t1) test "
        bulkQueries = List(
          (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""),
          (t1, "increment", "{\"time\": 10}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)


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
  "vetex tc" should {
    "tc1" in {
      running(FakeApplication()) {
        val ids = (0 until 3).toList
        val (serviceName, columnName) = (testServiceName, testColumnName)

        val data = vertexInsertsPayload(serviceName, columnName, ids)
        val payload = Json.parse(Json.toJson(data).toString)

        val req = FakeRequest(POST, s"/graphs/vertices/insert/$serviceName/$columnName").withBody(payload)
        println(s">> $req, $payload")
        val res = Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
        println(res)
        res.header.status must equalTo(200)
        Thread.sleep(asyncFlushInterval)
        println("---------------")

        val query = vertexQueryJson(serviceName, columnName, ids)
        val retFuture = route(FakeRequest(POST, "/graphs/getVertices").withJsonBody(query)).get

        val ret = contentAsJson(retFuture)
        println(">>>", ret)
        val fetched = ret.as[Seq[JsValue]]
        for {
          (d, f) <- data.zip(fetched)
        } yield {
          (d \ "id") must beEqualTo((f \ "id"))
          ((d \ "props") \ "age") must beEqualTo(((f \ "props") \ "age"))
        }
      }
      true
    }
  }
}

