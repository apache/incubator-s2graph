package test.controllers

import com.daumkakao.s2graph.core._

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
  protected val testColumnName = "user_id_test"
  lazy val TC_WAITING_TIME = 1200

  lazy val HTTP_REQ_WAITING_TIME = Duration(5000, MILLISECONDS)
  val asyncFlushInterval = 1000

  val createService =
    s"""
       |{
       |"serviceName" : "$testServiceName"
                                           |}
    """.stripMargin

  val createLabel = s"""
  {
    "label": "s2graph_label_test",
    "srcServiceName": "s2graph",
    "srcColumnName": "user_id_test",
    "srcColumnType": "long",
    "tgtServiceName": "s2graph",
    "tgtColumnName": "user_id_test",
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

  val NUM_OF_EACH_TEST = 10
  val TS = System.currentTimeMillis()
  def queryJson(id: Int) = {
    Json.parse( s"""{
      "srcVertices": [
      {
        "serviceName": "$testServiceName",
        "columnName": "$testColumnName",
        "id": $id
      }
      ],
      "steps": [
      [
      {
        "label": "$testLabelName",
        "direction": "out",
        "offset": 0,
        "limit": 10
      }
      ]
      ]
    }""")
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
    }
  }


  def runTC(tcNum: Int, tcString: String, opWithProps: List[(Long, String, String)], expected: Map[String, String]) = {
    for {
      i <- (1 to NUM_OF_EACH_TEST)
    } {
      val srcId = tcNum + i
      val tgtId = srcId + 1000
      val maxTs = opWithProps.map(t => t._1).max

      /** insert edges */
      println(s"---- TC${tcNum}_init ----")
      val bulkEdge = (for ((ts, op, props) <- opWithProps) yield {
        List(ts, op, "e", srcId, tgtId, testLabelName, props).mkString("\t")
      }).mkString("\n")

      val req = FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdge)
      println(s">> $req, $bulkEdge")
      val res = Await.result(route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdge)).get, HTTP_REQ_WAITING_TIME)

      res.header.status must equalTo(200)
      Thread.sleep(asyncFlushInterval)
      println(s"---- TC${tcNum}_init ----")

      val query = queryJson(srcId)
      val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(query)).get
      val jsResult = commonCheck(ret)


      val results = jsResult \ "results"
      val deegrees = (jsResult \ "degrees").as[List[JsObject]]
      val propsLs = (results \\ "props").seq
      (deegrees.head \ LabelMeta.degree.name).as[Int] must equalTo(1)

      (results \\ "from").seq.last.toString must equalTo(srcId.toString)
      (results \\ "to").seq.last.toString must equalTo(tgtId.toString)
      (results \\ "_timestamp").seq.last.as[Long] must equalTo(maxTs)
      for ((key, expectedVal) <- expected) {
        propsLs.last.as[JsObject].keys.contains(key) must equalTo(true)
        (propsLs.last \ key).toString must equalTo(expectedVal)
      }
      Await.result(ret, HTTP_REQ_WAITING_TIME)
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

        //        tcNum = 1
        //
        //        tcString = "[t1 -> t3 -> t2 test case] incr(t0) incr(t2) delete(t1) test"
        //        bulkQueries = List(
        //          (t1, "increment", "{\"weight\": 10}"),
        //          (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
        //          (t2, "delete", ""))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 2
        //
        //        tcString = "[t1 -> t2 -> t3 test case] incr(t1) delete(t2) incr(t3) test"
        //        bulkQueries = List(
        //          (t1, "increment", "{\"weight\": 10}"),
        //          (t2, "delete", ""),
        //          (t3, "increment", "{\"time\": 10, \"weight\": 20}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 3
        //        tcString = "[t2 -> t1 -> t3 test case] delete(t2) incr(t1) incr(t3) test "
        //        bulkQueries = List(
        //          (t2, "delete", ""),
        //          (t1, "increment", "{\"weight\": 10}"),
        //          (t3, "increment", "{\"time\": 10, \"weight\": 20}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 4
        //        tcString = "[t2 -> t3 -> t1 test case] delete(t2) incr(t3) incr(t1) test "
        //        bulkQueries = List(
        //          (t2, "delete", ""),
        //          (t3, "increment", "{\"weight\": 10}"),
        //          (t1, "increment", "{\"time\": 10, \"weight\": 20}"))
        //        expected = Map("time" -> "0", "weight" -> "10")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 5
        tcString = "[t3 -> t1 -> t2 test case] incr(t3) incr(t1) delete(t2) test "
        bulkQueries = List(
          (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
          (t1, "increment", "{\"time\": 10}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        //        tcNum = 6
        //        tcString = "[t3 -> t2 -> t1 test case] incr(t3) delete(t2) incr(t1) test "
        //        bulkQueries = List(
        //          (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
        //          (t2, "delete", ""),
        //          (t1, "increment", "{\"time\": 10}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //
        //        tcNum = 7
        //        tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
        //        bulkQueries = List(
        //          (t1, "insert", "{\"time\": 10}"),
        //          (t2, "delete", ""),
        //          (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 8
        //        tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
        //        bulkQueries = List(
        //          (t1, "insert", "{\"time\": 10}"),
        //          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
        //          (t2, "delete", ""))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 9
        //        tcString = "[t3 -> t2 -> t1 test case] insert(t3) delete(t2) insert(t1) test "
        //        bulkQueries = List(
        //          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
        //          (t2, "delete", ""),
        //          (t1, "insert", "{\"time\": 10}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 10
        //        tcString = "[t3 -> t1 -> t2 test case] insert(t3) insert(t1) delete(t2) test "
        //        bulkQueries = List(
        //          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
        //          (t1, "insert", "{\"time\": 10}"),
        //          (t2, "delete", ""))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 11
        //        tcString = "[t2 -> t1 -> t3 test case] delete(t2) insert(t1) insert(t3) test"
        //        bulkQueries = List(
        //          (t2, "delete", ""),
        //          (t1, "insert", "{\"time\": 10}"),
        //          (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 12
        //        tcString = "[t2 -> t3 -> t1 test case] delete(t2) insert(t3) insert(t1) test "
        //        bulkQueries = List(
        //          (t2, "delete", ""),
        //          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
        //          (t1, "insert", "{\"time\": 10}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 13
        //        tcString = "[t1 -> t2 -> t3 test case] update(t1) delete(t2) update(t3) test "
        //        bulkQueries = List(
        //          (t1, "update", "{\"time\": 10}"),
        //          (t2, "delete", ""),
        //          (t3, "update", "{\"time\": 10, \"weight\": 20}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //        tcNum = 14
        //        tcString = "[t1 -> t3 -> t2 test case] update(t1) update(t3) delete(t2) test "
        //        bulkQueries = List(
        //          (t1, "update", "{\"time\": 10}"),
        //          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
        //          (t2, "delete", ""))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //        tcNum = 15
        //        tcString = "[t2 -> t1 -> t3 test case] delete(t2) update(t1) update(t3) test "
        //        bulkQueries = List(
        //          (t2, "delete", ""),
        //          (t1, "update", "{\"time\": 10}"),
        //          (t3, "update", "{\"time\": 10, \"weight\": 20}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //        tcNum = 16
        //        tcString = "[t2 -> t3 -> t1 test case] delete(t2) update(t3) update(t1) test"
        //        bulkQueries = List(
        //          (t2, "delete", ""),
        //          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
        //          (t1, "update", "{\"time\": 10}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //        tcNum = 17
        //        tcString = "[t3 -> t2 -> t1 test case] update(t3) delete(t2) update(t1) test "
        //        bulkQueries = List(
        //          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
        //          (t2, "delete", ""),
        //          (t1, "update", "{\"time\": 10}"))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //        tcNum = 18
        //        tcString = "[t3 -> t1 -> t2 test case] update(t3) update(t1) delete(t2) test "
        //        bulkQueries = List(
        //          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
        //          (t1, "update", "{\"time\": 10}"),
        //          (t2, "delete", ""))
        //        expected = Map("time" -> "10", "weight" -> "20")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)
        //
        //        tcNum = 19
        //        tcString = "[t5 -> t1 -> t3 -> t2 -> t4 test case] update(t5) insert(t1) insert(t3) delete(t2) update(t4) test "
        //        bulkQueries = List(
        //          (t5, "update", "{\"is_blocked\": true}"),
        //          (t1, "insert", "{\"is_hidden\": false}"),
        //          (t3, "insert", "{\"is_hidden\": false, \"weight\": 10}"),
        //          (t2, "delete", ""),
        //          (t4, "update", "{\"time\": 1, \"weight\": -10}"))
        //        expected = Map("time" -> "1", "weight" -> "-10", "is_hidden" -> "false", "is_blocked" -> "true")
        //
        //        runTC(tcNum, tcString, bulkQueries, expected)



        true
      }
    }
  }
  //  def spec =
  //    new Specification {
  //      sequential
  //
  //      /**
  //       * Each TC init/logic/after step waiting time to complete
  //       */
  //      //      lazy val TC_WAITING_TIME = if (Config.CLIENT_AGGREGATE_BUFFER_FLUSH_TIME >= 500) 1500 else Config.CLIENT_AGGREGATE_BUFFER_FLUSH_TIME * 3 // in milliseconds
  //      lazy val TC_WAITING_TIME = 1200
  //      /**
  //       * Each http request waiting time to complete
  //       */
  //      lazy val HTTP_REQ_WAITING_TIME = Duration(5000, MILLISECONDS)
  //      val asyncFlushInterval = 1000
  //      // in millis
  //      val curTime = System.currentTimeMillis
  //      val t1 = curTime + 0
  //      val t2 = curTime + 1
  //      val t3 = curTime + 2
  //      val t4 = curTime + 3
  //      val t5 = curTime + 4
  //
  //
  //      def justHttpCheck(rslt: Future[play.api.mvc.Result]) = {
  //        status(rslt) must equalTo(OK)
  //        contentType(rslt) must beSome.which(_ == "text/plain")
  //
  //        Await.result(rslt, HTTP_REQ_WAITING_TIME)
  //      }
  //
  //      def runTC(tcNum: Int, tcString: String, opWithProps: List[(Long, String, String)], expected: Map[String, String]) = {
  //        val rets = for {
  //          i <- (1 to 10)
  //        } yield {
  //          val srcId = (tcNum + i).toString
  //          val tgtId = (tcNum + 1000).toString
  //          val maxTs = opWithProps.map(t => t._1).max
  //
  //          def init = {
  //            println(s"---- TC${tcNum}_init ----")
  //            val bulkEdge = (for ((ts, op, props) <- opWithProps) yield {
  //              List(ts, op, "e", srcId, tgtId, testLabelName, props).mkString("\t")
  //            }).mkString("\n")
  //
  //            val req = FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdge)
  //            println(s">> $req, $bulkEdge")
  //            val res = Await.result(route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdge)).get, HTTP_REQ_WAITING_TIME)
  //
  //            res.header.status must equalTo(200)
  //            Thread.sleep(asyncFlushInterval)
  //            println(s"---- TC${tcNum}_init ----")
  //          }
  //          def clean = {}
  //
  //          tcString in new WithTestApplication(init = init, after = clean, stepWaiting = TC_WAITING_TIME) {
  //            val simpleQuery =
  //              s"""
  //          {
  //              "srcVertices": [
  //                  {
  //                      "serviceName": "$testServiceName",
  //                      "columnName": "$testColumnName",
  //                      "id": $srcId
  //                  }
  //              ],
  //              "steps": [
  //                  [
  //                      {
  //                          "label": "$testLabelName",
  //                          "direction": "out",
  //                          "offset": 0,
  //                          "limit": 10
  //                      }
  //                  ]
  //              ]
  //          }
  //           """.stripMargin
  //
  //            val rslt = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(Json.parse(simpleQuery))).get
  //
  //            val jsResult = commonCheck(rslt)
  //            val results = jsResult \ "results"
  //            val deegrees = (jsResult \ "degrees").as[List[JsObject]]
  //            val propsLs = (results \\ "props").seq
  //            (deegrees.head \ LabelMeta.degree.name).as[Int] must equalTo(1)
  //            //          propsLs.head.as[JsObject].keys.contains(LabelMeta.degree.name) must equalTo(true)
  //            //          (propsLs.head.as[JsObject] \ LabelMeta.degree.name).as[Int] must equalTo(1)
  //
  //            (results \\ "from").seq.last.toString must equalTo(srcId)
  //            (results \\ "to").seq.last.toString must equalTo(tgtId)
  //            (results \\ "_timestamp").seq.last.as[Long] must equalTo(maxTs)
  //            for ((key, expectedVal) <- expected) {
  //              propsLs.last.as[JsObject].keys.contains(key) must equalTo(true)
  //              (propsLs.last \ key).toString must equalTo(expectedVal)
  //            }
  //            Await.result(rslt, HTTP_REQ_WAITING_TIME)
  //          }
  //          rets(0)
  //        }
  //      }
  //
  //      /**
  //       * Response result common check logic
  //       * @param rslt
  //       * @return
  //       */
  //      def commonCheck(rslt: Future[play.api.mvc.Result]): JsValue = {
  //        status(rslt) must equalTo(OK)
  //        contentType(rslt) must beSome.which(_ == "application/json")
  //        val jsRslt = contentAsJson(rslt)
  //        println("======")
  //        println(jsRslt)
  //        println("======")
  //        jsRslt.as[JsObject].keys.contains("size") must equalTo(true)
  //        (jsRslt \ "size").as[Int] must greaterThan(0)
  //        jsRslt.as[JsObject].keys.contains("results") must equalTo(true)
  //        val jsRsltsObj = jsRslt \ "results"
  //        println(s"$jsRsltsObj")
  //        jsRsltsObj.as[JsArray].value(0).as[JsObject].keys.contains("from") must equalTo(true)
  //        jsRsltsObj.as[JsArray].value(0).as[JsObject].keys.contains("to") must equalTo(true)
  //        jsRsltsObj.as[JsArray].value(0).as[JsObject].keys.contains("_timestamp") must equalTo(true)
  //        jsRsltsObj.as[JsArray].value(0).as[JsObject].keys.contains("props") must equalTo(true)
  //
  //        jsRslt
  //      }

  //      "Increment/Delete Application" should {
  //
  //                "[TC1]" in {
  //                  val tcNum = 1
  //
  //                  val tcString = "[t1 -> t3 -> t2 test case] incr(t0) incr(t2) delete(t1) test"
  //                  val bulkQueries = List(
  //                    (t1, "increment", "{\"weight\": 10}"),
  //                    (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
  //                    (t2, "delete", ""))
  //                  val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                  runTC(tcNum, tcString, bulkQueries, expected)
  //                }
  //
  //                "[TC2]" in {
  //                  val tcNum = 2
  //
  //                  val tcString = "[t1 -> t2 -> t3 test case] incr(t1) delete(t2) incr(t3) test"
  //                  val bulkQueries = List(
  //                    (t1, "increment", "{\"weight\": 10}"),
  //                    (t2, "delete", ""),
  //                    (t3, "increment", "{\"time\": 10, \"weight\": 20}"))
  //                  val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                  runTC(tcNum, tcString, bulkQueries, expected)
  //                }
  //
  //                "[TC3]" in {
  //                  val tcNum = 3
  //                  val tcString = "[t2 -> t1 -> t3 test case] delete(t2) incr(t1) incr(t3) test "
  //                  val bulkQueries = List(
  //                    (t2, "delete", ""),
  //                    (t1, "increment", "{\"weight\": 10}"),
  //                    (t3, "increment", "{\"time\": 10, \"weight\": 20}"))
  //                  val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                  runTC(tcNum, tcString, bulkQueries, expected)
  //                }
  //
  //                "[TC4]" in {
  //                  val tcNum = 4
  //                  val tcString = "[t2 -> t3 -> t1 test case] delete(t2) incr(t3) incr(t1) test "
  //                  val bulkQueries = List(
  //                    (t2, "delete", ""),
  //                    (t3, "increment", "{\"weight\": 10}"),
  //                    (t1, "increment", "{\"time\": 10, \"weight\": 20}"))
  //                  val expected = Map("time" -> "0", "weight" -> "10")
  //
  //                  runTC(tcNum, tcString, bulkQueries, expected)
  //                }
  //
  //                "[TC5]" in {
  //                  val tcNum = 5
  //                  val tcString = "[t3 -> t1 -> t2 test case] incr(t3) incr(t1) delete(t2) test "
  //                  val bulkQueries = List(
  //                    (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
  //                    (t1, "increment", "{\"time\": 10}"),
  //                    (t2, "delete", ""))
  //                  val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                  runTC(tcNum, tcString, bulkQueries, expected)
  //                }
  //                "[TC6]" in {
  //                  val tcNum = 6
  //                  val tcString = "[t3 -> t2 -> t1 test case] incr(t3) delete(t2) incr(t1) test "
  //                  val bulkQueries = List(
  //                    (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
  //                    (t2, "delete", ""),
  //                    (t1, "increment", "{\"time\": 10}"))
  //                  val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                  runTC(tcNum, tcString, bulkQueries, expected)
  //                }
  //
  //      }
  //       -- Increment / Delete Application
  //            "Insert/Delete Application" should {
  //              "[TC7]" in {
  //                val tcNum = 7
  //                val tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
  //                val bulkQueries = List(
  //                  (t1, "insert", "{\"time\": 10}"),
  //                  (t2, "delete", ""),
  //                  (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC8]" in {
  //                val tcNum = 8
  //                val tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
  //                val bulkQueries = List(
  //                  (t1, "insert", "{\"time\": 10}"),
  //                  (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
  //                  (t2, "delete", ""))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC9]" in {
  //                val tcNum = 9
  //                val tcString = "[t3 -> t2 -> t1 test case] insert(t3) delete(t2) insert(t1) test "
  //                val bulkQueries = List(
  //                  (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
  //                  (t2, "delete", ""),
  //                  (t1, "insert", "{\"time\": 10}"))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC10]" in {
  //                val tcNum = 10
  //                val tcString = "[t3 -> t1 -> t2 test case] insert(t3) insert(t1) delete(t2) test "
  //                val bulkQueries = List(
  //                  (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
  //                  (t1, "insert", "{\"time\": 10}"),
  //                  (t2, "delete", ""))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC11]" in {
  //                val tcNum = 11
  //                val tcString = "[t2 -> t1 -> t3 test case] delete(t2) insert(t1) insert(t3) test"
  //                val bulkQueries = List(
  //                  (t2, "delete", ""),
  //                  (t1, "insert", "{\"time\": 10}"),
  //                  (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC12]" in {
  //                val tcNum = 12
  //                val tcString = "[t2 -> t3 -> t1 test case] delete(t2) insert(t3) insert(t1) test "
  //                val bulkQueries = List(
  //                  (t2, "delete", ""),
  //                  (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
  //                  (t1, "insert", "{\"time\": 10}"))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //            } // -- Insert / Delete Application
  //
  //            "Update / Delete Application" should {
  //              "[TC13]" in {
  //                val tcNum = 13
  //                val tcString = "[t1 -> t2 -> t3 test case] update(t1) delete(t2) update(t3) test "
  //                val bulkQueries = List(
  //                  (t1, "update", "{\"time\": 10}"),
  //                  (t2, "delete", ""),
  //                  (t3, "update", "{\"time\": 10, \"weight\": 20}"))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC14]" in {
  //                val tcNum = 14
  //                val tcString = "[t1 -> t3 -> t2 test case] update(t1) update(t3) delete(t2) test "
  //                val bulkQueries = List(
  //                  (t1, "update", "{\"time\": 10}"),
  //                  (t3, "update", "{\"time\": 10, \"weight\": 20}"),
  //                  (t2, "delete", ""))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC15]" in {
  //                val tcNum = 15
  //                val tcString = "[t2 -> t1 -> t3 test case] delete(t2) update(t1) update(t3) test "
  //                val bulkQueries = List(
  //                  (t2, "delete", ""),
  //                  (t1, "update", "{\"time\": 10}"),
  //                  (t3, "update", "{\"time\": 10, \"weight\": 20}"))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC16]" in {
  //                val tcNum = 16
  //                val tcString = "[t2 -> t3 -> t1 test case] delete(t2) update(t3) update(t1) test"
  //                val bulkQueries = List(
  //                  (t2, "delete", ""),
  //                  (t3, "update", "{\"time\": 10, \"weight\": 20}"),
  //                  (t1, "update", "{\"time\": 10}"))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC17]" in {
  //                val tcNum = 17
  //                val tcString = "[t3 -> t2 -> t1 test case] update(t3) delete(t2) update(t1) test "
  //                val bulkQueries = List(
  //                  (t3, "update", "{\"time\": 10, \"weight\": 20}"),
  //                  (t2, "delete", ""),
  //                  (t1, "update", "{\"time\": 10}"))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //              "[TC18]" in {
  //                val tcNum = 18
  //                val tcString = "[t3 -> t1 -> t2 test case] update(t3) update(t1) delete(t2) test "
  //                val bulkQueries = List(
  //                  (t3, "update", "{\"time\": 10, \"weight\": 20}"),
  //                  (t1, "update", "{\"time\": 10}"),
  //                  (t2, "delete", ""))
  //                val expected = Map("time" -> "10", "weight" -> "20")
  //
  //                runTC(tcNum, tcString, bulkQueries, expected)
  //              }
  //            } // -- Update / Delete Application
  //
  //      "Composite Application" should {
  //        "[TC19]" in {
  //          val tcNum = 19
  //          val tcString = "[t5 -> t1 -> t3 -> t2 -> t4 test case] update(t5) insert(t1) insert(t3) delete(t2) update(t4) test "
  //          val bulkQueries = List(
  //            (t5, "update", "{\"is_blocked\": true}"),
  //            (t1, "insert", "{\"is_hidden\": false}"),
  //            (t3, "insert", "{\"is_hidden\": false, \"weight\": 10}"),
  //            (t2, "delete", ""),
  //            (t4, "update", "{\"time\": 1, \"weight\": -10}"))
  //          val expected = Map("time" -> "1", "weight" -> "-10", "is_hidden" -> "false", "is_blocked" -> "true")
  //
  //          runTC(tcNum, tcString, bulkQueries, expected)
  //        }
  //      } // -- Composite Application
  //
  //    }
}

