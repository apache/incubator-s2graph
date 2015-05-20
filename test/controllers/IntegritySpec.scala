package test.controllers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.rest.config.Config
import com.daumkakao.s2graph.rest.actors._
import com.twitter.io.exp.VarSource.Ok
import controllers.AdminController
import org.omg.CosNaming.NamingContextPackage.NotFound
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import org.specs2.execute.{ AsResult, Result }
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import play.api.libs.json.{ JsObject, JsValue, Json }
import play.api.test.Helpers._
import play.api.test.{ FakeApplication, FakeRequest, Helpers, WithApplication }
import play.api.libs.json.JsArray
import scala.concurrent.ExecutionContext


/**
 * Data Integrity Test specification
 * @author June.k( Junki Kim, june.k@kakao.com )
 * @since 15. 1. 5.
 */

class IntegritySpec extends IntegritySpecificationBase with Matchers {
  def spec =
    new Specification {
      sequential

      /**
       * Each TC init/logic/after step waiting time to complete
       */
//      lazy val TC_WAITING_TIME = if (Config.CLIENT_AGGREGATE_BUFFER_FLUSH_TIME >= 500) 1500 else Config.CLIENT_AGGREGATE_BUFFER_FLUSH_TIME * 3 // in milliseconds
      lazy val TC_WAITING_TIME = 1200
      /**
       * Each http request waiting time to complete
       */
      lazy val HTTP_REQ_WAITING_TIME = Duration(1000, MILLISECONDS)
      val asyncFlushInterval = 1500 // in millis
      val curTime = System.currentTimeMillis
      val t1 = curTime + 0
      val t2 = curTime + 1
      val t3 = curTime + 2
      val t4 = curTime + 3
      val t5 = curTime + 4
      /**
       * Response result common check logic
       * @param rslt
       * @return
       */
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

        jsRsltsObj
      }

      def justHttpCheck(rslt: Future[play.api.mvc.Result]) = {
        status(rslt) must equalTo(OK)
        contentType(rslt) must beSome.which(_ == "text/plain")

        Await.result(rslt, HTTP_REQ_WAITING_TIME)
      }
      def runTC(tcNum: Int, tcString: String, opWithProps: List[(Long, String, String)], expected: Map[String, String]) = {
        val srcId = tcNum.toString
        val tgtId = (srcId + 1000).toString
        val maxTs = opWithProps.map(t => t._1).max

        def init = {
          println(s"---- TC${tcNum}_init ----")
          for ((ts, op, props) <- opWithProps) {
            val bulkEdge = List(ts, op, "e", srcId, tgtId, testLabelName, props).mkString("\t")
            val req = FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdge)
            println(s">> $req, $bulkEdge")
            val res = Await.result(route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdge)).get, HTTP_REQ_WAITING_TIME)

            /**
             * since asynchbase hbase client flush interval is 1000 millis, we wait.
             */
            Thread.sleep(asyncFlushInterval)
            res.header.status must equalTo(200)
          }
          println(s"---- TC${tcNum}_init ----")
        }
        def clean = {
          val cleanTs = maxTs + 1
          val bulkQuery = List(cleanTs, "delete", "e", srcId, tgtId, testLabelName).mkString("\t")

          println(s"---- TC${tcNum}_cleanup ----")
          println(s"Cleanup Query : $bulkQuery")
          println(s"---- TC${tcNum}_cleanup ----")
          val rslt = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkQuery)).get

          justHttpCheck(rslt)
          Thread.sleep(1000)
        }

        tcString in new WithTestApplication(init = init, after = clean, stepWaiting = TC_WAITING_TIME) {
          val simpleQuery =
            s"""
          {
              "srcVertices": [
                  {
                      "serviceName": "$testServiceName",
                      "columnName": "$testColumnName",
                      "id": $srcId
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
          }
           """.stripMargin

          val rslt = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(Json.parse(simpleQuery))).get

          val jsRsltsObj = commonCheck(rslt)

          (jsRsltsObj \\ "from").seq(0).toString must equalTo(srcId)
          (jsRsltsObj \\ "to").seq(0).toString must equalTo(tgtId)
          (jsRsltsObj \\ "_timestamp").seq(0).as[Long] must equalTo(maxTs)
          for ((key, expectedVal) <- expected) {
            (jsRsltsObj \\ "props").seq(0).as[JsObject].keys.contains(key) must equalTo(true)
            ((jsRsltsObj \\ "props").seq(0) \ key).toString must equalTo(expectedVal)
          }
          Await.result(rslt, HTTP_REQ_WAITING_TIME)
        }
      }

      "Increment/Delete Application" should {

        "[TC1]" in {
          val tcNum = 1

          val tcString = "[t1 -> t3 -> t2 test case] incr(t0) incr(t2) delete(t1) test"
          val bulkQueries = List(
            (t1, "increment", "{\"weight\": 10}"),
            (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
            (t2, "delete", ""))
          val expected = Map("time" -> "10", "weight" -> "0")

          runTC(tcNum, tcString, bulkQueries, expected)
        }

        "[TC2]" in {
          val tcNum = 2

          val tcString = "[t1 -> t2 -> t3 test case] incr(t1) delete(t2) incr(t3) test"
          val bulkQueries = List(
            (t1, "increment", "{\"weight\": 10}"),
            (t2, "delete", ""),
            (t3, "increment", "{\"time\": 10, \"weight\": 20}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }

        "[TC3]" in {
          val tcNum = 3
          val tcString = "[t2 -> t1 -> t3 test case] delete(t2) incr(t1) incr(t3) test "
          val bulkQueries = List(
            (t2, "delete", ""),
            (t1, "increment", "{\"weight\": 10}"),
            (t3, "increment", "{\"time\": 10, \"weight\": 20}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }

        "[TC4]" in {
          val tcNum = 4
          val tcString = "[t2 -> t3 -> t1 test case] delete(t2) incr(t3) incr(t1) test "
          val bulkQueries = List(
            (t2, "delete", ""),
            (t3, "increment", "{\"weight\": 10}"),
            (t1, "increment", "{\"time\": 10, \"weight\": 20}"))
          val expected = Map("time" -> "0", "weight" -> "10")

          runTC(tcNum, tcString, bulkQueries, expected)
        }

        "[TC5]" in {
          val tcNum = 5
          val tcString = "[t3 -> t1 -> t2 test case] incr(t3) incr(t1) delete(t2) test "
          val bulkQueries = List(
            (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
            (t1, "increment", "{\"time\": 10}"),
            (t2, "delete", ""))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC6]" in {
          val tcNum = 6
          val tcString = "[t3 -> t2 -> t1 test case] incr(t3) delete(t2) incr(t1) test "
          val bulkQueries = List(
            (t3, "increment", "{\"time\": 10, \"weight\": 20}"),
            (t2, "delete", ""),
            (t1, "increment", "{\"time\": 10}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }

      }
      // -- Increment / Delete Application
      "Insert/Delete Application" should {
        "[TC7]" in {
          val tcNum = 7
          val tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
          val bulkQueries = List(
            (t1, "insert", "{\"time\": 10}"),
            (t2, "delete", ""),
            (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC8]" in {
          val tcNum = 8
          val tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
          val bulkQueries = List(
            (t1, "insert", "{\"time\": 10}"),
            (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
            (t2, "delete", ""))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC9]" in {
          val tcNum = 9
          val tcString = "[t3 -> t2 -> t1 test case] insert(t3) delete(t2) insert(t1) test "
          val bulkQueries = List(
            (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
            (t2, "delete", ""),
            (t1, "insert", "{\"time\": 10}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC10]" in {
          val tcNum = 10
          val tcString = "[t3 -> t1 -> t2 test case] insert(t3) insert(t1) delete(t2) test "
          val bulkQueries = List(
            (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
            (t1, "insert", "{\"time\": 10}"),
            (t2, "delete", ""))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC11]" in {
          val tcNum = 11
          val tcString = "[t2 -> t1 -> t3 test case] delete(t2) insert(t1) insert(t3) test"
          val bulkQueries = List(
            (t2, "delete", ""),
            (t1, "insert", "{\"time\": 10}"),
            (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC12]" in {
          val tcNum = 12
          val tcString = "[t2 -> t3 -> t1 test case] delete(t2) insert(t3) insert(t1) test "
          val bulkQueries = List(
            (t2, "delete", ""),
            (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
            (t1, "insert", "{\"time\": 10}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
      } // -- Insert / Delete Application

      "Update / Delete Application" should {
        "[TC13]" in {
          val tcNum = 13
          val tcString = "[t1 -> t2 -> t3 test case] update(t1) delete(t2) update(t3) test "
          val bulkQueries = List(
            (t1, "update", "{\"time\": 10}"),
            (t2, "delete", ""),
            (t3, "update", "{\"time\": 10, \"weight\": 20}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC14]" in {
          val tcNum = 14
          val tcString = "[t1 -> t3 -> t2 test case] update(t1) update(t3) delete(t2) test "
          val bulkQueries = List(
            (t1, "update", "{\"time\": 10}"),
            (t3, "update", "{\"time\": 10, \"weight\": 20}"),
            (t2, "delete", ""))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC15]" in {
          val tcNum = 15
          val tcString = "[t2 -> t1 -> t3 test case] delete(t2) update(t1) update(t3) test "
          val bulkQueries = List(
            (t2, "delete", ""),
            (t1, "update", "{\"time\": 10}"),
            (t3, "update", "{\"time\": 10, \"weight\": 20}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC16]" in {
          val tcNum = 16
          val tcString = "[t2 -> t3 -> t1 test case] delete(t2) update(t3) update(t1) test"
          val bulkQueries = List(
            (t2, "delete", ""),
            (t3, "update", "{\"time\": 10, \"weight\": 20}"),
            (t1, "update", "{\"time\": 10}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC17]" in {
          val tcNum = 17
          val tcString = "[t3 -> t2 -> t1 test case] update(t3) delete(t2) update(t1) test "
          val bulkQueries = List(
            (t3, "update", "{\"time\": 10, \"weight\": 20}"),
            (t2, "delete", ""),
            (t1, "update", "{\"time\": 10}"))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
        "[TC18]" in {
          val tcNum = 18
          val tcString = "[t3 -> t1 -> t2 test case] update(t3) update(t1) delete(t2) test "
          val bulkQueries = List(
            (t3, "update", "{\"time\": 10, \"weight\": 20}"),
            (t1, "update", "{\"time\": 10}"),
            (t2, "delete", ""))
          val expected = Map("time" -> "10", "weight" -> "20")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
      } // -- Update / Delete Application

      "Composite Application" should {
        "[TC19]" in {
          val tcNum = 19
          val tcString = "[t5 -> t1 -> t3 -> t2 -> t4 test case] update(t5) insert(t1) insert(t3) delete(t2) update(t4) test "
          val bulkQueries = List(
            (t5, "update", "{\"is_blocked\": true}"),
            (t1, "insert", "{\"is_hidden\": false}"),
            (t3, "insert", "{\"is_hidden\": false, \"weight\": 10}"),
            (t2, "delete", ""),
            (t4, "update", "{\"time\": 1, \"weight\": -10}"))
          val expected = Map("time" -> "1", "weight" -> "-10", "is_hidden" -> "false", "is_blocked" -> "true")

          runTC(tcNum, tcString, bulkQueries, expected)
        }
      } // -- Composite Application

    }
}

/**
 *
 * @param app
 * @param init This function run without play application context
 * @param after This function run without play application context
 */
class WithTestApplication(override val app: FakeApplication = FakeApplication(), init: => Unit, after: => Unit, stepWaiting: Int = 1000) extends WithApplication {
  override def around[T: AsResult](t: => T): Result = {
    Helpers.running(app) {

      init

      println(">> [init] Start waiting....")
      Thread.sleep(stepWaiting)
      println("<< [init] end waiting.")

      val rslt = AsResult.effectively(t)

      println(">> [TC] Start waiting....")
      Thread.sleep(stepWaiting)
      println("<< [TC] end waiting.")

      after

      println(">> [after] Start waiting....")
      Thread.sleep(stepWaiting)
      println("<< [after] end waiting.")

      rslt
    }
  }
}

abstract class IntegritySpecificationBase extends Specification {
  protected val testServiceName = "s2graph"
  protected val testLabelName = "s2graph_label_test"
  protected val testColumnName = "user_id"

  val createService =
    s"""
      |{
      | "serviceName" : "$testServiceName"
      |}
    """.stripMargin

  val createLabel =
    s"""
      |{
      | "label": "$testLabelName",
      | "srcServiceName": "$testServiceName",
      | "srcColumnName": "$testColumnName",
      | "srcColumnType": "long",
      | "tgtServiceName": "$testServiceName",
      | "tgtColumnName": "$testColumnName",
      | "tgtColumnType": "long",
      | "indexProps": {
      |   "time": 0,
      |   "weight":0,
      |   "is_hidden" : false,
      |   "is_blocked" : false
      | }, 
      | "consistencyLevel": "strong"
      |}
    """.stripMargin

  def initialize = {
    running(FakeApplication()) {

//      KafkaAggregatorActor.init()
      Graph(Config.conf.underlying)(ExecutionContext.Implicits.global)

      // 1. createService
      var result = AdminController.createServiceInner(Json.parse(createService))
      println(s">> Service created : $createService, $result")

      // 2. createLabel
      Label.findByName(testLabelName) match {
        case None =>
          result = AdminController.createLabelInner(Json.parse(createLabel))
          println(s">> Label created : $createLabel, $result")
        case Some(label) =>
          println(s">> Label already exist: $createLabel, $label")
      }

    }

  }
  def cleanup = {
    running(FakeApplication()) {

      //      Graph(Config.conf)(ExecutionContext.Implicits.global)
      //      // 1. delete label ( currently NOT supported )
      //      var result = AdminController.deleteLabelInner(testLabelName)
      //      println(s">> Label deleted : $testLabelName, $result")
      // 2. delete service ( currently NOT supported )

//      KafkaAggregatorActor.shutdown()
    }

  }

  //Do setup work here
  step {
    println("==== [IntegritySpecificationBase] Doing setup work... ====")
    initialize
    success
  }

  //Include the real spec from the derived class
  include(spec)

  //Do shutdown work here
  step {
    println("===== [IntegritySpecificationBase] Doing shutdown work... =====")
    cleanup
    success
  }

  /**
   * To be implemented in the derived class.  Returns the real specification
   * @return Specification
   */
  def spec: Specification
}
