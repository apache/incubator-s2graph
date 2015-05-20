package test.controllers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.rest.actors._
import com.daumkakao.s2graph.rest.config.Config
import controllers.{AdminController, RequestParser}
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import play.api.libs.json.{ JsArray, JsObject, Json }
import play.api.test.Helpers._
import play.api.test._
import scala.Array.canBuildFrom
import scala.concurrent.ExecutionContext

/**
 * Graph Query test case specification
 * @author June.k( Junki Kim, june.k@daumkakao.com )
 * @since 2015. 1. 2.
 */

class QuerySpec extends QuerySpecificationBase with Matchers {
  def spec =
    new Specification {
      sequential

      "Application" should {
        "[getEdgesGrouped query] test (function + response format check)" in new WithApplication {
          val simpleQuery =
            s"""
      {
          "srcVertices": [
              {
                  "serviceName": "$testServiceName",
                  "columnName": "$testColumnName",
                  "id": ${fromVertexIds(0)}
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

          val acts = route(FakeRequest(POST, "/graphs/getEdgesGrouped").withJsonBody(Json.parse(simpleQuery))).get

          //    logger.debug(s">> Social Controller test Acts result : ${contentAsJson(acts)}")
          status(acts) must equalTo(OK)
          contentType(acts) must beSome.which(_ == "application/json")
          val jsRslt = contentAsJson(acts)
          println("======")
          println(jsRslt)
          println("======")
          jsRslt.as[JsObject].keys.contains("size") must equalTo(true)
          jsRslt.as[JsObject].keys.contains("results") must equalTo(true)
          (jsRslt \ "size").as[Int] must greaterThan(0)
          ((jsRslt \ "results") \\ "name").seq(0).as[String] must equalTo("user_id")
          (((jsRslt \ "results") \\ "aggr").seq(0).as[JsObject] \ "ids").as[JsArray].value.map(_.as[String]).toList should containAllOf(Seq[String](fromVertexIds(0).toString))
        }

        "[getEdgesGroupedExcluded query] test (function + response format check)" in new WithApplication {
          val simpleQuery =
            s"""
      {
          "srcVertices": [
              {
                  "serviceName": "$testServiceName",
                  "columnName": "$testColumnName",
                  "id": ${fromVertexIds(0)}
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
              ],
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

          val acts = route(FakeRequest(POST, "/graphs/getEdgesGroupedExcludedFormatted").withJsonBody(Json.parse(simpleQuery))).get

          //    logger.debug(s">> Social Controller test Acts result : ${contentAsJson(acts)}")
          status(acts) must equalTo(OK)
          contentType(acts) must beSome.which(_ == "application/json")
          val jsRslt = contentAsJson(acts)
          println("======")
          println(jsRslt)
          println("======")
          jsRslt.as[JsObject].keys.contains("size") must equalTo(true)
          jsRslt.as[JsObject].keys.contains("results") must equalTo(true)
          (jsRslt \ "size").as[Int] must greaterThan(0)
          (jsRslt \ "size").as[Int] must equalTo(secondDepthVertexIds.length)
          ((jsRslt \ "results") \\ "name").seq(0).as[String] must equalTo("user_id")
          ((jsRslt \ "results") \\ "scoreSum").seq(0).as[Double] must equalTo(2.0f)
          (((jsRslt \ "results") \\ "aggr").seq(0).as[JsObject] \ "ids").as[JsArray].value.map(_.as[String]).toList should containAllOf(partialVertexIds.map(_.toString).toSeq)
        }

        "[getEdges query] test" in new WithApplication {
          val query =
            s"""
      {
          "srcVertices": [
              {
                  "serviceName": "$testServiceName",
                  "columnName": "$testColumnName",
                  "id": ${fromVertexIds(0)}
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

          val acts = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(Json.parse(query))).get

          //    logger.debug(s">> Social Controller test Acts result : ${contentAsJson(acts)}")
          status(acts) must equalTo(OK)
          contentType(acts) must beSome.which(_ == "application/json")
          val jsRslt = contentAsJson(acts)
          println("======")
          println(jsRslt)
          println("======")

          jsRslt.as[JsObject].keys.contains("size") must equalTo(true)
          jsRslt.as[JsObject].keys.contains("results") must equalTo(true)
          (jsRslt \ "size").as[Int] must greaterThan(0)
          (jsRslt \ "size").as[Int] must equalTo(toVertexIds.length)
          ((jsRslt \ "results") \\ "from").seq(0).toString must equalTo(s"${fromVertexIds(0)}")
        }
      }

    }

}

abstract class QuerySpecificationBase extends Specification with RequestParser {
  protected val testServiceName = "s2graph"
  protected val testLabelName = "s2graph_label_test"
  protected val testColumnName = "user_id"
  val asyncFlushInterval = 1500 // in mill

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
      |   "weight":0
      | }
      |}
    """.stripMargin

  // Generate sample vertex ids
  //val fromVertexIds = Array(1, 2, 3, 4, 5)
  val fromVertexIds = Array(1, 2)
  val toVertexIds = Array(10, 20, 30, 40, 50)
  // select some ids from vertice ids and add new ids
  val partialVertexIds = toVertexIds.partition(_ % 20 == 0)._1

  val secondDepthVertexIds = Array(100, 200, 300, 400)

  /**
   * Generate all combinations between from and to
   */
  def edgesInsertInitial() = {
    var idx = 0
    for {
      start <- fromVertexIds
      to <- toVertexIds
    } yield {
      idx += 1
      val curTime = System.currentTimeMillis + idx
      s"""
        |{
        | "from":$start,
        | "to":$to,
        | "label":"$testLabelName",
        | "timestamp":$curTime,
        | "props": {"time":$curTime, "weight":100}
        |}
      """.stripMargin
    }
  }

  /**
   * Generate all combinations between to and secondDepth
   */
  def edgesInsert2ndDepth() = {
    var idx = 0
    for {
      start <- partialVertexIds
      to <- secondDepthVertexIds
    } yield {
      idx += 1
      Thread.sleep(1)
      val curTime = System.currentTimeMillis + idx
      s"""
        |{
        | "from":$start,
        | "to":$to,
        | "label":"$testLabelName",
        | "timestamp":$curTime,
        | "props": {"time":$curTime, "weight":100}
        |}
      """.stripMargin
    }
  }

  def initialize = {
    running(FakeApplication()) {
      Graph(Config.conf.underlying)(ExecutionContext.Implicits.global)

      // 1. createService
      var result = AdminController.createServiceInner(Json.parse(createService))
      println(s">> Service created : $createService, $result")
      // 2. createLabel
      try {
        AdminController.createLabelInner(Json.parse(createLabel))
        println(s">> Label created : $createLabel, $result")
      } catch {
        case e: Throwable =>
      }

      // 3. insert edges
      val jsArrStr = s"[${edgesInsertInitial.mkString(",")}]"
      println(s">> 1st step Inserts : $jsArrStr")
      val inserts = toEdges(Json.parse(jsArrStr), "insert")
      Graph.mutateEdges(inserts)
      Thread.sleep(asyncFlushInterval)

      println(s"<< 1st step Inserted : $jsArrStr")

      val jsArrStr2nd = s"[${edgesInsert2ndDepth.mkString(",")}]"
      println(s">> 2nd step Inserts : $jsArrStr2nd")
      val inserts2nd = toEdges(Json.parse(jsArrStr2nd), "insert")
      Graph.mutateEdges(inserts2nd)
      Thread.sleep(asyncFlushInterval)
      println(s"<< 2nd step Inserted : $inserts2nd")
    }
  }

  def cleanup = {
    running(FakeApplication()) {

      Graph(Config.conf.underlying)(ExecutionContext.Implicits.global)

      // 1. delete edges
      val jsArrStr = s"[${edgesInsertInitial.mkString(",")}]"
      println(s"Deletes : $jsArrStr")
      val deletes = toEdges(Json.parse(jsArrStr), "delete")
      Graph.mutateEdges(deletes)
      Thread.sleep(asyncFlushInterval)

      val jsArrStr2nd = s"[${edgesInsert2ndDepth.mkString(",")}]"
      println(s">> 2nd step Deletes : $jsArrStr2nd")
      val deletes2nd = toEdges(Json.parse(jsArrStr2nd), "delete")
      Graph.mutateEdges(deletes2nd)
      Thread.sleep(asyncFlushInterval)
      println(s"<< 2nd step Deleted : $deletes2nd")

      // 2. delete label ( currently NOT supported )
//      var result = AdminController.deleteLabelInner(testLabelName)
//      println(s">> Label deleted : $testLabelName, $result")
      // 3. delete service ( currently NOT supported )
    }

  }

  //Do setup work here
  step {
    println("==== [QuerySpecificationBase] Doing setup work... ====")
    initialize
    success
  }

  //Include the real spec from the derived class
  include(spec)

  //Do shutdown work here
  step {
    println("===== [QuerySpecificationBase] Doing shutdown work... =====")
    cleanup
    success
  }

  /**
   * To be implemented in the derived class.  Returns the real specification
   * @return Specification
   */
  def spec: Specification
}
