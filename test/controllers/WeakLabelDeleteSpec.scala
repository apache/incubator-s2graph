package test.controllers

import controllers.{EdgeController}
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

class WeakLabelDeleteSpec extends SpecCommon {
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
      val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
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

