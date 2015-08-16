package test.controllers

//import com.daumkakao.s2graph.core.models._

import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

import scala.concurrent.Await

class QuerySpec extends SpecCommon {
  init()

  "query test" should {
    running(FakeApplication()) {
      // insert bulk and wait ..
      val bulkEdges: String = Seq(
        Seq("1", "insert", "e", "0", "1", testLabelName, "{\"weight\": 10}").mkString("\t"),
        Seq("1", "insert", "e", "0", "2", testLabelName, "{\"weight\": 20}").mkString("\t"),
        Seq("1", "insert", "e", "2", "0", testLabelName, "{\"weight\": 30}").mkString("\t"),
        Seq("1", "insert", "e", "2", "1", testLabelName, "{\"weight\": 40}").mkString("\t")
      ).mkString("\n")
      val req = FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges)
      Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
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

    def queryTransform(id: Int, transforms: String) = Json.parse( s"""
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
              "transform": $transforms
            }
          ]]
        }""")

    def getEdges(queryJson: JsValue): JsValue = {
      val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
      contentAsJson(ret)
    }

    "get edge exclude" in {
      running(FakeApplication()) {
        val result = getEdges(query(0))
        (result \ "results").as[List[JsValue]].size must equalTo(1)
      }
    }

    "edge transform " in {
      running(FakeApplication()) {
        var result = getEdges(queryTransform(0, "[[\"_to\"]]"))
        (result \ "results").as[List[JsValue]].size must equalTo(2)

        result = getEdges(queryTransform(0, "[[\"weight\"]]"))
        (result \ "results").as[List[JsValue]].size must equalTo(4)
      }
    }
  }
}
