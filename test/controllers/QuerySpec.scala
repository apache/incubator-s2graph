package test.controllers

//import com.daumkakao.s2graph.core.models._

import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

class QuerySpec extends SpecCommon {
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
      val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
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
