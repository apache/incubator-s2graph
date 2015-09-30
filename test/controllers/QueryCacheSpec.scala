package test.controllers

//import com.kakao.s2graph.core.models._
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

class QueryCacheSpec extends SpecCommon {
  init()

  "cache test" should {
    def queryWithTTL(id: Int, cacheTTL: Long) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [[ {
            "label": "${testLabelName}",
            "direction": "out",
            "offset": 0,
            "limit": 10,
            "cacheTTL": ${cacheTTL},
            "scoring": {"weight": 1} }]]
          }""")

    def getEdges(queryJson: JsValue): JsValue = {
      var ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
      contentAsJson(ret)
    }

    // init
    running(FakeApplication()) {
      // insert bulk and wait ..
      val bulkEdges: String = Seq(
        Seq("1", "insert", "e", "0", "2", "s2graph_label_test", "{}").mkString("\t"),
        Seq("1", "insert", "e", "1", "2", "s2graph_label_test", "{}").mkString("\t")
      ).mkString("\n")

      val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges)).get
      val jsRslt = contentAsJson(ret)
      Thread.sleep(asyncFlushInterval)
    }

    "tc1: query with {id: 0, ttl: 1000}" in {
      running(FakeApplication()) {
        var jsRslt = getEdges(queryWithTTL(0, 1000))
        var cacheRemain = (jsRslt \\ "cacheRemain").head
        cacheRemain.as[Int] must greaterThan(500)

        // get edges from cache after wait 500ms
        Thread.sleep(500)
        val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryWithTTL(0, 1000))).get
        jsRslt = contentAsJson(ret)
        cacheRemain = (jsRslt \\ "cacheRemain").head
        cacheRemain.as[Int] must lessThan(500)
      }
    }

    "tc2: query with {id: 1, ttl: 3000}" in {
      running(FakeApplication()) {
        var jsRslt = getEdges(queryWithTTL(1, 3000))
        var cacheRemain = (jsRslt \\ "cacheRemain").head
        // before update: is_blocked is false
        (jsRslt \\ "is_blocked").head must equalTo(JsBoolean(false))

        val bulkEdges = Seq(
          Seq("2", "update", "e", "0", "2", "s2graph_label_test", "{\"is_blocked\": true}").mkString("\t"),
          Seq("2", "update", "e", "1", "2", "s2graph_label_test", "{\"is_blocked\": true}").mkString("\t")
        ).mkString("\n")

        // update edges with {is_blocked: true}
        val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges)).get
        jsRslt = contentAsJson(ret)

        Thread.sleep(asyncFlushInterval)

        // prop 'is_blocked' still false, cause queryResult on cache
        jsRslt = getEdges(queryWithTTL(1, 3000))
        (jsRslt \\ "is_blocked").head must equalTo(JsBoolean(false))

        // after wait 3000ms prop 'is_blocked' is updated to true, cache cleared
        Thread.sleep(3000)
        jsRslt = getEdges(queryWithTTL(1, 3000))
        (jsRslt \\ "is_blocked").head must equalTo(JsBoolean(true))
      }
    }
  }
}
