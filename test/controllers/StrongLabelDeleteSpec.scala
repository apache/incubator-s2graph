package test.controllers

import controllers.EdgeController
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}
import test.controllers.SpecCommon

class StrongLabelDeleteSpec extends SpecCommon {
  init()

  def bulkEdges(startTs: Int = 0) = Seq(
    Seq(startTs + 1, "insert", "e", "0", "1", testLabelName2, s"""{"time": 10}""").mkString("\t"),
    Seq(startTs + 2, "insert", "e", "0", "1", testLabelName2, s"""{"time": 11}""").mkString("\t"),
    Seq(startTs + 3, "insert", "e", "0", "1", testLabelName2, s"""{"time": 12}""").mkString("\t"),
    Seq(startTs + 4, "insert", "e", "0", "2", testLabelName2, s"""{"time": 10}""").mkString("\t"),
    Seq(startTs + 5, "insert", "e", "10", "20", testLabelName2, s"""{"time": 10}""").mkString("\t"),
    Seq(startTs + 6, "insert", "e", "10", "21", testLabelName2, s"""{"time": 11}""").mkString("\t"),
    Seq(startTs + 7, "insert", "e", "11", "20", testLabelName2, s"""{"time": 12}""").mkString("\t"),
    Seq(startTs + 8, "insert", "e", "12", "20", testLabelName2, s"""{"time": 13}""").mkString("\t")
  ).mkString("\n")

  "strong label delete test" should {
    running(FakeApplication()) {
      // insert bulk and wait ..
      val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges())).get
      val jsRslt = contentAsJson(ret)
      Thread.sleep(asyncFlushInterval)
    }


    def query(id: Int, direction: String = "out", columnName: String = testColumnName) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$columnName",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName2}",
              "direction": "${direction}",
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

    "test strong consistency select" in {
      running(FakeApplication()) {
        var result = getEdges(query(0))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(2)
        result = getEdges(query(10))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(2)
        true
      }
    }

    "test strong consistency duration. insert -> delete -> insert" in {
      running(FakeApplication()) {
        val ts0 = 1
        val ts1 = 2
        val ts2 = 3

        val edges = Seq(
          Seq(5, "insert", "edge", "-10", "-20", testLabelName2).mkString("\t"),
          Seq(10, "delete", "edge", "-10", "-20", testLabelName2).mkString("\t"),
          Seq(20, "insert", "edge", "-10", "-20", testLabelName2).mkString("\t")
        ).mkString("\n")

        val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(edges)).get
        val jsRslt = contentAsJson(ret)

        Thread.sleep(asyncFlushInterval)
        var result = getEdges(query(-10))

        println(result)

        true
      }
    }

    "test strong consistency deleteAll" in {
      running(FakeApplication()) {
        val deletedAt = 100
        var result = getEdges(query(20, "in", testTgtColumnName))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(3)



        val json = Json.arr(Json.obj("label" -> testLabelName2,
          "direction" -> "in", "ids" -> Json.arr("20"), "timestamp" -> deletedAt))
        println(json)
        EdgeController.deleteAllInner(json)
        Thread.sleep(asyncFlushInterval)


        result = getEdges(query(11, "out"))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(0)

        result = getEdges(query(12, "out"))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(0)

        result = getEdges(query(10, "out"))
        println(result)
        // 10 -> out -> 20 should not be in result.
        (result \ "results").as[List[JsValue]].size must equalTo(1)
        (result \\ "to").size must equalTo(1)
        (result \\ "to").head.as[String] must equalTo("21")


        result = getEdges(query(20, "in", testTgtColumnName))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(0)


        val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges(startTs = deletedAt + 1))).get
        val jsRslt = contentAsJson(ret)
        Thread.sleep(asyncFlushInterval)

        result = getEdges(query(20, "in", testTgtColumnName))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(3)

        true

      }
    }
  }
}

