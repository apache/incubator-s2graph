//package controllers
//
//import play.api.libs.json._
//import play.api.test.Helpers._
//import play.api.test.{FakeApplication, FakeRequest}
//
//class WeakLabelDeleteSpec extends SpecCommon {
//  init()
//
//  def bulkEdges(startTs: Int = 0) = Seq(
//    Seq(startTs + 1, "insert", "e", "0", "1", testLabelNameWeak, s"""{"time": 10}""").mkString("\t"),
//    Seq(startTs + 2, "insert", "e", "0", "1", testLabelNameWeak, s"""{"time": 11}""").mkString("\t"),
//    Seq(startTs + 3, "insert", "e", "0", "1", testLabelNameWeak, s"""{"time": 12}""").mkString("\t"),
//    Seq(startTs + 4, "insert", "e", "0", "2", testLabelNameWeak, s"""{"time": 10}""").mkString("\t"),
//    Seq(startTs + 5, "insert", "e", "10", "20", testLabelNameWeak, s"""{"time": 10}""").mkString("\t"),
//    Seq(startTs + 6, "insert", "e", "10", "21", testLabelNameWeak, s"""{"time": 11}""").mkString("\t"),
//    Seq(startTs + 7, "insert", "e", "11", "20", testLabelNameWeak, s"""{"time": 12}""").mkString("\t"),
//    Seq(startTs + 8, "insert", "e", "12", "20", testLabelNameWeak, s"""{"time": 13}""").mkString("\t")
//  ).mkString("\n")
//
//  "weak label delete test" should {
//    running(FakeApplication()) {
//      // insert bulk and wait ..
//      val jsResult = contentAsJson(EdgeController.mutateAndPublish(bulkEdges(), withWait = true))
//    }
//
//
//    def query(id: Int, direction: String = "out", columnName: String = testColumnName) = Json.parse(
//      s"""
//        { "srcVertices": [
//          { "serviceName": "$testServiceName",
//            "columnName": "$columnName",
//            "id": ${id}
//           }],
//          "steps": [
//          [ {
//              "label": "${testLabelNameWeak}",
//              "direction": "${direction}",
//              "offset": 0,
//              "limit": 10,
//              "duplicate": "raw"
//            }
//          ]]
//        }""")
//
//    def getEdges(queryJson: JsValue): JsValue = {
//      val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
//      contentAsJson(ret)
//    }
//
//    "test weak consistency select" in {
//      running(FakeApplication()) {
//        var result = getEdges(query(0))
//        println(result)
//        (result \ "results").as[List[JsValue]].size must equalTo(4)
//        result = getEdges(query(10))
//        println(result)
//        (result \ "results").as[List[JsValue]].size must equalTo(2)
//        true
//      }
//    }
//
//    "test weak consistency delete" in {
//      running(FakeApplication()) {
//        var result = getEdges(query(0))
//        println(result)
//
//        /** expect 4 edges */
//        (result \ "results").as[List[JsValue]].size must equalTo(4)
//        val edges = (result \ "results").as[List[JsObject]]
//
//        contentAsJson(EdgeController.tryMutates(Json.toJson(edges), "delete", withWait = true))
//
//        /** expect noting */
//        result = getEdges(query(0))
//        println(result)
//        (result \ "results").as[List[JsValue]].size must equalTo(0)
//
//        /** insert should be ignored */
//        contentAsJson(EdgeController.tryMutates(Json.toJson(edges), "insert", withWait = true))
//
//        result = getEdges(query(0))
//        (result \ "results").as[List[JsValue]].size must equalTo(0)
//      }
//    }
//
//    "test weak consistency deleteAll" in {
//      running(FakeApplication()) {
//        val deletedAt = 100
//        var result = getEdges(query(20, "in", testTgtColumnName))
//        println(result)
//        (result \ "results").as[List[JsValue]].size must equalTo(3)
//
//        val json = Json.arr(Json.obj("label" -> testLabelNameWeak,
//          "direction" -> "in", "ids" -> Json.arr("20"), "timestamp" -> deletedAt))
//        println(json)
//        contentAsString(EdgeController.deleteAllInner(json, withWait = true))
//
//
//        result = getEdges(query(11, "out"))
//        (result \ "results").as[List[JsValue]].size must equalTo(0)
//
//        result = getEdges(query(12, "out"))
//        (result \ "results").as[List[JsValue]].size must equalTo(0)
//
//        result = getEdges(query(10, "out"))
//
//        // 10 -> out -> 20 should not be in result.
//        (result \ "results").as[List[JsValue]].size must equalTo(1)
//        (result \\ "to").size must equalTo(1)
//        (result \\ "to").head.as[String] must equalTo("21")
//
//        result = getEdges(query(20, "in", testTgtColumnName))
//        println(result)
//        (result \ "results").as[List[JsValue]].size must equalTo(0)
//
//        val jsResult = contentAsJson(EdgeController.mutateAndPublish(bulkEdges(startTs = deletedAt + 1), withWait = true))
//
//        result = getEdges(query(20, "in", testTgtColumnName))
//        (result \ "results").as[List[JsValue]].size must equalTo(3)
//      }
//    }
//  }
//
//}
//
