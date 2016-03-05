package com.kakao.s2graph.core.Integrate

import java.util.concurrent.TimeUnit

import org.scalatest.BeforeAndAfterEach
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WeakLabelDeleteTest extends IntegrateCommon with BeforeAndAfterEach {

  import TestUtil._
  import WeakLabelDeleteHelper._

  test("test weak consistency select") {
    var result = getEdgesSync(query(0))
    println(result)
    (result \ "results").as[List[JsValue]].size should be(4)
    result = getEdgesSync(query(10))
    println(result)
    (result \ "results").as[List[JsValue]].size should be(2)
  }

  test("test weak consistency delete") {
    var result = getEdgesSync(query(0))
    println(result)

    /** expect 4 edges */
    (result \ "results").as[List[JsValue]].size should be(4)
    val edges = (result \ "results").as[List[JsObject]]
    val edgesToStore = parser.toEdges(Json.toJson(edges), "delete")
    val rets = graph.mutateEdges(edgesToStore, withWait = true)
    Await.result(rets, Duration(20, TimeUnit.MINUTES))

    /** expect noting */
    result = getEdgesSync(query(0))
    println(result)
    (result \ "results").as[List[JsValue]].size should be(0)

    /** insert should be ignored */
    /**
     * I am wondering if this is right test case
     * This makes sense because hbase think cell is deleted when there are
     * insert/delete with same timestamp(version) on same cell.
     * This can be different on different storage system so I think
     * this test should be removed.
     */
//    val edgesToStore2 = parser.toEdges(Json.toJson(edges), "insert")
//    val rets2 = graph.mutateEdges(edgesToStore2, withWait = true)
//    Await.result(rets2, Duration(20, TimeUnit.MINUTES))
//
//    result = getEdgesSync(query(0))
//    (result \ "results").as[List[JsValue]].size should be(0)
  }


  test("test weak consistency deleteAll") {
    val deletedAt = 100
    var result = getEdgesSync(query(20, "in", testTgtColumnName))
    println(result)
    (result \ "results").as[List[JsValue]].size should be(3)

    val json = Json.arr(Json.obj("label" -> testLabelNameWeak,
      "direction" -> "in", "ids" -> Json.arr("20"), "timestamp" -> deletedAt))
    println(json)
    deleteAllSync(json)

    result = getEdgesSync(query(11, "out"))
    (result \ "results").as[List[JsValue]].size should be(0)

    result = getEdgesSync(query(12, "out"))
    (result \ "results").as[List[JsValue]].size should be(0)

    result = getEdgesSync(query(10, "out"))

    // 10 -> out -> 20 should not be in result.
    (result \ "results").as[List[JsValue]].size should be(1)
    (result \\ "to").size should be(1)
    (result \\ "to").head.as[String] should be("21")

    result = getEdgesSync(query(20, "in", testTgtColumnName))
    println(result)
    (result \ "results").as[List[JsValue]].size should be(0)

    insertEdgesSync(bulkEdges(startTs = deletedAt + 1): _*)

    result = getEdgesSync(query(20, "in", testTgtColumnName))
    (result \ "results").as[List[JsValue]].size should be(3)
  }


  // called by each test, each
  override def beforeEach = initTestData()

  // called by start test, once

  override def initTestData(): Unit = {
    super.initTestData()
    insertEdgesSync(bulkEdges(): _*)
  }

  object WeakLabelDeleteHelper {

    def bulkEdges(startTs: Int = 0) = Seq(
      toEdge(startTs + 1, "insert", "e", "0", "1", testLabelNameWeak, s"""{"time": 10}"""),
      toEdge(startTs + 2, "insert", "e", "0", "1", testLabelNameWeak, s"""{"time": 11}"""),
      toEdge(startTs + 3, "insert", "e", "0", "1", testLabelNameWeak, s"""{"time": 12}"""),
      toEdge(startTs + 4, "insert", "e", "0", "2", testLabelNameWeak, s"""{"time": 10}"""),
      toEdge(startTs + 5, "insert", "e", "10", "20", testLabelNameWeak, s"""{"time": 10}"""),
      toEdge(startTs + 6, "insert", "e", "10", "21", testLabelNameWeak, s"""{"time": 11}"""),
      toEdge(startTs + 7, "insert", "e", "11", "20", testLabelNameWeak, s"""{"time": 12}"""),
      toEdge(startTs + 8, "insert", "e", "12", "20", testLabelNameWeak, s"""{"time": 13}""")
    )

    def query(id: Int, direction: String = "out", columnName: String = testColumnName) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$columnName",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelNameWeak}",
              "direction": "${direction}",
              "offset": 0,
              "limit": 10,
              "duplicate": "raw"
            }
          ]]
        }""")
  }

}

