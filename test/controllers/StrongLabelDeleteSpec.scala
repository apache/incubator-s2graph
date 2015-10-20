package test.controllers

import com.kakao.s2graph.core.Graph
import controllers.EdgeController
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}
import test.controllers.SpecCommon

import scala.util.Random

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

  def query(id: Long, serviceName: String = testServiceName, columnName: String = testColumnName,
            labelName: String = testLabelName2, direction: String = "out") = Json.parse(
    s"""
        { "srcVertices": [
          { "serviceName": "$serviceName",
            "columnName": "$columnName",
            "id": $id
           }],
          "steps": [
          [ {
              "label": "$labelName",
              "direction": "${direction}",
              "offset": 0,
              "limit": 1000,
              "duplicate": "raw"
            }
          ]]
        }""")

  def getEdges(queryJson: JsValue): JsValue = {
    val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
    contentAsJson(ret)
  }
  def getDegree(jsValue: JsValue): Long = {
    ((jsValue \ "degrees") \\ "_degree").headOption.map(_.as[Long]).getOrElse(0L)
  }


  "strong label delete test" should {
    running(FakeApplication()) {
      // insert bulk and wait ..
      val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges())).get
      val jsRslt = contentAsJson(ret)
      Thread.sleep(asyncFlushInterval)
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
        var result = getEdges(query(20, direction = "in", columnName = testTgtColumnName))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(3)



        val json = Json.arr(Json.obj("label" -> testLabelName2,
          "direction" -> "in", "ids" -> Json.arr("20"), "timestamp" -> deletedAt))
        println(json)
        EdgeController.deleteAllInner(json)
        Thread.sleep(asyncFlushInterval)


        result = getEdges(query(11, direction = "out"))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(0)

        result = getEdges(query(12, direction = "out"))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(0)

        result = getEdges(query(10, direction = "out"))
        println(result)
        // 10 -> out -> 20 should not be in result.
        (result \ "results").as[List[JsValue]].size must equalTo(1)
        (result \\ "to").size must equalTo(1)
        (result \\ "to").head.as[String] must equalTo("21")


        result = getEdges(query(20, direction = "in", columnName = testTgtColumnName))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(0)


        val ret = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges(startTs = deletedAt + 1))).get
        val jsRslt = contentAsJson(ret)
        Thread.sleep(asyncFlushInterval)

        result = getEdges(query(20, direction = "in", columnName = testTgtColumnName))
        println(result)
        (result \ "results").as[List[JsValue]].size must equalTo(3)

        true

      }
    }
  }
  

  "largeSet of contention" should {
    val labelName = testLabelName2
    val maxTgtId = 5
    val maxRetryNum = 10
    val maxTestNum = 10
    val maxTestIntervalNum = 10
    def testInner(src: Long) = {
      val labelName = testLabelName2
      val lastOps = Array.fill(maxTgtId)("none")
      for {
        ith <- (0 until maxTestIntervalNum)
      } {
        val bulkEdgeStr = for {
          jth <- (0 until maxRetryNum)
        } yield {
            val currentTs = System.currentTimeMillis() + ith + jth
            val tgt = Random.nextInt(maxTgtId)
            val op = if (Random.nextDouble() < 0.5) "delete" else "update"
            lastOps(tgt) = op
            Seq(currentTs, op, "e", src, tgt, labelName, "{}").mkString("\t")
          }
        val bulkEdge = bulkEdgeStr.mkString("\n")
        val req = route(FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdge)).get
        val jsRslt = contentAsJson(req)
        println(s">> $req, $bulkEdge")
        Thread.sleep(asyncFlushInterval)
      }
      Thread.sleep(asyncFlushInterval)
      val expectedDegree = lastOps.count(op => op != "delete" && op != "none")
      val queryJson = query(id = src)
      val result = getEdges(queryJson)
      val resultDegree = getDegree(result)
      println(lastOps.toList)
      println(result)
      resultDegree == expectedDegree
    }

    "update delete" in {
      running(FakeApplication()) {
        val ret = for {
          i <- (0 until maxTestNum)
        } yield {
          val src = System.currentTimeMillis()
          val ret = testInner(src)
          ret must beEqualTo(true)
          ret
        }
        ret.forall(identity)
      }

    }

    "deleteAll" in {
      running(FakeApplication()) {
        val src = System.currentTimeMillis()
        val ret = testInner(src)
        ret must beEqualTo(true)

        val deletedAt = System.currentTimeMillis()
        val deleteAllRequest = Json.arr(Json.obj("label" -> labelName, "ids" -> Json.arr(src), "timestamp" -> deletedAt))

        val req = route(FakeRequest(POST, "/graphs/edges/deleteAll").withJsonBody(deleteAllRequest)).get

        val jsResult = contentAsString(req)
        println(s">> $req, $deleteAllRequest, $jsResult")
        Thread.sleep(asyncFlushInterval)

        val result = getEdges(query(id = src))
        println(result)
        val resultEdges = (result \ "results").as[Seq[JsValue]]
        resultEdges.isEmpty must beEqualTo(true)

        val degreeAfterDeleteAll = getDegree(result)
        degreeAfterDeleteAll must beEqualTo(0)
      }

    }
  }
}

