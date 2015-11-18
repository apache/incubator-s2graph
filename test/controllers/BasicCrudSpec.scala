package controllers

import com.kakao.s2graph.core.Management
import com.kakao.s2graph.core.mysqls._

//import com.kakao.s2graph.core.models._

import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

import scala.concurrent.Await


class BasicCrudSpec extends SpecCommon {
  sequential

  var seed = 0
  def runTC(tcNum: Int, tcString: String, opWithProps: List[(Long, String, String)], expected: Map[String, String]) = {
    for {
      labelName <- List(testLabelName, testLabelName2)
      i <- 0 until NUM_OF_EACH_TEST
    } {
      seed += 1
//      val srcId = ((tcNum * 1000) + i).toString
//      val tgtId = if (labelName == testLabelName) s"${srcId + 1000 + i}" else s"${srcId + 1000 + i}abc"
      val srcId = seed.toString
      val tgtId = srcId

      val maxTs = opWithProps.map(t => t._1).max

      /** insert edges */
      println(s"---- TC${tcNum}_init ----")
      val bulkEdge = (for ((ts, op, props) <- opWithProps) yield {
        List(ts, op, "e", srcId, tgtId, labelName, props).mkString("\t")
      }).mkString("\n")

      val req = EdgeController.mutateAndPublish(bulkEdge, withWait = true)
      val res = Await.result(req, HTTP_REQ_WAITING_TIME)

      res.header.status must equalTo(200)

      println(s"---- TC${tcNum}_init ----")
//      Thread.sleep(100)

      for {
        label <- Label.findByName(labelName)
        direction <- List("out", "in")
        cacheTTL <- List(-1L)
      } {
        val (serviceName, columnName, id, otherId) = direction match {
          case "out" => (label.srcService.serviceName, label.srcColumn.columnName, srcId, tgtId)
          case "in" => (label.tgtService.serviceName, label.tgtColumn.columnName, tgtId, srcId)
        }
        val qId = if (labelName == testLabelName) id else "\"" + id + "\""
        val query = queryJson(serviceName, columnName, labelName, qId, direction, cacheTTL)
        val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(query)).get
        val jsResult = commonCheck(ret)

        val results = jsResult \ "results"
        val deegrees = (jsResult \ "degrees").as[List[JsObject]]
        val propsLs = (results \\ "props").seq
        (deegrees.head \ LabelMeta.degree.name).as[Int] must equalTo(1)

        val from = (results \\ "from").seq.last.toString.replaceAll("\"", "")
        val to = (results \\ "to").seq.last.toString.replaceAll("\"", "")

        from must equalTo(id.toString)
        to must equalTo(otherId.toString)
//        (results \\ "_timestamp").seq.last.as[Long] must equalTo(maxTs)
        for ((key, expectedVal) <- expected) {
          propsLs.last.as[JsObject].keys.contains(key) must equalTo(true)
          (propsLs.last \ key).toString must equalTo(expectedVal)
        }
        Await.result(ret, HTTP_REQ_WAITING_TIME)
      }
    }
  }

  init()
  "Basic Crud " should {
    "tc1" in {
      running(FakeApplication()) {

        var tcNum = 0
        var tcString = ""
        var bulkQueries = List.empty[(Long, String, String)]
        var expected = Map.empty[String, String]

        tcNum = 7
        tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
        bulkQueries = List(
          (t1, "insert", "{\"time\": 10}"),
          (t2, "delete", ""),
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 8
        tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
        bulkQueries = List(
          (t1, "insert", "{\"time\": 10}"),
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 9
        tcString = "[t3 -> t2 -> t1 test case] insert(t3) delete(t2) insert(t1) test "
        bulkQueries = List(
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""),
          (t1, "insert", "{\"time\": 10}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 10
        tcString = "[t3 -> t1 -> t2 test case] insert(t3) insert(t1) delete(t2) test "
        bulkQueries = List(
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
          (t1, "insert", "{\"time\": 10}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 11
        tcString = "[t2 -> t1 -> t3 test case] delete(t2) insert(t1) insert(t3) test"
        bulkQueries = List(
          (t2, "delete", ""),
          (t1, "insert", "{\"time\": 10}"),
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 12
        tcString = "[t2 -> t3 -> t1 test case] delete(t2) insert(t3) insert(t1) test "
        bulkQueries = List(
          (t2, "delete", ""),
          (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
          (t1, "insert", "{\"time\": 10}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 13
        tcString = "[t1 -> t2 -> t3 test case] update(t1) delete(t2) update(t3) test "
        bulkQueries = List(
          (t1, "update", "{\"time\": 10}"),
          (t2, "delete", ""),
          (t3, "update", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 14
        tcString = "[t1 -> t3 -> t2 test case] update(t1) update(t3) delete(t2) test "
        bulkQueries = List(
          (t1, "update", "{\"time\": 10}"),
          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 15
        tcString = "[t2 -> t1 -> t3 test case] delete(t2) update(t1) update(t3) test "
        bulkQueries = List(
          (t2, "delete", ""),
          (t1, "update", "{\"time\": 10}"),
          (t3, "update", "{\"time\": 10, \"weight\": 20}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 16
        tcString = "[t2 -> t3 -> t1 test case] delete(t2) update(t3) update(t1) test"
        bulkQueries = List(
          (t2, "delete", ""),
          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
          (t1, "update", "{\"time\": 10}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 17
        tcString = "[t3 -> t2 -> t1 test case] update(t3) delete(t2) update(t1) test "
        bulkQueries = List(
          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
          (t2, "delete", ""),
          (t1, "update", "{\"time\": 10}"))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)
        tcNum = 18
        tcString = "[t3 -> t1 -> t2 test case] update(t3) update(t1) delete(t2) test "
        bulkQueries = List(
          (t3, "update", "{\"time\": 10, \"weight\": 20}"),
          (t1, "update", "{\"time\": 10}"),
          (t2, "delete", ""))
        expected = Map("time" -> "10", "weight" -> "20")

        runTC(tcNum, tcString, bulkQueries, expected)

        tcNum = 19
        tcString = "[t5 -> t1 -> t3 -> t2 -> t4 test case] update(t5) insert(t1) insert(t3) delete(t2) update(t4) test "
        bulkQueries = List(
          (t5, "update", "{\"is_blocked\": true}"),
          (t1, "insert", "{\"is_hidden\": false}"),
          (t3, "insert", "{\"is_hidden\": false, \"weight\": 10}"),
          (t2, "delete", ""),
          (t4, "update", "{\"time\": 1, \"weight\": -10}"))
        expected = Map("time" -> "1", "weight" -> "-10", "is_hidden" -> "false", "is_blocked" -> "true")

        runTC(tcNum, tcString, bulkQueries, expected)
        true
      }
    }
  }

  "toLogString" in {
    running(FakeApplication()) {
      val bulkQueries = List(
        ("1445240543366", "update", "{\"is_blocked\":true}"),
        ("1445240543362", "insert", "{\"is_hidden\":false}"),
        ("1445240543364", "insert", "{\"is_hidden\":false,\"weight\":10}"),
        ("1445240543363", "delete", "{}"),
        ("1445240543365", "update", "{\"time\":1, \"weight\":-10}"))

      val (srcId, tgtId, labelName) = ("1", "2", testLabelName)

      val bulkEdge = (for ((ts, op, props) <- bulkQueries) yield {
        Management.toEdge(ts.toLong, op, srcId, tgtId, labelName, "out", props).toLogString
      }).mkString("\n")

      val expected = Seq(
        Seq("1445240543366", "update", "e", "1", "2", "s2graph_label_test", "{\"is_blocked\":true}"),
        Seq("1445240543362", "insert", "e", "1", "2", "s2graph_label_test", "{\"is_hidden\":false}"),
        Seq("1445240543364", "insert", "e", "1", "2", "s2graph_label_test", "{\"is_hidden\":false,\"weight\":10}"),
        Seq("1445240543363", "delete", "e", "1", "2", "s2graph_label_test"),
        Seq("1445240543365", "update", "e", "1", "2", "s2graph_label_test", "{\"time\":1,\"weight\":-10}")
      ).map(_.mkString("\t")).mkString("\n")

      bulkEdge must equalTo(expected)

      true
    }
  }
}


