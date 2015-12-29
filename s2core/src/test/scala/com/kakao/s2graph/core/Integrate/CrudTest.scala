package com.kakao.s2graph.core.Integrate

import com.kakao.s2graph.core.mysqls._
import play.api.libs.json.{JsObject, Json}

class CrudTest extends IntegrateCommon {
  import CrudHelper._
  import TestUtil._

  test("test CRUD") {
    var tcNum = 0
    var tcString = ""
    var bulkQueries = List.empty[(Long, String, String)]
    var expected = Map.empty[String, String]

    val curTime = System.currentTimeMillis
    val t1 = curTime + 0
    val t2 = curTime + 1
    val t3 = curTime + 2
    val t4 = curTime + 3
    val t5 = curTime + 4

    val tcRunner = new CrudTestRunner()
    tcNum = 1
    tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "

    bulkQueries = List(
      (t1, "insert", "{\"time\": 10}"),
      (t2, "delete", ""),
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)

    tcNum = 2
    tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
    bulkQueries = List(
      (t1, "insert", "{\"time\": 10}"),
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
      (t2, "delete", ""))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)

    tcNum = 3
    tcString = "[t3 -> t2 -> t1 test case] insert(t3) delete(t2) insert(t1) test "
    bulkQueries = List(
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
      (t2, "delete", ""),
      (t1, "insert", "{\"time\": 10}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)

    tcNum = 4
    tcString = "[t3 -> t1 -> t2 test case] insert(t3) insert(t1) delete(t2) test "
    bulkQueries = List(
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
      (t1, "insert", "{\"time\": 10}"),
      (t2, "delete", ""))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)

    tcNum = 5
    tcString = "[t2 -> t1 -> t3 test case] delete(t2) insert(t1) insert(t3) test"
    bulkQueries = List(
      (t2, "delete", ""),
      (t1, "insert", "{\"time\": 10}"),
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)

    tcNum = 6
    tcString = "[t2 -> t3 -> t1 test case] delete(t2) insert(t3) insert(t1) test "
    bulkQueries = List(
      (t2, "delete", ""),
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
      (t1, "insert", "{\"time\": 10}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)

    tcNum = 7
    tcString = "[t1 -> t2 -> t3 test case] update(t1) delete(t2) update(t3) test "
    bulkQueries = List(
      (t1, "update", "{\"time\": 10}"),
      (t2, "delete", ""),
      (t3, "update", "{\"time\": 10, \"weight\": 20}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
    tcNum = 8
    tcString = "[t1 -> t3 -> t2 test case] update(t1) update(t3) delete(t2) test "
    bulkQueries = List(
      (t1, "update", "{\"time\": 10}"),
      (t3, "update", "{\"time\": 10, \"weight\": 20}"),
      (t2, "delete", ""))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
    tcNum = 9
    tcString = "[t2 -> t1 -> t3 test case] delete(t2) update(t1) update(t3) test "
    bulkQueries = List(
      (t2, "delete", ""),
      (t1, "update", "{\"time\": 10}"),
      (t3, "update", "{\"time\": 10, \"weight\": 20}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
    tcNum = 10
    tcString = "[t2 -> t3 -> t1 test case] delete(t2) update(t3) update(t1) test"
    bulkQueries = List(
      (t2, "delete", ""),
      (t3, "update", "{\"time\": 10, \"weight\": 20}"),
      (t1, "update", "{\"time\": 10}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
    tcNum = 11
    tcString = "[t3 -> t2 -> t1 test case] update(t3) delete(t2) update(t1) test "
    bulkQueries = List(
      (t3, "update", "{\"time\": 10, \"weight\": 20}"),
      (t2, "delete", ""),
      (t1, "update", "{\"time\": 10}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
    tcNum = 12
    tcString = "[t3 -> t1 -> t2 test case] update(t3) update(t1) delete(t2) test "
    bulkQueries = List(
      (t3, "update", "{\"time\": 10, \"weight\": 20}"),
      (t1, "update", "{\"time\": 10}"),
      (t2, "delete", ""))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)

    tcNum = 13
    tcString = "[t5 -> t1 -> t3 -> t2 -> t4 test case] update(t5) insert(t1) insert(t3) delete(t2) update(t4) test "
    bulkQueries = List(
      (t5, "update", "{\"is_blocked\": true}"),
      (t1, "insert", "{\"is_hidden\": false}"),
      (t3, "insert", "{\"is_hidden\": false, \"weight\": 10}"),
      (t2, "delete", ""),
      (t4, "update", "{\"time\": 1, \"weight\": -10}"))
    expected = Map("time" -> "1", "weight" -> "-10", "is_hidden" -> "false", "is_blocked" -> "true")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }


  object CrudHelper {

    class CrudTestRunner {
      var seed = 0

      def run(tcNum: Int, tcString: String, opWithProps: List[(Long, String, String)], expected: Map[String, String]) = {
        for {
          labelName <- List(testLabelName, testLabelName2)
          i <- 0 until NumOfEachTest
        } {
          seed += 1
          val srcId = seed.toString
          val tgtId = srcId

          val maxTs = opWithProps.map(t => t._1).max

          /** insert edges */
          println(s"---- TC${tcNum}_init ----")
          val bulkEdges = (for ((ts, op, props) <- opWithProps) yield {
            TestUtil.toEdge(ts, op, "e", srcId, tgtId, labelName, props)
          })

          TestUtil.insertEdgesSync(bulkEdges: _*)

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

            val jsResult = TestUtil.getEdgesSync(query)

            val results = jsResult \ "results"
            val deegrees = (jsResult \ "degrees").as[List[JsObject]]
            val propsLs = (results \\ "props").seq
            (deegrees.head \ LabelMeta.degree.name).as[Int] should be(1)

            val from = (results \\ "from").seq.last.toString.replaceAll("\"", "")
            val to = (results \\ "to").seq.last.toString.replaceAll("\"", "")

            from should be(id.toString)
            to should be(otherId.toString)
            (results \\ "_timestamp").seq.last.as[Long] should be(maxTs)

            for ((key, expectedVal) <- expected) {
              propsLs.last.as[JsObject].keys.contains(key) should be(true)
              (propsLs.last \ key).toString should be(expectedVal)
            }
          }
        }
      }

      def queryJson(serviceName: String, columnName: String, labelName: String, id: String, dir: String, cacheTTL: Long = -1L) = Json.parse(
        s""" { "srcVertices": [
             { "serviceName": "$serviceName",
               "columnName": "$columnName",
               "id": $id } ],
             "steps": [ [ {
             "label": "$labelName",
             "direction": "$dir",
             "offset": 0,
             "limit": 10,
             "cacheTTL": $cacheTTL }]]}""")
    }
  }
}
