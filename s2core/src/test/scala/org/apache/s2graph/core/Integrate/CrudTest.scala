/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.Integrate

import org.apache.s2graph.core.schema.{Label, LabelMeta}
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.{JsObject, Json}

class CrudTest extends IntegrateCommon {
  import CrudHelper._
  import TestUtil._

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
  test("1: [t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test") {
    val tcNum = 1
    tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "

    bulkQueries = List(
      (t1, "insert", "{\"time\": 10}"),
      (t2, "delete", ""),
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("2: [t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test") {
    val tcNum = 2
    tcString = "[t1 -> t2 -> t3 test case] insert(t1) delete(t2) insert(t3) test "
    bulkQueries = List(
      (t1, "insert", "{\"time\": 10}"),
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
      (t2, "delete", ""))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("3: [t3 -> t2 -> t1 test case] insert(t3) delete(t2) insert(t1) test") {
    val tcNum = 3
    tcString = "[t3 -> t2 -> t1 test case] insert(t3) delete(t2) insert(t1) test "
    bulkQueries = List(
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
      (t2, "delete", ""),
      (t1, "insert", "{\"time\": 10}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("4: [t3 -> t1 -> t2 test case] insert(t3) insert(t1) delete(t2) test") {
    val tcNum = 4
    tcString = "[t3 -> t1 -> t2 test case] insert(t3) insert(t1) delete(t2) test "
    bulkQueries = List(
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
      (t1, "insert", "{\"time\": 10}"),
      (t2, "delete", ""))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("5: [t2 -> t1 -> t3 test case] delete(t2) insert(t1) insert(t3) test") {
    val tcNum = 5
    tcString = "[t2 -> t1 -> t3 test case] delete(t2) insert(t1) insert(t3) test"
    bulkQueries = List(
      (t2, "delete", ""),
      (t1, "insert", "{\"time\": 10}"),
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("6: [t2 -> t3 -> t1 test case] delete(t2) insert(t3) insert(t1) test") {
    val tcNum = 6
    tcString = "[t2 -> t3 -> t1 test case] delete(t2) insert(t3) insert(t1) test "
    bulkQueries = List(
      (t2, "delete", ""),
      (t3, "insert", "{\"time\": 10, \"weight\": 20}"),
      (t1, "insert", "{\"time\": 10}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("7: [t1 -> t2 -> t3 test case] update(t1) delete(t2) update(t3) test ") {
    val tcNum = 7
    tcString = "[t1 -> t2 -> t3 test case] update(t1) delete(t2) update(t3) test "
    bulkQueries = List(
      (t1, "update", "{\"time\": 10}"),
      (t2, "delete", ""),
      (t3, "update", "{\"time\": 10, \"weight\": 20}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("8: [t1 -> t3 -> t2 test case] update(t1) update(t3) delete(t2) test ") {
    val tcNum = 8
    tcString = "[t1 -> t3 -> t2 test case] update(t1) update(t3) delete(t2) test "
    bulkQueries = List(
      (t1, "update", "{\"time\": 10}"),
      (t3, "update", "{\"time\": 10, \"weight\": 20}"),
      (t2, "delete", ""))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("9: [t2 -> t1 -> t3 test case] delete(t2) update(t1) update(t3) test") {
    val tcNum = 9
    tcString = "[t2 -> t1 -> t3 test case] delete(t2) update(t1) update(t3) test "
    bulkQueries = List(
      (t2, "delete", ""),
      (t1, "update", "{\"time\": 10}"),
      (t3, "update", "{\"time\": 10, \"weight\": 20}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("10: [t2 -> t3 -> t1 test case] delete(t2) update(t3) update(t1) test") {
    val tcNum = 10
    tcString = "[t2 -> t3 -> t1 test case] delete(t2) update(t3) update(t1) test"
    bulkQueries = List(
      (t2, "delete", ""),
      (t3, "update", "{\"time\": 10, \"weight\": 20}"),
      (t1, "update", "{\"time\": 10}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("11: [t3 -> t2 -> t1 test case] update(t3) delete(t2) update(t1) test") {
    val tcNum = 11
    tcString = "[t3 -> t2 -> t1 test case] update(t3) delete(t2) update(t1) test "
    bulkQueries = List(
      (t3, "update", "{\"time\": 10, \"weight\": 20}"),
      (t2, "delete", ""),
      (t1, "update", "{\"time\": 10}"))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("12: [t3 -> t1 -> t2 test case] update(t3) update(t1) delete(t2) test") {
    val tcNum = 12
    tcString = "[t3 -> t1 -> t2 test case] update(t3) update(t1) delete(t2) test "
    bulkQueries = List(
      (t3, "update", "{\"time\": 10, \"weight\": 20}"),
      (t1, "update", "{\"time\": 10}"),
      (t2, "delete", ""))
    expected = Map("time" -> "10", "weight" -> "20")

    tcRunner.run(tcNum, tcString, bulkQueries, expected)
  }
  test("13: [t5 -> t1 -> t3 -> t2 -> t4 test case] update(t5) insert(t1) insert(t3) delete(t2) update(t4) test ") {
    val tcNum = 13
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

  test("14 - test lock expire") {
    for {
      labelName <- List(testLabelName, testLabelName2)
    } {
      val id = 0
      tcRunner.expireTC(labelName, id)
    }
  }


  object CrudHelper {

    class CrudTestRunner {
      var seed = System.currentTimeMillis()

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
          println(s"${bulkEdges.mkString("\n")}")
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
              (propsLs.last \ key).get.toString should be(expectedVal)
            }
          }
        }
      }

      def expireTC(labelName: String, id: Int) = {
        var i = 1
        val label = Label.findByName(labelName).get
        val serviceName = label.serviceName
        val columnName = label.srcColumnName
        val id = 0

        while (i < 1000) {
          val bulkEdges = Seq(TestUtil.toEdge(i, "u", "e", id, id, testLabelName, Json.obj("time" -> 10).toString()))
          val rets = TestUtil.insertEdgesSync(bulkEdges: _*)


          val queryJson = querySnapshotEdgeJson(serviceName, columnName, labelName, id)

          if (!rets.forall(_.isSuccess)) {
            Thread.sleep(graph.LockExpireDuration + 100)
            /** expect current request would be ignored */
            val bulkEdges = Seq(TestUtil.toEdge(i-1, "u", "e", 0, 0, testLabelName, Json.obj("time" -> 20).toString()))
            val rets = TestUtil.insertEdgesSync(bulkEdges: _*)
            if (rets.forall(_.isSuccess)) {
              // check
              val jsResult = TestUtil.getEdgesSync(queryJson)
              (jsResult \\ "time").head.as[Int] should be(10)
              logger.debug(jsResult)
              i = 100000
            }
          }

          i += 1
        }

        i = 1
        while (i < 1000) {
          val bulkEdges = Seq(TestUtil.toEdge(i, "u", "e", id, id, testLabelName, Json.obj("time" -> 10).toString()))
          val rets = TestUtil.insertEdgesSync(bulkEdges: _*)


          val queryJson = querySnapshotEdgeJson(serviceName, columnName, labelName, id)

          if (!rets.forall(_.isSuccess)) {
            Thread.sleep(graph.LockExpireDuration + 100)
            /** expect current request would be applied */
            val bulkEdges = Seq(TestUtil.toEdge(i+1, "u", "e", 0, 0, testLabelName, Json.obj("time" -> 20).toString()))
            val rets = TestUtil.insertEdgesSync(bulkEdges: _*)
            if (rets.forall(_.isSuccess)) {
              // check
              val jsResult = TestUtil.getEdgesSync(queryJson)
              (jsResult \\ "time").head.as[Int] should be(20)
              logger.debug(jsResult)
              i = 100000
            }
          }

          i += 1
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

      def querySnapshotEdgeJson(serviceName: String, columnName: String, labelName: String, id: Int) = Json.parse(
        s""" { "srcVertices": [
             { "serviceName": "$serviceName",
               "columnName": "$columnName",
               "id": $id } ],
             "steps": [ [ {
             "label": "$labelName",
             "_to": $id }]]}""")
    }
  }
}
