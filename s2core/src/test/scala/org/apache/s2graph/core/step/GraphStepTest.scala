package org.apache.s2graph.core.step

import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core._
import org.apache.s2graph.core.parsers.Where
import org.apache.s2graph.core.rest.RequestParser
import play.api.libs.json.Json
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class GraphStepTest extends IntegrateCommon {

  import TestUtil._
  import RxStep._

  val insert = "insert"
  val e = "e"
  val weight = "weight"
  val is_hidden = "is_hidden"

  override def initTestData(): Unit = {
    super.initTestData()

    insertEdgesSync(
      toEdge(1000, insert, e, 1, 10, testLabelName),
      toEdge(1000, insert, e, 2, 20, testLabelName),
      toEdge(1000, insert, e, 3, 30, testLabelName),

      toEdge(1000, insert, e, 100, 1, testLabelName, Json.obj(weight -> 30))
    )
  }

  test("basic compose") {
    val vertices = Seq(
      graph.toVertex(testServiceName, testColumnName, 1),
      graph.toVertex(testServiceName, testColumnName, 2),
      graph.toVertex(testServiceName, testColumnName, 3),

      graph.toVertex(testServiceName, testColumnName, 10)
    )

    val v1 = VertexFetchStep(graph)

    val qpIn = QueryParam(labelName = testLabelName, direction = "in")
    val qpOut = QueryParam(labelName = testLabelName, direction = "out")

    val e1 = EdgeFetchStep(graph, qpIn)
    val e2 = EdgeFetchStep(graph, qpOut)

    val where = Where("_to = 20").get

    val q =
      v1.apply(vertices) // vertices: 4 - (1, 2, 3, 10)
        .flatMap(e1) // edges: 4 - (srcId = 1, 2, 3 and tgtId = 10)
        .filter(where.filter) // filterOut (only _to == 20)
        .map(_.tgtForVertex) // vertices: (20)
        .flatMap(v => e1.apply(v) ++ e2.apply(v)) // edges: (tgtId = 20)

    val res = q.toBlocking.toList
  }

  test("Query to RxSteps") {
    def q(id: Int) = Json.parse(
      s"""
        {
          "srcVertices": [
            { "serviceName": "$testServiceName",
              "columnName": "$testColumnName",
              "id": $id
            }],
          "steps": [
            [{
              "label": "$testLabelName",
              "direction": "out",
              "offset": 0,
              "limit": 10
            },
            {
              "label": "$testLabelName",
              "direction": "in",
              "offset": 0,
              "limit": 10
            }],

            [{
              "label": "$testLabelName",
              "direction": "out",
              "offset": 0,
              "limit": 10,
              "where": "weight > 10"
            },
            {
              "label": "$testLabelName",
              "direction": "in",
              "offset": 0,
              "limit": 10
            }]
          ]
        }""")

    val queryJs = q(1)
    val requestParser = new RequestParser(graph)
    val query = requestParser.toQuery(queryJs, None)

    val actual = RxStep.toObservable(query)(graph).toBlocking.toList.sortBy(_.srcVertex.innerIdVal.toString)
    val expected = Await.result(graph.getEdges(query), Duration("30 sec")).edgeWithScores.map(_.edge).sortBy(_.srcVertex.innerIdVal.toString)

    actual shouldBe expected
  }
}
