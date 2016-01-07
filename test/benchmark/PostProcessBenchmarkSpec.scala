package benchmark

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.types.{HBaseType, InnerVal, SourceVertexId}
import controllers._
import play.api.libs.json.{JsValue, Json}
import play.api.test.{FakeApplication, FakeRequest, PlaySpecification}
import scala.util.Random

class PostProcessBenchmarkSpec extends SpecCommon with BenchmarkCommon with PlaySpecification {
  sequential
  object Parser extends RequestParser
  val numOfTry = 10
  val steps = Seq(100, 1000, 10000)

  def createQueryRequestWithResult(queryRequest: QueryRequest, n: Int): QueryRequestWithResult = {
    val edgeWithScoreLs = for {
      i <- (0 until n)
      s = Seq(System.currentTimeMillis(), "insert", "e", "0", i, testLabelNameWeak).mkString("\t")
      edge <- Graph.toEdge(s)
      score = Random.nextDouble()
    } yield EdgeWithScore(edge, score)
    QueryRequestWithResult(queryRequest, QueryResult(edgeWithScoreLs))
  }

  def dummyVertex() = Vertex(SourceVertexId(0, InnerVal.withStr("0", HBaseType.DEFAULT_VERSION)))

  def getEdges(queryJson: JsValue): JsValue = {
    val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
    contentAsJson(ret)
  }

  def runTC(prefix: String)(strJs: String): Boolean = {
    println(Seq("*" * 50, s"$prefix, average with $numOfTry try", "*" * 50).mkString("\n"))


    val js = Json.parse(strJs)

    val q = Parser.toQuery(js)

    val queryRequest = QueryRequest(q, 1, q.vertices.head, q.steps.head.queryParams.head)
    for {
      step <- steps
      n <- (step until step * 10 by step)
    } {
      var total = 0L
      for {
        i <- (0 until numOfTry)
      } {
        val queryRequestWithResult = createQueryRequestWithResult(queryRequest, n)
        val logMsg = s"toSimpleVertexArrJson new: n = $n"
        val (_, elapsed) = durationWithReturn(logMsg) {
          PostProcess.toSimpleVertexArrJson(Seq(queryRequestWithResult), Nil)
        }
        total += elapsed
      }
      println(Seq(s"n = $n", s" average ${total / numOfTry.toDouble} ms").mkString(","))
    }
    true
  }

  val s2: Graph = com.kakao.s2graph.rest.Global.s2graph

  "test performance of PostProcess with orderBy" >> {
    running(FakeApplication()) {
      val strJs =
        s"""
           |{
           |  "orderBy": [
           |    {"score": "DESC"},
           |    {"timestamp": "DESC"}
           |  ],
           |  "srcVertices": [
           |    {
           |      "serviceName": "$testServiceName",
           |      "columnName": "$testColumnName",
           |      "ids": [0]
           |    }
           |  ],
           |  "steps": [
           |    {
           |      "step": [
           |        {
           |          "cacheTTL": 60000,
           |          "label": "$testLabelNameWeak",
           |          "offset": 0,
           |          "limit": -1,
           |          "direction": "out",
           |          "scoring": [
           |            {"weight": 1}
           |          ]
           |        }
           |      ]
           |    }
           |  ]
           |}
      """.stripMargin

      runTC("test performance of PostProcess with orderBy")(strJs)
    }
  }

  "test performance of PostProcess with score" >> {
    running(FakeApplication()) {
      val strJs =
        s"""
           |{
           |  "srcVertices": [
           |    {
           |      "serviceName": "$testServiceName",
           |      "columnName": "$testColumnName",
           |      "ids": [0]
           |    }
           |  ],
           |  "steps": [
           |    {
           |      "step": [
           |        {
           |          "cacheTTL": 60000,
           |          "label": "$testLabelNameWeak",
           |          "offset": 0,
           |          "limit": -1,
           |          "direction": "out",
           |          "scoring": [
           |            {"weight": 1}
           |          ]
           |        }
           |      ]
           |    }
           |  ]
           |}
      """.stripMargin

      runTC("test performance of PostProcess with score")(strJs)
    }
  }

  "test performance of PostProcess without score" >> {
    running(FakeApplication()) {
      val strJs =
        s"""
           |{
           |  "withScore": false,
           |  "srcVertices": [
           |    {
           |      "serviceName": "$testServiceName",
           |      "columnName": "$testColumnName",
           |      "ids": [0]
           |    }
           |  ],
           |  "steps": [
           |    {
           |      "step": [
           |        {
           |          "cacheTTL": 60000,
           |          "label": "$testLabelNameWeak",
           |          "offset": 0,
           |          "limit": -1,
           |          "direction": "out",
           |          "scoring": [
           |            {"weight": 1}
           |          ]
           |        }
           |      ]
           |    }
           |  ]
           |}
      """.stripMargin

      runTC("test performance of PostProcess without score")(strJs)
    }
  }
}
