package benchmark

import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core.{PostProcess, RequestParser, Graph, Management}
import com.typesafe.config.ConfigFactory
import controllers._
import play.api.libs.json.{JsValue, Json}
import play.api.test.{FakeApplication, FakeRequest, PlaySpecification}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 6..
  */
class PostProcessBenchmarkSpec extends SpecCommon with BenchmarkCommon with PlaySpecification {
  sequential

  import Helper._

  init()

  override def init() = {
    running(FakeApplication()) {
      println("[init start]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      Management.deleteService(testServiceName)

      // 1. createService
      val result = AdminController.createServiceInner(Json.parse(createService))
      println(s">> Service created : $createService, $result")

      val labelNames = Map(
        testLabelNameWeak -> testLabelNameWeakCreate
      )

      for {
        (labelName, create) <- labelNames
      } {
        Management.deleteLabel(labelName)
        Label.findByName(labelName, useCache = false) match {
          case None =>
            AdminController.createLabelInner(Json.parse(create))
          case Some(label) =>
            println(s">> Label already exist: $create, $label")
        }
      }

      // create edges
      val bulkEdges: String = (0 until 500).map { i =>
        edge"${System.currentTimeMillis()} insert e 0 $i $testLabelNameWeak"($(weight=i))
      }.mkString("\n")

      val jsResult = contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true))

      println("[init end]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    }
  }

  def getEdges(queryJson: JsValue): JsValue = {
    val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
    contentAsJson(ret)
  }

  val s2: Graph = com.kakao.s2graph.rest.Global.s2graph

//  "test performance of getEdges orderBy" >> {
//    running(FakeApplication()) {
//      val strJs =
//        s"""
//           |{
//           |  "orderBy": [
//           |    {"score": "DESC"},
//           |    {"timestamp": "DESC"}
//           |  ],
//           |  "srcVertices": [
//           |    {
//           |      "serviceName": "$testServiceName",
//           |      "columnName": "$testColumnName",
//           |      "ids": [0]
//           |    }
//           |  ],
//           |  "steps": [
//           |    {
//           |      "step": [
//           |        {
//           |          "cacheTTL": 60000,
//           |          "label": "$testLabelNameWeak",
//           |          "offset": 0,
//           |          "limit": -1,
//           |          "direction": "out",
//           |          "scoring": [
//           |            {"weight": 1}
//           |          ]
//           |        }
//           |      ]
//           |    }
//           |  ]
//           |}
//      """.stripMargin
//
//      object Parser extends RequestParser
//
//      val js = Json.parse(strJs)
//
//      val q = Parser.toQuery(js)
//
//      val queryResultLs = Await.result(s2.getEdges(q), 1 seconds)
//
//      val resultJs = PostProcess.toSimpleVertexArrJson(queryResultLs)
//
//      (resultJs \ "size").as[Int] must_== 500
//
//      (0 to 5) foreach { _ =>
//        duration("toSimpleVertexArrJson new orderBy") {
//          (0 to 1000) foreach { _ =>
//            PostProcess.toSimpleVertexArrJson(queryResultLs, Nil)
//          }
//        }
//      }
//
//      (resultJs \ "size").as[Int] must_== 500
//    }
//  }

  "test performance of getEdges" >> {
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

      val config = ConfigFactory.load()
      val s2graph = new Graph(config)(scala.concurrent.ExecutionContext.Implicits.global)
      val s2parser = new RequestParser(s2graph)
      val js = Json.parse(strJs)

      val q = s2parser.toQuery(js)

      val queryResultLs = Await.result(s2.getEdges(q), 1 seconds)

      val resultJs = PostProcess.toSimpleVertexArrJson(queryResultLs, Nil)

      (0 to 5) foreach { _ =>
        duration("toSimpleVertexArrJson new") {
          (0 to 1000) foreach { _ =>
            PostProcess.toSimpleVertexArrJson(queryResultLs, Nil)
          }
        }
      }

      (resultJs \ "size").as[Int] must_== 500
    }
  }

//  "test performance of getEdges withScore=false" >> {
//    running(FakeApplication()) {
//      val strJs =
//        s"""
//           |{
//           |  "withScore": false,
//           |  "srcVertices": [
//           |    {
//           |      "serviceName": "$testServiceName",
//           |      "columnName": "$testColumnName",
//           |      "ids": [0]
//           |    }
//           |  ],
//           |  "steps": [
//           |    {
//           |      "step": [
//           |        {
//           |          "cacheTTL": 60000,
//           |          "label": "$testLabelNameWeak",
//           |          "offset": 0,
//           |          "limit": -1,
//           |          "direction": "out",
//           |          "scoring": [
//           |            {"weight": 1}
//           |          ]
//           |        }
//           |      ]
//           |    }
//           |  ]
//           |}
//      """.stripMargin
//
//      object Parser extends RequestParser
//
//      val js = Json.parse(strJs)
//
//      val q = Parser.toQuery(js)
//
//      val queryResultLs = Await.result(s2.getEdges(q), 1 seconds)
//
//      val resultJs = PostProcess.toSimpleVertexArrJson(queryResultLs)
//
//      (resultJs \ "size").as[Int] must_== 500
//
//      (0 to 5) foreach { _ =>
//        duration("toSimpleVertexArrJson withScore=false org") {
//          (0 to 1000) foreach { _ =>
//            PostProcess.toSimpleVertexArrJsonOrg(queryResultLs, Nil)
//          }
//        }
//
//        duration("toSimpleVertexArrJson withScore=false new") {
//          (0 to 1000) foreach { _ =>
//            PostProcess.toSimpleVertexArrJson(queryResultLs, Nil)
//          }
//        }
//      }
//
//      (resultJs \ "size").as[Int] must_== 500
//    }
//  }
}
