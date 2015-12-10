package controllers

import play.api.libs.json._
import play.api.test.{FakeApplication, FakeRequest, PlaySpecification}
import play.api.{Application => PlayApplication}

class QueryExtendSpec extends SpecCommon with PlaySpecification {

  import Helper._

  implicit val app = FakeApplication()

  init()

  "query test" should {
    def queryParents(id: Long) = Json.parse(s"""
        {
          "returnTree": true,
          "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": 0,
              "limit": 2
            }
          ],[{
              "label": "${testLabelName}",
              "direction": "in",
              "offset": 0,
              "limit": -1
            }
          ]]
        }""".stripMargin)

    def queryExclude(id: Int) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": 0,
              "limit": 2
            },
            {
              "label": "${testLabelName}",
              "direction": "in",
              "offset": 0,
              "limit": 2,
              "exclude": true
            }
          ]]
        }""")

    def queryTransform(id: Int, transforms: String) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": 0,
              "transform": $transforms
            }
          ]]
        }""")

    def queryWhere(id: Int, where: String) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": 0,
              "limit": 100,
              "where": "${where}"
            }
          ]]
        }""")

    def querySingleWithTo(id: Int, offset: Int = 0, limit: Int = 100, to: Int) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": $offset,
              "limit": $limit,
              "_to": $to
            }
          ]]
        }
        """)

    def querySingle(id: Int, offset: Int = 0, limit: Int = 100) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": $offset,
              "limit": $limit
            }
          ]]
        }
        """)

    def queryUnion(id: Int, size: Int) = JsArray(List.tabulate(size)(_ => querySingle(id)))

    def queryGroupBy(id: Int, props: Seq[String]): JsValue = {
      Json.obj(
        "groupBy" -> props,
        "srcVertices" -> Json.arr(
          Json.obj("serviceName" -> testServiceName, "columnName" -> testColumnName, "id" -> id)
        ),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj(
                "label" -> testLabelName
              )
            )
          )
        )
      )
    }

    def queryScore(id: Int, scoring: Map[String, Int]): JsValue = {
      val q = Json.obj(
        "srcVertices" -> Json.arr(
          Json.obj(
            "serviceName" -> testServiceName,
            "columnName" -> testColumnName,
            "id" -> id
          )
        ),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj(
                "label" -> testLabelName,
                "scoring" -> scoring
              )
            )
          )
        )
      )
      println(q)
      q
    }

    def queryOrderBy(id: Int, scoring: Map[String, Int], props: Seq[Map[String, String]]): JsValue = {
      Json.obj(
        "orderBy" -> props,
        "srcVertices" -> Json.arr(
          Json.obj("serviceName" -> testServiceName, "columnName" -> testColumnName, "id" -> id)
        ),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj(
                "label" -> testLabelName,
                "scoring" -> scoring
              )
            )
          )
        )
      )
    }

    def queryGlobalLimit(id: Int, limit: Int): JsValue = {
      Json.obj(
        "limit" -> limit,
        "srcVertices" -> Json.arr(
          Json.obj("serviceName" -> testServiceName, "columnName" -> testColumnName, "id" -> id)
        ),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj(
                "label" -> testLabelName
              )
            )
          )
        )
      )
    }

    def getEdges(queryJson: JsValue): JsValue = {
      val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
      contentAsJson(ret)
    }

    def queryIndex(ids: Seq[Int], indexName: String) = {
      val $from = $a(
        $(serviceName = testServiceName,
          columnName = testColumnName,
          ids = ids))

      val $step = $a($(label = testLabelName, index = indexName))
      val $steps = $a($(step = $step))

      val js = $(withScore = false, srcVertices = $from, steps = $steps).toJson
      js
    }

    def queryDuration(ids: Seq[Int], from: Int, to: Int) = {
      val $from = $a(
        $(serviceName = testServiceName,
          columnName = testColumnName,
          ids = ids))

      val $step = $a($(
        label = testLabelName, direction = "out", offset = 0, limit = 100,
        duration = $(from = from, to = to)))

      val $steps = $a($(step = $step))

      $(srcVertices = $from, steps = $steps).toJson
    }

    "limit" >> {
      running(FakeApplication()) {
        // insert test set
        val bulkEdges: String = Seq(
          edge"1001 insert e 0 1 $testLabelName"($(weight = 10, is_hidden = true)),
          edge"2002 insert e 0 2 $testLabelName"($(weight = 20, is_hidden = false)),
          edge"3003 insert e 2 0 $testLabelName"($(weight = 30)),
          edge"4004 insert e 2 1 $testLabelName"($(weight = 40))
        ).mkString("\n")
        contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true))

        val edges = getEdges(querySingle(0, limit=1))
        val limitEdges = getEdges(queryGlobalLimit(0, 1))

        println(edges)
        println(limitEdges)

        val edgesTo = edges \ "results" \\ "to"
        val limitEdgesTo = limitEdges \ "results" \\ "to"

        edgesTo must_== limitEdgesTo
      }
    }
  }
}
