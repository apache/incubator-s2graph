package test.controllers

import play.api.libs.json._
import play.api.test.{FakeApplication, FakeRequest, PlaySpecification}
import play.api.{Application => PlayApplication}

import scala.concurrent.Await

class QuerySpec extends SpecCommon with PlaySpecification {

  import Helper._

  implicit val app = FakeApplication()

  init()

  "query test" should {
    running(FakeApplication()) {

      // insert bulk and wait ..
      val bulkEdges: String = Seq(
        edge"1000 insert e 0 1 $testLabelName"($(weight = 40, is_hidden = true)),
        edge"2000 insert e 0 2 $testLabelName"($(weight = 30, is_hidden = false)),
        edge"3000 insert e 2 0 $testLabelName"($(weight = 20)),
        edge"4000 insert e 2 1 $testLabelName"($(weight = 10)),
        edge"3000 insert e 10 20 $testLabelName"($(weight = 20)),
        edge"4000 insert e 20 20 $testLabelName"($(weight = 10)),
        edge"1 insert e -1 1000 $testLabelName",
        edge"1 insert e -1 2000 $testLabelName",
        edge"1 insert e -1 3000 $testLabelName",
        edge"1 insert e 1000 10000 $testLabelName",
        edge"1 insert e 1000 11000 $testLabelName",
        edge"1 insert e 2000 11000 $testLabelName",
        edge"1 insert e 2000 12000 $testLabelName",
        edge"1 insert e 3000 12000 $testLabelName",
        edge"1 insert e 3000 13000 $testLabelName",
        edge"1 insert e 10000 100000 $testLabelName",
        edge"2 insert e 11000 200000 $testLabelName",
        edge"3 insert e 12000 300000 $testLabelName").mkString("\n")

      val req = FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges)
      Await.result(route(req).get, HTTP_REQ_WAITING_TIME)

      Thread.sleep(asyncFlushInterval)
    }

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

    def queryAncestor(ids: Seq[Int], ancestorAt: Int) = {
      val $from = $a(
        $(serviceName = testServiceName,
          columnName = testColumnName,
          ids = ids))

      val $step = $a($(label = testLabelName, direction = "out", offset = 0, limit = 100))

      val $steps =
        if (ancestorAt == 1) {
          $a($(step = $step, shouldPropagate = true), $(step = $step), $(step = $step))
        } else {
          $a($(step = $step), $(step = $step, shouldPropagate = true), $(step = $step))
        }

      $(srcVertices = $from, steps = $steps).toJson
    }

    "get edge with where condition" in {
      running(FakeApplication()) {
        var result = getEdges(queryWhere(0, "is_hidden=false and _from in (-1, 0)"))
        (result \ "results").as[List[JsValue]].size must equalTo(1)

        result = getEdges(queryWhere(0, "is_hidden=true and _to in (1)"))
        (result \ "results").as[List[JsValue]].size must equalTo(1)

        result = getEdges(queryWhere(0, "_from=0"))
        (result \ "results").as[List[JsValue]].size must equalTo(2)

        result = getEdges(queryWhere(2, "_from=2 or weight in (-1)"))
        (result \ "results").as[List[JsValue]].size must equalTo(2)

        result = getEdges(queryWhere(2, "_from=2 and weight in (10, 20)"))
        (result \ "results").as[List[JsValue]].size must equalTo(2)
      }
    }

    "get edge exclude" in {
      running(FakeApplication()) {
        val result = getEdges(queryExclude(0))
        (result \ "results").as[List[JsValue]].size must equalTo(1)
      }
    }

    "edge transform " in {
      running(FakeApplication()) {
        var result = getEdges(queryTransform(0, "[[\"_to\"]]"))
        (result \ "results").as[List[JsValue]].size must equalTo(2)

        result = getEdges(queryTransform(0, "[[\"weight\"]]"))
        (result \\ "to").map(_.toString).sorted must equalTo((result \\ "weight").map(_.toString).sorted)

        result = getEdges(queryTransform(0, "[[\"_from\"]]"))
        val results = (result \ "results").as[JsValue]
        (result \\ "to").map(_.toString).sorted must equalTo((results \\ "from").map(_.toString).sorted)
      }
    }

    "index" in {
      running(FakeApplication()) {
        // weight order
        var result = getEdges(queryIndex(Seq(0), "idx_1"))
        ((result \ "results").as[List[JsValue]].head \\ "weight").head must equalTo(JsNumber(40))

        // timestamp order
        result = getEdges(queryIndex(Seq(0), "idx_2"))
        ((result \ "results").as[List[JsValue]].head \\ "weight").head must equalTo(JsNumber(30))
      }
    }

    "duration" in {
      running(FakeApplication()) {
        // get all
        var result = getEdges(queryDuration(Seq(0, 2), from = 0, to = 5000))
        (result \ "results").as[List[JsValue]].size must equalTo(4)

        // inclusive, exclusive
        result = getEdges(queryDuration(Seq(0, 2), from = 1000, to = 4000))
        (result \ "results").as[List[JsValue]].size must equalTo(3)

        result = getEdges(queryDuration(Seq(0, 2), from = 1000, to = 2000))
        (result \ "results").as[List[JsValue]].size must equalTo(1)

        val bulkEdges: String = Seq(
          edge"1001 insert e 0 1 $testLabelName"($(weight = 10, is_hidden = true)),
          edge"2002 insert e 0 2 $testLabelName"($(weight = 20, is_hidden = false)),
          edge"3003 insert e 2 0 $testLabelName"($(weight = 30)),
          edge"4004 insert e 2 1 $testLabelName"($(weight = 40))
        ).mkString("\n")

        val req = FakeRequest(POST, "/graphs/edges/bulk").withBody(bulkEdges)
        Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
        Thread.sleep(asyncFlushInterval)

        // duration test after udpate
        // get all
        result = getEdges(queryDuration(Seq(0, 2), from = 0, to = 5000))
        (result \ "results").as[List[JsValue]].size must equalTo(4)

        // inclusive, exclusive
        result = getEdges(queryDuration(Seq(0, 2), from = 1000, to = 4000))
        (result \ "results").as[List[JsValue]].size must equalTo(3)

        result = getEdges(queryDuration(Seq(0, 2), from = 1000, to = 2000))
        (result \ "results").as[List[JsValue]].size must equalTo(1)
        true
      }
    }

    "ancestor" in {
      running(FakeApplication()) {
        var result = getEdges(queryAncestor(Seq(-1), 1))
        var expect = Seq(Set("2000", "3000"), Set("1000", "2000"), Set("1000"))
        var ancestors = (result \\ "ancestor").map(_.as[Seq[String]]).map(_.toSet)
        ancestors must equalTo(expect)

        result = getEdges(queryAncestor(Seq(-1), 2))
        expect = Seq(Set("12000"), Set("11000"), Set("10000"))
        ancestors = (result \\ "ancestor").map(_.as[Seq[String]]).map(_.toSet)
        ancestors must equalTo(expect)
        true
      }
    }
  }
}
