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

import org.apache.s2graph.core.parsers.Where
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core._
import org.scalatest.BeforeAndAfterEach
import play.api.libs.json._

class QueryTest extends IntegrateCommon with BeforeAndAfterEach {

  import TestUtil._

  val insert = "insert"
  val e = "e"
  val weight = "weight"
  val is_hidden = "is_hidden"

  def querySingle(id: Int, offset: Int = 0, limit: Int = 100) = Json.parse(
    s"""
          { "srcVertices": [
            { "serviceName": "$testServiceName",
              "columnName": "$testColumnName",
              "id": $id
             }],
            "steps": [
            [ {
                "label": "$testLabelName",
                "direction": "out",
                "offset": $offset,
                "limit": $limit
              }
            ]]
          }
          """)

  def queryGlobalLimit(id: Int, limit: Int): JsValue = Json.obj(
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

  def getQuery(id: Int, where: String): Query =
    Query(
      vertices = Seq(graph.toVertex(testServiceName, testColumnName, id)),
      steps = Vector(
        Step(Seq(QueryParam(testLabelName, where = Where(where))))
      )
    )

  def queryIntervalWithParent(id: Int, index: String, prop: String, value: String) =
    Query(
      vertices = Seq(graph.toVertex(testServiceName, testColumnName, id)),
      steps = Vector(
        Step(Seq(QueryParam(testLabelName, indexName = index))),
        Step(Seq(QueryParam(testLabelName, indexName = index,
          intervalOpt = Option(Seq(prop -> JsString(value)), Seq(prop -> JsString(value)))))
        )
      )
    )

  def queryIntervalWithParentRange(id: Int, index: String,
                                   prop: String, value: String,
                                   toProp: String, toValue: String) =
    Query(
      vertices = Seq(graph.toVertex(testServiceName, testColumnName, id)),
      steps = Vector(
        Step(Seq(QueryParam(testLabelName, indexName = index))),
        Step(Seq(QueryParam(testLabelName, indexName = index,
          intervalOpt = Option(Seq(prop -> JsString(value)), Seq(toProp -> JsString(toValue)))))
        )
      )
    )

  def queryWithInterval(id: Int, index: String, prop: String, fromVal: Int, toVal: Int) =
    Query(
      vertices = Seq(graph.toVertex(testServiceName, testColumnName, id)),
      steps = Vector(
        Step(Seq(QueryParam(testLabelName, indexName = index,
          intervalOpt = Option(Seq(prop -> JsNumber(fromVal)), Seq(prop -> JsNumber(toVal))))))
      )
    )

  def queryExclude(id: Int) =
    Query(
      vertices = Seq(graph.toVertex(testServiceName, testColumnName, id)),
      steps = Vector(
        Step(
          Seq(
            QueryParam(testLabelName, limit = 2),
            QueryParam(testLabelName, direction = "in", limit = 2, exclude = true)
          )
        )
      )
    )

  def queryGroupBy(id: Int, props: Seq[String]) =
    Query(
      vertices = Seq(graph.toVertex(testServiceName, testColumnName, id)),
      steps = Vector(
        Step(
          Seq(QueryParam(testLabelName))
        )
      ),
      queryOption = QueryOption(groupBy = GroupBy(props, 100))
    )


  test("query with defaultValue") {
    // ref: edges from initTestData()

    // no default value
    var edges = getEdgesSync(getQuery(0, "_to = 1"))
    (edges \\ "is_hidden").head.as[Boolean] should be(true)

    // default value(weight, is_hidden)
    edges = getEdgesSync(getQuery(-1, "_to = 1000"))
    (edges \\ "is_hidden").head.as[Boolean] should be(false)
    (edges \\ "weight").head.as[Long] should be(0)

    // default value(is_hidden)
    edges = getEdgesSync(getQuery(10, "_to = 20"))
    (edges \\ "is_hidden").head.as[Boolean] should be(false)
  }

  test("degree with `Where clause") {
    val edges = getEdgesSync(getQuery(2, "_from != 2"))
    (edges \ "degrees").as[Seq[JsValue]].nonEmpty should be(true)
  }

  ignore("interval parent") {
    val baseId = 1024

    insertEdgesSync(
      toEdge(20, insert, e, baseId, baseId + 1, testLabelName, Json.obj(weight -> 30, is_hidden -> true)),

      toEdge(10, insert, e, baseId + 1, baseId + 10, testLabelName, Json.obj(weight -> 30, is_hidden -> true)),
      toEdge(20, insert, e, baseId + 1, baseId + 20, testLabelName, Json.obj(weight -> 30, is_hidden -> true)),
      toEdge(30, insert, e, baseId + 1, baseId + 30, testLabelName, Json.obj(weight -> 30, is_hidden -> true))
    )

    val edges = getEdgesSync(queryIntervalWithParent(baseId, index2, "_timestamp", "_parent._timestamp"))
    (edges \ "size").get.toString should be("1")

    val to = (edges \\ "to").head.as[Long]
    to should be (baseId + 20)
  }

  ignore("interval parent with range") {
    val baseId = 9876

    val minute: Long = 60 * 1000L
    val hour = 60 * minute

    insertEdgesSync(
      toEdge(1, insert, e, baseId, baseId + 1, testLabelName, Json.obj(weight -> 30, is_hidden -> true)),
      toEdge(1 + hour * 2, insert, e, baseId + 1, baseId + 10, testLabelName, Json.obj(weight -> 30, is_hidden -> true)),
      toEdge(1 + hour * 3, insert, e, baseId + 1, baseId + 20, testLabelName, Json.obj(weight -> 30, is_hidden -> true)),
      toEdge(1 + hour * 4, insert, e, baseId + 1, baseId + 30, testLabelName, Json.obj(weight -> 30, is_hidden -> true))
    )

    val edges = getEdgesSync(queryIntervalWithParentRange(baseId, index2,
      "_timestamp", "${_parent._timestamp}",
      "_timestamp", "${_parent._timestamp + 3 hour}"))

    (edges \ "size").get.toString should be("2")

    val edges2 = getEdgesSync(queryIntervalWithParentRange(baseId, index2,
      "_timestamp", "${_parent._timestamp}",
      "_timestamp", "${_parent._timestamp + 2 hour}"))

    (edges2 \ "size").get.toString should be("1")

    val edges3 = getEdgesSync(queryIntervalWithParentRange(baseId, index2,
      "_timestamp", "${_parent._timestamp + 130 minute}",
      "_timestamp", "${_parent._timestamp + 4 hour}"))

    (edges3 \ "size").get.toString should be("2")
  }

  test("interval") {
    var edges = getEdgesSync(queryWithInterval(0, index2, "_timestamp", 1000, 1001)) // test interval on timestamp index
    (edges \ "size").get.toString should be("1")

    edges = getEdgesSync(queryWithInterval(0, index2, "_timestamp", 1000, 2000)) // test interval on timestamp index
    (edges \ "size").get.toString should be("2")

    edges = getEdgesSync(queryWithInterval(2, index1, "weight", 10, 11)) // test interval on weight index
    (edges \ "size").get.toString should be("1")

    edges = getEdgesSync(queryWithInterval(2, index1, "weight", 10, 20)) // test interval on weight index
    (edges \ "size").get.toString should be("2")
  }

  test("get edge with where condition") {

    var result = getEdgesSync(getQuery(0, "is_hidden=false and _from in (-1, 0)"))
    (result \ "results").as[List[JsValue]].size should be(1)

    result = getEdgesSync(getQuery(0, "is_hidden=true and _to in (1)"))
    (result \ "results").as[List[JsValue]].size should be(1)

    result = getEdgesSync(getQuery(0, "_from=0"))
    (result \ "results").as[List[JsValue]].size should be(2)

    result = getEdgesSync(getQuery(2, "_from=2 or weight in (-1)"))
    (result \ "results").as[List[JsValue]].size should be(2)

    result = getEdgesSync(getQuery(2, "_from=2 and weight in (10, 20)"))
    (result \ "results").as[List[JsValue]].size should be(2)
  }

  test("get edge exclude") {
    val result = getEdgesSync(queryExclude(0))
    (result \ "results").as[List[JsValue]].size should be(1)
  }

  test("get edge groupBy property") {
    val result = getEdgesSync(queryGroupBy(0, Seq("weight")))
    (result \ "size").as[Int] should be(2)
    val weights = (result \ "results" \\ "groupBy").map { js =>
      (js \ "weight").as[Int]
    }
    weights should contain(30)
    weights should contain(40)

    weights should not contain (10)
  }

  test("edge transform") {
    def queryTransform(id: Int, transforms: String) = Json.parse(
      s"""
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

    var result = getEdgesSync(queryTransform(0, "[[\"_to\"]]"))
    (result \ "results").as[List[JsValue]].size should be(2)

    result = getEdgesSync(queryTransform(0, "[[\"weight\"]]"))
    (result \ "results" \\ "to").map(_.toString).sorted should be((result \ "results" \\ "weight").map(_.toString).sorted)

    result = getEdgesSync(queryTransform(0, "[[\"_from\"]]"))
    (result \ "results" \\ "to").map(_.toString).sorted should be((result \ "results" \\ "from").map(_.toString).sorted)
  }

  test("index") {
    def queryIndex(ids: Seq[Int], indexName: String) = {
      val $from = Json.arr(
        Json.obj("serviceName" -> testServiceName,
          "columnName" -> testColumnName,
          "ids" -> ids))

      val $step = Json.arr(Json.obj("label" -> testLabelName, "index" -> indexName))
      val $steps = Json.arr(Json.obj("step" -> $step))

      val js = Json.obj("withScore" -> false, "srcVertices" -> $from, "steps" -> $steps)
      js
    }

    // weight order
    var result = getEdgesSync(queryIndex(Seq(0), "idx_1"))
    ((result \ "results").as[List[JsValue]].head \\ "weight").head should be(JsNumber(40))

    // timestamp order
    result = getEdgesSync(queryIndex(Seq(0), "idx_2"))
    ((result \ "results").as[List[JsValue]].head \\ "weight").head should be(JsNumber(30))
  }

  //    "checkEdges" in {
  //      running(FakeApplication()) {
  //        val json = Json.parse( s"""
  //         [{"from": 0, "to": 1, "label": "$testLabelName"},
  //          {"from": 0, "to": 2, "label": "$testLabelName"}]
  //        """)
  //
  //        def checkEdges(queryJson: JsValue): JsValue = {
  //          val ret = route(FakeRequest(POST, "/graphs/checkEdges").withJsonBody(queryJson)).get
  //          contentAsJson(ret)
  //        }
  //
  //        val res = checkEdges(json)
  //        val typeRes = res.isInstanceOf[JsArray]
  //        typeRes must equalTo(true)
  //
  //        val fst = res.as[Seq[JsValue]].head \ "to"
  //        fst.as[Int] must equalTo(1)
  //
  //        val snd = res.as[Seq[JsValue]].last \ "to"
  //        snd.as[Int] must equalTo(2)
  //      }
  //    }

  test("duration") {
    def queryDuration(ids: Seq[Int], from: Int, to: Int) = {
      val $from = Json.arr(
        Json.obj("serviceName" -> testServiceName,
          "columnName" -> testColumnName,
          "ids" -> ids))

      val $step = Json.arr(Json.obj(
        "label" -> testLabelName, "direction" -> "out", "offset" -> 0, "limit" -> 100,
        "duration" -> Json.obj("from" -> from, "to" -> to)))

      val $steps = Json.arr(Json.obj("step" -> $step))

      Json.obj("srcVertices" -> $from, "steps" -> $steps)
    }

    // get all
    var result = getEdgesSync(queryDuration(Seq(0, 2), from = 0, to = 5000))
    (result \ "results").as[List[JsValue]].size should be(4)
    // inclusive, exclusive
    result = getEdgesSync(queryDuration(Seq(0, 2), from = 1000, to = 4000))
    (result \ "results").as[List[JsValue]].size should be(3)

    result = getEdgesSync(queryDuration(Seq(0, 2), from = 1000, to = 2000))
    (result \ "results").as[List[JsValue]].size should be(1)

    val bulkEdges = Seq(
      toEdge(1001, insert, e, 0, 1, testLabelName, Json.obj(weight -> 10, is_hidden -> true)),
      toEdge(2002, insert, e, 0, 2, testLabelName, Json.obj(weight -> 20, is_hidden -> false)),
      toEdge(3003, insert, e, 2, 0, testLabelName, Json.obj(weight -> 30)),
      toEdge(4004, insert, e, 2, 1, testLabelName, Json.obj(weight -> 40))
    )
    insertEdgesSync(bulkEdges: _*)

    // duration test after udpate
    // get all
    result = getEdgesSync(queryDuration(Seq(0, 2), from = 0, to = 5000))
    (result \ "results").as[List[JsValue]].size should be(4)

    // inclusive, exclusive
    result = getEdgesSync(queryDuration(Seq(0, 2), from = 1000, to = 4000))
    (result \ "results").as[List[JsValue]].size should be(3)

    result = getEdgesSync(queryDuration(Seq(0, 2), from = 1000, to = 2000))
    (result \ "results").as[List[JsValue]].size should be(1)

  }

  test("return tree") {
    def queryParentsWithoutSelect(id: Long) = Json.parse(
      s"""
        {
          "returnTree": true,
          "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
          [ {
              "label": "$testLabelName",
              "direction": "out",
              "offset": 0,
              "limit": 2
            }
          ],[{
              "label": "$testLabelName",
              "direction": "in",
              "offset": 0,
              "limit": 1000
            }
          ]]
        }""".stripMargin)

    def queryParents(id: Long) = Json.parse(
      s"""
        {
          "select": ["weight"],
          "returnTree": true,
          "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
          [ {
              "label": "$testLabelName",
              "direction": "out",
              "offset": 0,
              "limit": 2
            }
          ],[{
              "label": "$testLabelName",
              "direction": "in",
              "offset": 0,
              "limit": 1000
            }
          ]]
        }""".stripMargin)

    val src = 100
    val tgt = 200

    insertEdgesSync(toEdge(1001, "insert", "e", src, tgt, testLabelName))

    // test parent With select fields
    var result = TestUtil.getEdgesSync(queryParents(src))
    println(s"$result")
    var parents = (result \ "results").as[Seq[JsValue]]
    var ret = parents.forall { edge =>
      val parentEdges = (edge \ "parents").as[Seq[JsValue]]
      val assertSize = parentEdges.size == 1
      val parentProps = (parentEdges.head \ "props").as[JsObject]
      val parentWeight = (parentProps \ "weight").as[Long]
      val parentIsHidden = (parentProps \ "is_hidden").asOpt[Boolean]

      val assertProp = parentWeight == 0 && parentIsHidden.isEmpty // select only "weight"

      assertSize && assertProp
    }

    ret should be(true)

    // test parent With select fields: check default Prop
    result = TestUtil.getEdgesSync(queryParentsWithoutSelect(src))
    parents = (result \ "results").as[Seq[JsValue]]
    ret = parents.forall { edge =>
      val parentEdges = (edge \ "parents").as[Seq[JsValue]]
      val assertSize = parentEdges.size == 1

      val parentProps = (parentEdges.head \ "props").as[JsObject]

      val parentWeight = (parentProps \ "weight").as[Int]
      val parentIsHidden = (parentProps \ "is_hidden").as[Boolean]
      val parentIsBlocked = (parentProps \ "is_blocked").as[Boolean]
      val parentTime = (parentProps \ "time").as[Long]

      val assertProp =
        parentWeight == 0 && parentIsHidden == false && parentIsBlocked == false && parentTime == 0

      assertSize && assertProp
    }

    ret should be(true)
  }


  test("pagination and _to") {
    def querySingleWithTo(id: Int, offset: Int = 0, limit: Int = 100, to: Int) = Json.parse(
      s"""
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

    val src = System.currentTimeMillis().toInt

    val bulkEdges = Seq(
      toEdge(1001, insert, e, src, 1, testLabelName, Json.obj(weight -> 10, is_hidden -> true)),
      toEdge(2002, insert, e, src, 2, testLabelName, Json.obj(weight -> 20, is_hidden -> false)),
      toEdge(3003, insert, e, src, 3, testLabelName, Json.obj(weight -> 30)),
      toEdge(4004, insert, e, src, 4, testLabelName, Json.obj(weight -> 40))
    )
    insertEdgesSync(bulkEdges: _*)

    var result = getEdgesSync(querySingle(src, offset = 0, limit = 2))
    var edges = (result \ "results").as[List[JsValue]]

    edges.size should be(2)
    (edges(0) \ "to").as[Long] should be(4)
    (edges(1) \ "to").as[Long] should be(3)

    result = getEdgesSync(querySingle(src, offset = 1, limit = 2))

    edges = (result \ "results").as[List[JsValue]]
    edges.size should be(2)
    (edges(0) \ "to").as[Long] should be(3)
    (edges(1) \ "to").as[Long] should be(2)

    result = getEdgesSync(querySingleWithTo(src, offset = 0, limit = -1, to = 1))
    edges = (result \ "results").as[List[JsValue]]
    edges.size should be(1)
  }

  test("order by") {
    def queryScore(id: Int, scoring: Map[String, Int]): JsValue = Json.obj(
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
    def queryOrderBy(id: Int, scoring: Map[String, Int], props: Seq[Map[String, String]]): JsValue = Json.obj(
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

    val bulkEdges = Seq(
      toEdge(1001, insert, e, 0, 1, testLabelName, Json.obj(weight -> 10, is_hidden -> true)),
      toEdge(2002, insert, e, 0, 2, testLabelName, Json.obj(weight -> 20, is_hidden -> false)),
      toEdge(3003, insert, e, 2, 0, testLabelName, Json.obj(weight -> 30)),
      toEdge(4004, insert, e, 2, 1, testLabelName, Json.obj(weight -> 40))
    )

    insertEdgesSync(bulkEdges: _*)

    // get edges
    val edges = getEdgesSync(queryScore(0, Map("weight" -> 1)))
    val orderByScore = getEdgesSync(queryOrderBy(0, Map("weight" -> 1), Seq(Map("score" -> "DESC", "timestamp" -> "DESC"))))
    val ascOrderByScore = getEdgesSync(queryOrderBy(0, Map("weight" -> 1), Seq(Map("score" -> "ASC", "timestamp" -> "DESC"))))

    val edgesTo = edges \ "results" \\ "to"
    val orderByTo = orderByScore \ "results" \\ "to"
    val ascOrderByTo = ascOrderByScore \ "results" \\ "to"

    edgesTo should be(Seq(JsNumber(2), JsNumber(1)))
    edgesTo should be(orderByTo)
    ascOrderByTo should be(Seq(JsNumber(1), JsNumber(2)))
    edgesTo.reverse should be(ascOrderByTo)
  }

  test("query with sampling") {
    def queryWithSampling(id: Int, sample: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
            {
              "step": [{
                "label": "$testLabelName",
                "direction": "out",
                "offset": 0,
                "limit": 100,
                "sample": $sample
                }]
            }
          ]
        }""")

    def twoStepQueryWithSampling(id: Int, sample: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
            {
              "step": [{
                "label": "$testLabelName",
                "direction": "out",
                "offset": 0,
                "limit": 100,
                "sample": $sample
                }]
            },
            {
               "step": [{
                 "label": "$testLabelName",
                 "direction": "out",
                 "offset": 0,
                 "limit": 100,
                 "sample": $sample
               }]
            }
          ]
        }""")

    def twoQueryWithSampling(id: Int, sample: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
            {
              "step": [{
                "label": "$testLabelName",
                "direction": "out",
                "offset": 0,
                "limit": 50,
                "sample": $sample
              },
              {
                "label": "$testLabelName2",
                "direction": "out",
                "offset": 0,
                "limit": 50
              }]
            }
          ]
        }""")

    val sampleSize = 2
    val ts = "1442985659166"
    val testId = 22

    val bulkEdges = Seq(
      toEdge(ts, insert, e, testId, 122, testLabelName),
      toEdge(ts, insert, e, testId, 222, testLabelName),
      toEdge(ts, insert, e, testId, 322, testLabelName),

      toEdge(ts, insert, e, testId, 922, testLabelName2),
      toEdge(ts, insert, e, testId, 222, testLabelName2),
      toEdge(ts, insert, e, testId, 322, testLabelName2),

      toEdge(ts, insert, e, 122, 1122, testLabelName),
      toEdge(ts, insert, e, 122, 1222, testLabelName),
      toEdge(ts, insert, e, 122, 1322, testLabelName),
      toEdge(ts, insert, e, 222, 2122, testLabelName),
      toEdge(ts, insert, e, 222, 2222, testLabelName),
      toEdge(ts, insert, e, 222, 2322, testLabelName),
      toEdge(ts, insert, e, 322, 3122, testLabelName),
      toEdge(ts, insert, e, 322, 3222, testLabelName),
      toEdge(ts, insert, e, 322, 3322, testLabelName)
    )

    insertEdgesSync(bulkEdges: _*)

    val result1 = getEdgesSync(queryWithSampling(testId, sampleSize))
    (result1 \ "results").as[List[JsValue]].size should be(math.min(sampleSize, bulkEdges.size))

    val result2 = getEdgesSync(twoStepQueryWithSampling(testId, sampleSize))
    (result2 \ "results").as[List[JsValue]].size should be(math.min(sampleSize * sampleSize, bulkEdges.size * bulkEdges.size))

    val result3 = getEdgesSync(twoQueryWithSampling(testId, sampleSize))
    (result3 \ "results").as[List[JsValue]].size should be(sampleSize + 3) // edges in testLabelName2 = 3
  }
  test("test query with filterOut query") {
    def queryWithFilterOut(id1: String, id2: String) = Json.parse(
      s"""{
         |	"limit": 10,
         |	"filterOut": {
         |		"srcVertices": [{
         |			"serviceName": "$testServiceName",
         |			"columnName": "$testColumnName",
         |			"id": $id1
         |		}],
         |		"steps": [{
         |			"step": [{
         |				"label": "$testLabelName",
         |				"direction": "out",
         |				"offset": 0,
         |				"limit": 10
         |			}]
         |		}]
         |	},
         |	"srcVertices": [{
         |		"serviceName": "$testServiceName",
         |		"columnName": "$testColumnName",
         |		"id": $id2
         |	}],
         |	"steps": [{
         |		"step": [{
         |			"label": "$testLabelName",
         |			"direction": "out",
         |			"offset": 0,
         |			"limit": 5
         |		}]
         |	}]
         |}
       """.stripMargin
    )

    val testId1 = "-23"
    val testId2 = "-25"

    val bulkEdges = Seq(
      toEdge(1, insert, e, testId1, 111, testLabelName, Json.obj(weight -> 10)),
      toEdge(2, insert, e, testId1, 222, testLabelName, Json.obj(weight -> 10)),
      toEdge(3, insert, e, testId1, 333, testLabelName, Json.obj(weight -> 10)),
      toEdge(4, insert, e, testId2, 111, testLabelName, Json.obj(weight -> 1)),
      toEdge(5, insert, e, testId2, 333, testLabelName, Json.obj(weight -> 1)),
      toEdge(6, insert, e, testId2, 555, testLabelName, Json.obj(weight -> 1))
    )
    logger.debug(s"${bulkEdges.mkString("\n")}")
    insertEdgesSync(bulkEdges: _*)

    val rs = getEdgesSync(queryWithFilterOut(testId1, testId2))
    logger.debug(Json.prettyPrint(rs))
    val results = (rs \ "results").as[List[JsValue]]
    results.size should be(1)
    (results(0) \ "to").get.toString should be("555")
  }


  /** note that this merge two different label result into one */
  test("weighted union") {
    def queryWithWeightedUnion(id1: String, id2: String) = Json.parse(
      s"""
				|{
				|  "limit": 10,
				|  "weights": [
				|    10,
				|    1
				|  ],
				|  "groupBy": ["weight"],
				|  "queries": [
				|    {
				|      "srcVertices": [
				|        {
				|          "serviceName": "$testServiceName",
				|          "columnName": "$testColumnName",
				|          "id": $id1
				|        }
				|      ],
				|      "steps": [
				|        {
				|          "step": [
				|            {
				|              "label": "$testLabelName",
				|              "direction": "out",
				|              "offset": 0,
				|              "limit": 5
				|            }
				|          ]
				|        }
				|      ]
				|    },
				|    {
				|      "srcVertices": [
				|        {
				|          "serviceName": "$testServiceName",
				|          "columnName": "$testColumnName",
				|          "id": $id2
				|        }
				|      ],
				|      "steps": [
				|        {
				|          "step": [
				|            {
				|              "label": "$testLabelName2",
				|              "direction": "out",
				|              "offset": 0,
				|              "limit": 5
				|            }
				|          ]
				|        }
				|      ]
				|    }
				|  ]
				|}
       """.stripMargin
    )

    val testId1 = "1"
    val testId2 = "2"

    val bulkEdges = Seq(
      toEdge(1, insert, e, testId1, 111, testLabelName, Json.obj(weight -> 10)),
      toEdge(2, insert, e, testId1, 222, testLabelName, Json.obj(weight -> 10)),
      toEdge(3, insert, e, testId1, 333, testLabelName, Json.obj(weight -> 10)),
      toEdge(4, insert, e, testId2, 444, testLabelName2, Json.obj(weight -> 1)),
      toEdge(5, insert, e, testId2, 555, testLabelName2, Json.obj(weight -> 1)),
      toEdge(6, insert, e, testId2, 666, testLabelName2, Json.obj(weight -> 1))
    )

    insertEdgesSync(bulkEdges: _*)

    val rs = getEdgesSync(queryWithWeightedUnion(testId1, testId2))
    logger.debug(Json.prettyPrint(rs))
    val results = (rs \ "results").as[List[JsValue]]
    results.size should be(2)
    (results(0) \ "scoreSum").as[Float] should be(30.0)
    (results(0) \ "agg").as[List[JsValue]].size should be(3)
    (results(1) \ "scoreSum").as[Float] should be(3.0)
    (results(1) \ "agg").as[List[JsValue]].size should be(3)
  }

  test("weighted union with options") {
    def queryWithWeightedUnionWithOptions(id1: String, id2: String) = Json.parse(
      s"""
         |{
         |  "limit": 10,
         |  "weights": [
         |    10,
         |    1
         |  ],
         |  "groupBy": ["to"],
         |  "select": ["to", "weight"],
         |  "filterOut": {
         |    "srcVertices": [
         |      {
         |        "serviceName": "$testServiceName",
         |        "columnName": "$testColumnName",
         |        "id": $id1
         |      }
         |    ],
         |    "steps": [
         |      {
         |        "step": [
         |          {
         |            "label": "$testLabelName",
         |            "direction": "out",
         |            "offset": 0,
         |            "limit": 10
         |          }
         |        ]
         |      }
         |    ]
         |  },
         |  "queries": [
         |    {
         |      "srcVertices": [
         |        {
         |          "serviceName": "$testServiceName",
         |          "columnName": "$testColumnName",
         |          "id": $id1
         |        }
         |      ],
         |      "steps": [
         |        {
         |          "step": [
         |            {
         |              "label": "$testLabelName",
         |              "direction": "out",
         |              "offset": 0,
         |              "limit": 5
         |            }
         |          ]
         |        }
         |      ]
         |    },
         |    {
         |      "srcVertices": [
         |        {
         |          "serviceName": "$testServiceName",
         |          "columnName": "$testColumnName",
         |          "id": $id2
         |        }
         |      ],
         |      "steps": [
         |        {
         |          "step": [
         |            {
         |              "label": "$testLabelName",
         |              "direction": "out",
         |              "offset": 0,
         |              "limit": 5
         |            }
         |          ]
         |        }
         |      ]
         |    }
         |  ]
         |}
       """.stripMargin
    )

    val testId1 = "-192848"
    val testId2 = "-193849"

    val bulkEdges = Seq(
      toEdge(1, insert, e, testId1, 111, testLabelName, Json.obj(weight -> 10)),
      toEdge(2, insert, e, testId1, 222, testLabelName, Json.obj(weight -> 10)),
      toEdge(3, insert, e, testId1, 333, testLabelName, Json.obj(weight -> 10)),
      toEdge(4, insert, e, testId2, 111, testLabelName, Json.obj(weight -> 1)),
      toEdge(5, insert, e, testId2, 333, testLabelName, Json.obj(weight -> 1)),
      toEdge(6, insert, e, testId2, 555, testLabelName, Json.obj(weight -> 1))
    )

    insertEdgesSync(bulkEdges: _*)

    val rs = getEdgesSync(queryWithWeightedUnionWithOptions(testId1, testId2))
    logger.debug(Json.prettyPrint(rs))
    val results = (rs \ "results").as[List[JsValue]]
    results.size should be(1)

  }

  test("scoreThreshold") {
    def queryWithScoreThreshold(id: String, scoreThreshold: Int) = Json.parse(
      s"""{
         |  "limit": 10,
         |  "scoreThreshold": $scoreThreshold,
         |  "groupBy": ["to"],
         |  "srcVertices": [
         |    {
         |      "serviceName": "$testServiceName",
         |      "columnName": "$testColumnName",
         |      "id": $id
         |    }
         |  ],
         |  "steps": [
         |    {
         |      "step": [
         |        {
         |          "label": "$testLabelName",
         |          "direction": "out",
         |          "offset": 0,
         |          "limit": 10
         |        }
         |      ]
         |    },
         |    {
         |      "step": [
         |        {
         |          "label": "$testLabelName",
         |          "direction": "out",
         |          "offset": 0,
         |          "limit": 10
         |        }
         |      ]
         |    }
         |  ]
         |}
       """.stripMargin
    )

    val testId = "-23903"

    val bulkEdges = Seq(
      toEdge(1, insert, e, testId, 101, testLabelName, Json.obj(weight -> 10)),
      toEdge(1, insert, e, testId, 102, testLabelName, Json.obj(weight -> 10)),
      toEdge(1, insert, e, testId, 103, testLabelName, Json.obj(weight -> 10)),
      toEdge(1, insert, e, 101, 102, testLabelName, Json.obj(weight -> 10)),
      toEdge(1, insert, e, 101, 103, testLabelName, Json.obj(weight -> 10)),
      toEdge(1, insert, e, 101, 104, testLabelName, Json.obj(weight -> 10)),
      toEdge(1, insert, e, 102, 103, testLabelName, Json.obj(weight -> 10)),
      toEdge(1, insert, e, 102, 104, testLabelName, Json.obj(weight -> 10)),
      toEdge(1, insert, e, 103, 105, testLabelName, Json.obj(weight -> 10))
    )
    // expected: 104 -> 2, 103 -> 2, 102 -> 1,, 105 -> 1
    insertEdgesSync(bulkEdges: _*)

    var rs = getEdgesSync(queryWithScoreThreshold(testId, 2))
    logger.debug(Json.prettyPrint(rs))
    var results = (rs \ "results").as[List[JsValue]]
    results.size should be(2)

    rs = getEdgesSync(queryWithScoreThreshold(testId, 1))
    logger.debug(Json.prettyPrint(rs))

    results = (rs \ "results").as[List[JsValue]]
    results.size should be(4)
  }

  // called by each test, each
  override def beforeEach = initTestData()

  // called by start test, once
  override def initTestData(): Unit = {
    super.initTestData()

    insertEdgesSync(
      toEdge(1000, insert, e, 0, 1, testLabelName, Json.obj(weight -> 40, is_hidden -> true)),
      toEdge(2000, insert, e, 0, 2, testLabelName, Json.obj(weight -> 30, is_hidden -> false)),
      toEdge(3000, insert, e, 2, 0, testLabelName, Json.obj(weight -> 20)),
      toEdge(4000, insert, e, 2, 1, testLabelName, Json.obj(weight -> 10)),
      toEdge(3000, insert, e, 10, 20, testLabelName, Json.obj(weight -> 20)),
      toEdge(4000, insert, e, 20, 20, testLabelName, Json.obj(weight -> 10)),
      toEdge(1, insert, e, -1, 1000, testLabelName),
      toEdge(1, insert, e, -1, 2000, testLabelName),
      toEdge(1, insert, e, -1, 3000, testLabelName),
      toEdge(1, insert, e, 1000, 10000, testLabelName),
      toEdge(1, insert, e, 1000, 11000, testLabelName),
      toEdge(1, insert, e, 2000, 11000, testLabelName),
      toEdge(1, insert, e, 2000, 12000, testLabelName),
      toEdge(1, insert, e, 3000, 12000, testLabelName),
      toEdge(1, insert, e, 3000, 13000, testLabelName),
      toEdge(1, insert, e, 10000, 100000, testLabelName),
      toEdge(2, insert, e, 11000, 200000, testLabelName),
      toEdge(3, insert, e, 12000, 300000, testLabelName)
    )
  }
}
