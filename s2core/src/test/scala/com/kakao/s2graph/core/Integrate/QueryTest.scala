package com.kakao.s2graph.core.Integrate

import com.kakao.s2graph.core.GraphExceptions.BadQueryException
import com.kakao.s2graph.core.utils.logger
import org.scalatest.BeforeAndAfterEach
import play.api.libs.json.{JsNull, JsNumber, JsValue, Json}

import scala.util.{Success, Try}

class QueryTest extends IntegrateCommon with BeforeAndAfterEach {

  import TestUtil._

  val insert = "insert"
  val e = "e"
  val weight = "weight"
  val is_hidden = "is_hidden"


  test("interval") {
    def queryWithInterval(id: Int, index: String, prop: String, fromVal: Int, toVal: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
          [ {
              "label": "$testLabelName",
              "index": "$index",
              "interval": {
                  "from": [ { "$prop": $fromVal } ],
                  "to": [ { "$prop": $toVal } ]
              }
            }
          ]]
        }
        """)

    var edges = getEdgesSync(queryWithInterval(0, index2, "_timestamp", 1000, 1001)) // test interval on timestamp index
    (edges \ "size").toString should be("1")

    edges = getEdgesSync(queryWithInterval(0, index2, "_timestamp", 1000, 2000)) // test interval on timestamp index
    (edges \ "size").toString should be("2")

    edges = getEdgesSync(queryWithInterval(2, index1, "weight", 10, 11)) // test interval on weight index
    (edges \ "size").toString should be("1")

    edges = getEdgesSync(queryWithInterval(2, index1, "weight", 10, 20)) // test interval on weight index
    (edges \ "size").toString should be("2")
  }

  test("get edge with where condition") {
    def queryWhere(id: Int, where: String) = Json.parse(
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
              "limit": 100,
              "where": "${where}"
            }
          ]]
        }""")

    var result = getEdgesSync(queryWhere(0, "is_hidden=false and _from in (-1, 0)"))
    (result \ "results").as[List[JsValue]].size should be(1)

    result = getEdgesSync(queryWhere(0, "is_hidden=true and _to in (1)"))
    (result \ "results").as[List[JsValue]].size should be(1)

    result = getEdgesSync(queryWhere(0, "_from=0"))
    (result \ "results").as[List[JsValue]].size should be(2)

    result = getEdgesSync(queryWhere(2, "_from=2 or weight in (-1)"))
    (result \ "results").as[List[JsValue]].size should be(2)

    result = getEdgesSync(queryWhere(2, "_from=2 and weight in (10, 20)"))
    (result \ "results").as[List[JsValue]].size should be(2)
  }

  test("get edge exclude") {
    def queryExclude(id: Int) = Json.parse(
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

    val result = getEdgesSync(queryExclude(0))
    (result \ "results").as[List[JsValue]].size should be(1)
  }

  test("get edge groupBy property") {
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

    def a: JsValue = getEdgesSync(queryDuration(Seq(0, 2), from = 3000, to = 2000))
    Try(a).recover {
      case e: BadQueryException => JsNull
    } should be(Success(JsNull))
  }


  test("return tree") {
    def queryParents(id: Long) = Json.parse(
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
              "limit": -1
            }
          ]]
        }""".stripMargin)

    val src = 100
    val tgt = 200

    insertEdgesSync(toEdge(1001, "insert", "e", src, tgt, testLabelName))

    val result = TestUtil.getEdgesSync(queryParents(src))
    val parents = (result \ "results").as[Seq[JsValue]]
    val ret = parents.forall {
      edge => (edge \ "parents").as[Seq[JsValue]].size == 1
    }

    ret should be(true)
  }



//  test("pagination and _to") {
//    def querySingleWithTo(id: Int, offset: Int = 0, limit: Int = 100, to: Int) = Json.parse(
//      s"""
//        { "srcVertices": [
//          { "serviceName": "${testServiceName}",
//            "columnName": "${testColumnName}",
//            "id": ${id}
//           }],
//          "steps": [
//          [ {
//              "label": "${testLabelName}",
//              "direction": "out",
//              "offset": $offset,
//              "limit": $limit,
//              "_to": $to
//            }
//          ]]
//        }
//        """)
//
//    val src = System.currentTimeMillis().toInt
//
//    val bulkEdges = Seq(
//      toEdge(1001, insert, e, src, 1, testLabelName, Json.obj(weight -> 10, is_hidden -> true)),
//      toEdge(2002, insert, e, src, 2, testLabelName, Json.obj(weight -> 20, is_hidden -> false)),
//      toEdge(3003, insert, e, src, 3, testLabelName, Json.obj(weight -> 30)),
//      toEdge(4004, insert, e, src, 4, testLabelName, Json.obj(weight -> 40))
//    )
//    insertEdgesSync(bulkEdges: _*)
//
//    var result = getEdgesSync(querySingle(src, offset = 0, limit = 2))
//    var edges = (result \ "results").as[List[JsValue]]
//
//    edges.size should be(2)
//    (edges(0) \ "to").as[Long] should be(4)
//    (edges(1) \ "to").as[Long] should be(3)
//
//    result = getEdgesSync(querySingle(src, offset = 1, limit = 2))
//
//    edges = (result \ "results").as[List[JsValue]]
//    edges.size should be(2)
//    (edges(0) \ "to").as[Long] should be(3)
//    (edges(1) \ "to").as[Long] should be(2)
//
//    result = getEdgesSync(querySingleWithTo(src, offset = 0, limit = -1, to = 1))
//    edges = (result \ "results").as[List[JsValue]]
//    edges.size should be(1)
//  }
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
    (results(0) \ "to").toString should be("555")
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

    val testId1 = "-192848"
    val testId2 = "-193849"

    val bulkEdges = Seq(
      toEdge(1, insert, e, testId1, 111, testLabelName, Json.obj(weight -> 10)),
      toEdge(2, insert, e, testId1, 222, testLabelName, Json.obj(weight -> 10)),
      toEdge(3, insert, e, testId1, 333, testLabelName, Json.obj(weight -> 10)),
      toEdge(4, insert, e, testId2, 111, testLabelName2, Json.obj(weight -> 1)),
      toEdge(5, insert, e, testId2, 333, testLabelName2, Json.obj(weight -> 1)),
      toEdge(6, insert, e, testId2, 555, testLabelName2, Json.obj(weight -> 1))
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
