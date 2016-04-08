package org.apache.s2graph.core.Integrate

import org.apache.s2graph.core.utils.logger
import org.scalatest.BeforeAndAfterEach
import play.api.libs.json._

class QueryTest extends IntegrateCommon with BeforeAndAfterEach {

  import TestUtil._

  val insert = "insert"
  val e = "e"
  val weight = "weight"
  val is_hidden = "is_hidden"

  versions map { n =>

    val ver = s"v$n"
    val label = getLabelName(ver)
    val label2 = getLabelName2(ver)
    val tag = getTag(ver)

    test(s"interval $ver", tag) {

      var edges = getEdgesSync(queryWithInterval(label, 0, index2, "_timestamp", 1000, 1001)) // test interval on timestamp index
      (edges \ "size").toString should be("1")

      edges = getEdgesSync(queryWithInterval(label, 0, index2, "_timestamp", 1000, 2000)) // test interval on timestamp index
      (edges \ "size").toString should be("2")

      edges = getEdgesSync(queryWithInterval(label, 2, index1, "weight", 10, 11)) // test interval on weight index
      (edges \ "size").toString should be("1")

      edges = getEdgesSync(queryWithInterval(label, 2, index1, "weight", 10, 20)) // test interval on weight index
      (edges \ "size").toString should be("2")
    }

    test(s"get edge with where condition $ver", tag) {

      var result = getEdgesSync(queryWhere(0, label, "is_hidden=false and _from in (-1, 0)"))
      (result \ "results").as[List[JsValue]].size should be(1)

      result = getEdgesSync(queryWhere(0, label, "is_hidden=true and _to in (1)"))
      (result \ "results").as[List[JsValue]].size should be(1)

      result = getEdgesSync(queryWhere(0, label, "_from=0"))
      (result \ "results").as[List[JsValue]].size should be(2)

      result = getEdgesSync(queryWhere(2, label, "_from=2 or weight in (-1)"))
      (result \ "results").as[List[JsValue]].size should be(2)

      result = getEdgesSync(queryWhere(2, label, "_from=2 and weight in (10, 20)"))
      (result \ "results").as[List[JsValue]].size should be(2)
    }

    test(s"get edge exclude $ver", tag) {
      val result = getEdgesSync(queryExclude(0, label))
      (result \ "results").as[List[JsValue]].size should be(1)
    }

    test(s"get edge groupBy property $ver", tag) {

      val result = getEdgesSync(queryGroupBy(0, label, Seq("weight")))
      (result \ "size").as[Int] should be(2)
      val weights = (result \\ "groupBy").map { js =>
        (js \ "weight").as[Int]
      }
      weights should contain(30)
      weights should contain(40)

      weights should not contain (10)
    }

    test(s"edge transform $ver", tag) {

      var result = getEdgesSync(queryTransform(0, label, "[[\"_to\"]]"))
      (result \ "results").as[List[JsValue]].size should be(2)

      result = getEdgesSync(queryTransform(0, label, "[[\"weight\"]]"))
      (result \\ "to").map(_.toString).sorted should be((result \\ "weight").map(_.toString).sorted)

      result = getEdgesSync(queryTransform(0, label, "[[\"_from\"]]"))
      val results = (result \ "results").as[JsValue]
      (result \\ "to").map(_.toString).sorted should be((results \\ "from").map(_.toString).sorted)
    }


    test(s"index $ver", tag) {
      // weight order
      var result = getEdgesSync(queryIndex(Seq(0), label, "idx_1"))
      ((result \ "results").as[List[JsValue]].head \\ "weight").head should be(JsNumber(40))

      // timestamp order
      result = getEdgesSync(queryIndex(Seq(0), label, "idx_2"))
      ((result \ "results").as[List[JsValue]].head \\ "weight").head should be(JsNumber(30))
    }


    test(s"return tree $ver", tag) {
      val src = 100
      val tgt = 200

      insertEdgesSync(toEdge(1001, "insert", "e", src, tgt, label))

      val result = TestUtil.getEdgesSync(queryParents(src))
      val parents = (result \ "results").as[Seq[JsValue]]
      val ret = parents.forall {
        edge => (edge \ "parents").as[Seq[JsValue]].size == 1
      }

      ret should be(true)
    }



//    test(s"pagination and _to $ver", tag) {
//      val src = System.currentTimeMillis().toInt
//
//      val bulkEdges = Seq(
//        toEdge(1001, insert, e, src, 1, label, Json.obj(weight -> 10, is_hidden -> true)),
//        toEdge(2002, insert, e, src, 2, label, Json.obj(weight -> 20, is_hidden -> false)),
//        toEdge(3003, insert, e, src, 3, label, Json.obj(weight -> 30)),
//        toEdge(4004, insert, e, src, 4, label, Json.obj(weight -> 40))
//      )
//      insertEdgesSync(bulkEdges: _*)
//
//      var result = getEdgesSync(querySingle(src, label, offset = 1, limit = 2))
//
//      var edges = (result \ "results").as[List[JsValue]]
//
//      edges.size should be(2)
//      (edges(0) \ "to").as[Long] should be(4)
//      (edges(1) \ "to").as[Long] should be(3)
//
//      result = getEdgesSync(querySingle(src, label, offset = 1, limit = 2))
//
//      edges = (result \ "results").as[List[JsValue]]
//      edges.size should be(2)
//      (edges(0) \ "to").as[Long] should be(3)
//      (edges(1) \ "to").as[Long] should be(2)
//
//      result = getEdgesSync(querySingleWithTo(src, label, offset = 0, limit = -1, to = 1))
//      edges = (result \ "results").as[List[JsValue]]
//      edges.size should be(1)
//    }

    test(s"order by $ver", tag) {
      val bulkEdges = Seq(
        toEdge(1001, insert, e, 0, 1, label, Json.obj(weight -> 10, is_hidden -> true)),
        toEdge(2002, insert, e, 0, 2, label, Json.obj(weight -> 20, is_hidden -> false)),
        toEdge(3003, insert, e, 2, 0, label, Json.obj(weight -> 30)),
        toEdge(4004, insert, e, 2, 1, label, Json.obj(weight -> 40))
      )

      insertEdgesSync(bulkEdges: _*)

      // get edges
      val edges = getEdgesSync(queryScore(0, label, Map("weight" -> 1)))
      val orderByScore = getEdgesSync(queryOrderBy(0, label, Map("weight" -> 1), Seq(Map("score" -> "DESC", "timestamp" -> "DESC"))))
      val ascOrderByScore = getEdgesSync(queryOrderBy(0, label, Map("weight" -> 1), Seq(Map("score" -> "ASC", "timestamp" -> "DESC"))))

      val edgesTo = edges \ "results" \\ "to"
      val orderByTo = orderByScore \ "results" \\ "to"
      val ascOrderByTo = ascOrderByScore \ "results" \\ "to"

      edgesTo should be(Seq(JsNumber(2), JsNumber(1)))
      edgesTo should be(orderByTo)
      ascOrderByTo should be(Seq(JsNumber(1), JsNumber(2)))
      edgesTo.reverse should be(ascOrderByTo)
    }

    test(s"query with sampling $ver", tag) {

      val sampleSize = 2
      val ts = "1442985659166"
      val testId = 22

      val bulkEdges = Seq(
        toEdge(ts, insert, e, testId, 122, label),
        toEdge(ts, insert, e, testId, 222, label),
        toEdge(ts, insert, e, testId, 322, label),

        toEdge(ts, insert, e, testId, 922, label2),
        toEdge(ts, insert, e, testId, 222, label2),
        toEdge(ts, insert, e, testId, 322, label2),

        toEdge(ts, insert, e, 122, 1122, label),
        toEdge(ts, insert, e, 122, 1222, label),
        toEdge(ts, insert, e, 122, 1322, label),
        toEdge(ts, insert, e, 222, 2122, label),
        toEdge(ts, insert, e, 222, 2222, label),
        toEdge(ts, insert, e, 222, 2322, label),
        toEdge(ts, insert, e, 322, 3122, label),
        toEdge(ts, insert, e, 322, 3222, label),
        toEdge(ts, insert, e, 322, 3322, label)
      )

      insertEdgesSync(bulkEdges: _*)
      val result0 = getEdgesSync(querySingle(testId, label))
      logger.debug(s"2-step sampling res 0: ${Json.prettyPrint(result0)}")
      (result0 \ "results").as[List[JsValue]].size should be(3)

      val result1 = getEdgesSync(queryWithSampling(testId, label, sampleSize))
      logger.debug(s"2-step sampling res 1: ${Json.prettyPrint(result1)}")
      (result1 \ "results").as[List[JsValue]].size should be(math.min(sampleSize, bulkEdges.size))

      val result2 = getEdgesSync(twoStepQueryWithSampling(testId, label, sampleSize))
      logger.debug(s"2-step sampling res 2: ${Json.prettyPrint(result2)}")
      (result2 \ "results").as[List[JsValue]].size should be(math.min(sampleSize * sampleSize, bulkEdges.size * bulkEdges.size))

      val result3 = getEdgesSync(twoQueryWithSampling(testId, label, label2, sampleSize))
      logger.debug(s"2-step sampling res 3: ${Json.prettyPrint(result3)}")
      (result3 \ "results").as[List[JsValue]].size should be(sampleSize + 3) // edges in testLabelName2 = 3
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
  }

  // called by each test, each
  override def beforeEach = initTestData()

  // called by start test, once
  override def initTestData(): Unit = {
    super.initTestData()

    val vers = config.getString("s2graph.storage.backend") match {
      case "hbase" => versions
      case "redis" => Seq(4)
      case _ => throw new RuntimeException("not supported storage.")
    }

    vers map { n =>
      val ver = s"v$n"
      val label = getLabelName(ver)
      insertEdgesSync(
        toEdge(1000, insert, e, 0, 1, label, Json.obj(weight -> 40, is_hidden -> true)),
        toEdge(2000, insert, e, 0, 2, label, Json.obj(weight -> 30, is_hidden -> false)),
        toEdge(3000, insert, e, 2, 0, label, Json.obj(weight -> 20)),
        toEdge(4000, insert, e, 2, 1, label, Json.obj(weight -> 10)),
        toEdge(3000, insert, e, 10, 20, label, Json.obj(weight -> 20)),
        toEdge(4000, insert, e, 20, 20, label, Json.obj(weight -> 10)),
        toEdge(1, insert, e, -1, 1000, label),
        toEdge(1, insert, e, -1, 2000, label),
        toEdge(1, insert, e, -1, 3000, label),
        toEdge(1, insert, e, 1000, 10000, label),
        toEdge(1, insert, e, 1000, 11000, label),
        toEdge(1, insert, e, 2000, 11000, label),
        toEdge(1, insert, e, 2000, 12000, label),
        toEdge(1, insert, e, 3000, 12000, label),
        toEdge(1, insert, e, 3000, 13000, label),
        toEdge(1, insert, e, 10000, 100000, label),
        toEdge(2, insert, e, 11000, 200000, label),
        toEdge(3, insert, e, 12000, 300000, label)
      )
    }
  }
}
