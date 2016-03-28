package org.apache.s2graph.core.storage.redis

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.mysqls.Label
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.{Graph, Management, RedisTest}
import org.scalatest.BeforeAndAfterEach
import play.api.libs.json.{JsValue, Json}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext

/**
 * Created by june.kay on 2016. 1. 20..
 */
class RedisCrudTest extends IntegrateCommon with BeforeAndAfterEach {
  import TestUtil._

  val insert = "insert"
  val increment = "increment"
  val e = "e"
  val sid1 = 1000001
  val sid2 = 1000002
  val sid3 = 1000003
  val sid4 = 1000004
  val sid6 = 1000006
  val tid1 = 1000
  val tid2 = 1100
  val tid3 = 1110
  val tid4 = 2000

  val testLabel = "redis_crud_label"
  val testService = "redis_crud_service"
  val testColumn = "redis_crud_column"

  override def beforeAll = {
    config = ConfigFactory.load()
      .withValue("storage.engine", ConfigValueFactory.fromAnyRef("redis")) // for redis test
      .withValue("storage.redis.instances", ConfigValueFactory.fromIterable(List[String]("localhost"))) // for redis test

//    println(s">> Config for storage.engine : ${config.getString("storage.engine")}")
//    println(s">> Config for redis.instances : ${config.getStringList("storage.redis.instances").mkString(",")}")

    graph = new Graph(config)(ExecutionContext.Implicits.global)
    parser = new RequestParser(graph.config)
    management = new Management(graph)
    initTestData()
  }

  /**
   * Make Service, Label, Vertex for integrate test
   */
  override def initTestData() = {
//    println("[Redis init start]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    Management.deleteService(testService)

    // 1. createService
    val jsValue = Json.parse(s"""{"serviceName" : "$testService"}""")
    val (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) =
      parser.toServiceElements(jsValue)

    val tryRes =
      management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
//    println(s">> Service created : $createService, $tryRes")

    def lc(ver: String, consistency: String = "strong") = {
      s"""
  {
    "label": "$testLabel",
    "srcServiceName": "$testService",
    "srcColumnName": "$testColumn",
    "srcColumnType": "long",
     "tgtServiceName": "$testService",
    "tgtColumnName": "$testColumn",
    "tgtColumnType": "long",
    "indices": [
      {"name": "$index1", "propNames": ["weight", "time", "is_hidden", "is_blocked"]},
      {"name": "$index2", "propNames": ["_timestamp"]}
    ],
    "props": [
    {
      "name": "time",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "weight",
      "dataType": "long",
      "defaultValue": 0
    },
    {
      "name": "is_hidden",
      "dataType": "boolean",
      "defaultValue": false
    },
    {
      "name": "is_blocked",
      "dataType": "boolean",
      "defaultValue": false
    }
    ],
    "consistencyLevel": "$consistency",
    "schemaVersion": "$ver",
    "compressionAlgorithm": "gz"
  }"""
    }

    // with only v4 label
    val labelNames = Map(testLabel -> lc("v4"))

    for {
      (labelName, create) <- labelNames
    } {
      Management.deleteLabel(labelName)
      Label.findByName(labelName, useCache = false) match {
        case None =>
          val json = Json.parse(create)
          val tryRes = for {
            labelArgs <- parser.toLabelElements(json)
            label <- (management.createLabel _).tupled(labelArgs)
          } yield label

          tryRes.get
        case Some(label) =>
//          println(s">> Label already exist: $create, $label")
      }
    }

    val vertexPropsKeys = List("age" -> "int")

    vertexPropsKeys.map { case (key, keyType) =>
      Management.addVertexProp(testService, testColumn, key, keyType)
    }

//    println("[Redis init end]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  }


  test("test insert/check/get edges", RedisTest) {
    insertEdgesSync(
      toEdge(1, insert, e, sid1, tid1, testLabel),
      toEdge(1, insert, e, sid1, tid2, testLabel),
      toEdge(1, insert, e, sid1, tid3, testLabel),
      toEdge(1, insert, e, sid2, tid4, testLabel)
    )
    def queryCheckEdges(fromId: Int, toId: Int): JsValue = Json.parse(
      s"""
        |[{
        |  "label": "$testLabel",
        |  "direction": "out",
        |  "from": $fromId,
        |  "to": $toId
        |}]
       """.stripMargin
    )
    def query(id: Int, label: String, offset: Int = 0, limit: Int = 100) = Json.parse(
      s"""
      { "srcVertices": [
        { "serviceName": "$testService",
          "columnName": "$testColumn",
          "id": $id
         }],
        "steps": [
        [ {
            "label": "$label",
            "direction": "out",
            "offset": $offset,
            "limit": $limit
          }
        ]]
      }
    """)


    var result = checkEdgesSync(queryCheckEdges(sid1, tid1))
    println(result.toString())
    (result \ "size").toString should be("1") // edge 1000001 -> 1000 should be present

    result = checkEdgesSync(queryCheckEdges(sid2, tid4))
    println(result.toString())
    (result \ "size").toString should be("1") // edge 1000002 -> 2000 should be present

    result = getEdgesSync(query(sid1, testLabel, 0, 10))
    println(result.toString())
    (result \ "size").toString should be("3") // edge 1000001 -> 1000, 1100, 1110 should be present
  }

  test("get vertex", RedisTest) {
    val ids = Array(sid1, sid2)
    val q = vertexQueryJson(testService, testColumn, ids)

    val rs = getVerticesSync(q)
    rs.as[Array[JsValue]].size should be (2)
  }

//  test("insert vertex", RedisTest) {
//    val ids = (sid3 until sid6)
//    val data = vertexInsertsPayload(testServiceName, testColumnName, ids)
//    val payload = Json.parse(Json.toJson(data).toString())
//    println(Json.prettyPrint(payload))
//
//    val vertices = parser.toVertices(payload, "insert", Option(testServiceName), Option(testColumnName))
//    Await.result(graph.mutateVertices(vertices, true), HttpRequestWaitingTime)
//
//
//    val q = vertexQueryJson(testServiceName, testColumnName, ids)
//    println("vertex get query: " + q.toString())
//
//    val rs = getVerticesSync(q)
//    println("vertex get result: " + rs.toString())
//    rs.as[Array[JsValue]].size should be (3)
//  }
//
//  test("test increment", RedisTest) {
//    def queryTo(id: Int, to:Int, offset: Int = 0, limit: Int = 10) = Json.parse(
//      s"""
//      |{
//      |	"srcVertices": [{
//      |		"serviceName": "$testServiceName",
//      |		"columnName": "$testColumnName",
//      |		"id": $id
//      |	}],
//      |	"steps": [
//      |		[{
//      |			"label": "$testLabelNameV4",
//      |			"direction": "out",
//      |			"offset": $offset,
//      |			"limit": $limit,
//      |     "where": "_to=$to"
//      |		}]
//      |	]
//      |}
//      """.stripMargin
//    )
//
//    val incrementVal = 10
//    mutateEdgesSync(
//      toEdge(1, increment, e, sid3, sid4, testLabelNameV4, "{\"weight\":%s}".format(incrementVal), "out")
//    )
//
//    val resp = getEdgesSync(queryTo(sid3, sid4))
//    println(s"Result: ${Json.prettyPrint(resp)}")
//    (resp \ "size").toString should be ("1")  // edge 1000003 -> 1000004 should be present
//
//    val result = (resp \\ "results" ).head(0)
//    (result \ "props" \ "weight" ).toString should be (s"$incrementVal")
//  }
//
//  test("deleteAll", RedisTest) {
//    val deletedAt = 100
//    var result = getEdgesSync(querySingle(sid1, testLabelNameV4, 0, 10))
//
//    println(s"before deleteAll: ${Json.prettyPrint(result)}")
//
//    result = getEdgesSync(querySingle(sid1, testLabelNameV4, 0, 10))
//    println(result.toString())
//    (result \ "size").toString should be("3") // edge 1000001 -> 1000, 1100, 1110 should be present
//
//    val deleteParam = Json.arr(
//      Json.obj("label" -> testLabelNameV4,
//        "direction" -> "out",
//        "ids" -> Json.arr(sid1.toString),
//        "timestamp" -> deletedAt
//      )
//    )
//    deleteAllSync(deleteParam)
//
//    result = getEdgesSync(querySingle(sid1, testLabelNameV4, 0, 10))
//    println(result.toString())
//    (result \ "size").toString should be("0") // edge 1000001 -> 1000, 1100, 1110 should be deleted
//
//  }

}
