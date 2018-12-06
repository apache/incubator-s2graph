package org.apache.s2graph.core.Integrate

import org.apache.s2graph.core.schema._
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ExampleTest extends IntegrateCommon {

  import TestUtil._

  private def prepareTestData() = {
    // create service KakaoFavorites
    val createServicePayload = Json.parse(
      """
        |{"serviceName": "KakaoFavorites", "compressionAlgorithm" : "gz"}
        |""".stripMargin)

    val createFriendsPayload = Json.parse(
      s"""{
         |  "label": "friends",
         |  "srcServiceName": "KakaoFavorites",
         |  "srcColumnName": "userName",
         |  "srcColumnType": "string",
         |  "tgtServiceName": "KakaoFavorites",
         |  "tgtColumnName": "userName",
         |  "tgtColumnType": "string",
         |  "isDirected": "false",
         |  "indices": [],
         |  "props": [],
         |  "consistencyLevel": "strong"
         |}""".stripMargin)

    val createPostPayload = Json.parse(
      """{
        |  "label": "post",
        |  "srcServiceName": "KakaoFavorites",
        |  "srcColumnName": "userName",
        |  "srcColumnType": "string",
        |  "tgtServiceName": "KakaoFavorites",
        |  "tgtColumnName": "url",
        |  "tgtColumnType": "string",
        |  "isDirected": "true",
        |  "indices": [],
        |  "props": [],
        |  "consistencyLevel": "strong"
        |}""".stripMargin)

    val insertFriendsPayload = Json.parse(
      """[
        |  {"from":"Elmo","to":"Big Bird","label":"friends","props":{},"timestamp":1444360152477},
        |  {"from":"Elmo","to":"Ernie","label":"friends","props":{},"timestamp":1444360152478},
        |  {"from":"Elmo","to":"Bert","label":"friends","props":{},"timestamp":1444360152479},
        |
        |  {"from":"Cookie Monster","to":"Grover","label":"friends","props":{},"timestamp":1444360152480},
        |  {"from":"Cookie Monster","to":"Kermit","label":"friends","props":{},"timestamp":1444360152481},
        |  {"from":"Cookie Monster","to":"Oscar","label":"friends","props":{},"timestamp":1444360152482}
        |]""".stripMargin)

    val insertPostPayload = Json.parse(
      """[
        |  {"from":"Big Bird","to":"www.kakaocorp.com/en/main","label":"post","props":{},"timestamp":1444360152477},
        |  {"from":"Big Bird","to":"github.com/kakao/s2graph","label":"post","props":{},"timestamp":1444360152478},
        |  {"from":"Ernie","to":"groups.google.com/forum/#!forum/s2graph","label":"post","props":{},"timestamp":1444360152479},
        |  {"from":"Grover","to":"hbase.apache.org/forum/#!forum/s2graph","label":"post","props":{},"timestamp":1444360152480},
        |  {"from":"Kermit","to":"www.playframework.com","label":"post","props":{},"timestamp":1444360152481},
        |  {"from":"Oscar","to":"www.scala-lang.org","label":"post","props":{},"timestamp":1444360152482}
        |]""".stripMargin)


    val (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) = parser.toServiceElements(createServicePayload)
    management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)

    Service.findByName("KakaoFavorites", useCache = false)

    Label.findByName("friends", useCache = false).foreach { label =>
      Label.delete(label.id.get)
    }
    parser.toLabelElements(createFriendsPayload)

    Label.findByName("post", useCache = false).foreach { label =>
      Label.delete(label.id.get)
    }

    parser.toLabelElements(createPostPayload)
    Await.result(graph.mutateEdges(parser.parseJsonFormat(insertFriendsPayload, operation = "insert").map(_._1), withWait = true), Duration("10 seconds"))
    Await.result(graph.mutateEdges(parser.parseJsonFormat(insertPostPayload, operation = "insert").map(_._1), withWait = true), Duration("10 seconds"))
  }

  override def initTestData(): Unit = {
    prepareTestData()
  }

  test("[S2GRAPH-243]: Limit bug on 'graph/getEdges' offset 0, limit 3") {
    val queryJson = Json.parse(
      """{
        |    "select": ["to"],
        |    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Elmo"}],
        |    "steps": [
        |      {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 3}]}
        |    ]
        |}""".stripMargin)

    val response = getEdgesSync(queryJson)

    val expectedFriends = Seq("Bert", "Ernie", "Big Bird")
    val friends = (response \ "results").as[Seq[JsObject]].map(obj => (obj \ "to").as[String])

    friends shouldBe expectedFriends
  }

  test("[S2GRAPH-243]: Limit bug on 'graph/getEdges' offset 1, limit 2") {
    val queryJson = Json.parse(
      """{
        |    "select": ["to"],
        |    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Elmo"}],
        |    "steps": [
        |      {"step": [{"label": "friends", "direction": "out", "offset": 1, "limit": 2}]}
        |    ]
        |}""".stripMargin)

    val response = getEdgesSync(queryJson)

    val expectedFriends = Seq("Ernie", "Big Bird")
    val friends = (response \ "results").as[Seq[JsObject]].map(obj => (obj \ "to").as[String])

    friends shouldBe expectedFriends
  }
}
