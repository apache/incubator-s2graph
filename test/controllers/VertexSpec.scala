package controllers

class VertexSpec extends SpecCommon {
//  init()

//  "vetex tc" should {
//    "tc1" in {
//      running(FakeApplication()) {
//        val ids = (0 until 3).toList
//        val (serviceName, columnName) = (testServiceName, testColumnName)
//
//        val data = vertexInsertsPayload(serviceName, columnName, ids)
//        val payload = Json.parse(Json.toJson(data).toString)
//
//        val req = FakeRequest(POST, s"/graphs/vertices/insert/$serviceName/$columnName").withBody(payload)
//        println(s">> $req, $payload")
//        val res = Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
//        println(res)
//        res.header.status must equalTo(200)
//        Thread.sleep(asyncFlushInterval)
//        println("---------------")
//
//        val query = vertexQueryJson(serviceName, columnName, ids)
//        val retFuture = route(FakeRequest(POST, "/graphs/getVertices").withJsonBody(query)).get
//
//        val ret = contentAsJson(retFuture)
//        println(">>>", ret)
//        val fetched = ret.as[Seq[JsValue]]
//        for {
//          (d, f) <- data.zip(fetched)
//        } yield {
//          (d \ "id") must beEqualTo((f \ "id"))
//          ((d \ "props") \ "age") must beEqualTo(((f \ "props") \ "age"))
//        }
//      }
//      true
//    }
//  }
}
