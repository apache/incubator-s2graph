package controllers

import play.api.libs.json._
import play.api.test.FakeApplication
import play.api.test.Helpers._

class VertexSpec extends SpecCommon {
  //  init()

  "vetex tc" should {
    "tc1" in {

      running(FakeApplication()) {
        val ids = (7 until 20).map(tcNum => tcNum * 1000 + 0)

        val (serviceName, columnName) = (testServiceName, testColumnName)

        val data = vertexInsertsPayload(serviceName, columnName, ids)
        val payload = Json.parse(Json.toJson(data).toString)
        println(payload)

        val jsResult = contentAsString(VertexController.tryMutates(payload, "insert",
          Option(serviceName), Option(columnName), withWait = true))
        Thread.sleep(asyncFlushInterval)

        val query = vertexQueryJson(serviceName, columnName, ids)
        val ret = contentAsJson(QueryController.getVerticesInner(query))
        println(">>>", ret)
        val fetched = ret.as[Seq[JsValue]]
        for {
          (d, f) <- data.zip(fetched)
        } yield {
          (d \ "id") must beEqualTo((f \ "id"))
          ((d \ "props") \ "age") must beEqualTo(((f \ "props") \ "age"))
        }
      }
      true
    }
  }
}
