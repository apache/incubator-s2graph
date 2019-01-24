package org.apache.s2graph.http

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core.S2Graph
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsValue, Json}

class MutateRouteSpec extends WordSpec with Matchers with PlayJsonSupport with ScalaFutures with ScalatestRouteTest with S2GraphMutateRoute with BeforeAndAfterAll {

  import scala.collection.JavaConverters._

  val dbUrl = "jdbc:h2:file:./var/metastore_mutate_route;MODE=MYSQL;AUTO_SERVER=true"
  val config =
    ConfigFactory.parseMap(Map("db.default.url" -> dbUrl).asJava)
  lazy val s2graph = new S2Graph(config.withFallback(ConfigFactory.load()))
  override val logger = LoggerFactory.getLogger(this.getClass)

  override def afterAll(): Unit = {
    s2graph.shutdown(true)
  }

  lazy val routes = mutateRoute

  val serviceName = "kakaoFavorites"
  val columnName = "userName"

  "MutateRoute" should {

    "be able to insert vertex (POST /mutate/vertex/insert)" in {
      s2graph.management.createService(serviceName, "localhost", s"${serviceName}-dev", 1, None)
      s2graph.management.createServiceColumn(serviceName, columnName, "string", Seq(Prop("age", "0", "integer")))

      // {"timestamp": 10, "serviceName": "s2graph", "columnName": "user", "id": 1, "props": {}}
      val param = Json.obj(
        "timestamp" -> 10,
        "serviceName" -> serviceName,
        "columnName" -> columnName,
        "id" -> "user_a",
        "props" -> Json.obj(
          "age" -> 20
        )
      )

      val entity = Marshal(param).to[MessageEntity].futureValue
      val request = Post("/vertex/insert").withEntity(entity)

      request ~> routes ~> check {
        status should ===(StatusCodes.OK)
        contentType should ===(ContentTypes.`application/json`)

        val response = entityAs[JsValue]
        response should ===(Json.toJson(Seq(true)))
      }
    }
  }
}

