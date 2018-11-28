package org.apache.s2graph.http

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.S2Graph
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsString, JsValue, Json}

class AdminRoutesSpec extends WordSpec with Matchers with ScalaFutures with ScalatestRouteTest with S2GraphAdminRoute with BeforeAndAfterAll with PlayJsonSupport {

  val config = ConfigFactory.load()
  val s2graph = new S2Graph(config)

  override val logger = LoggerFactory.getLogger(this.getClass)

  override def afterAll(): Unit = {
    s2graph.shutdown()
  }

  lazy val routes = adminRoute

  "AdminRoute" should {
    "be able to create service (POST /createService)" in {
      val serviceParam = Json.obj(
        "serviceName" -> "kakaoFavorites",
        "compressionAlgorithm" -> "gz"
      )

      val serviceEntity = Marshal(serviceParam).to[MessageEntity].futureValue
      val request = Post("/createService").withEntity(serviceEntity)

      request ~> routes ~> check {
        status should ===(StatusCodes.Created)
        contentType should ===(ContentTypes.`application/json`)

        val response = entityAs[JsValue]

        (response \\ "name").head should ===(JsString("kakaoFavorites"))
        (response \\ "status").head should ===(JsString("ok"))
      }
    }

    "return service if present (GET /getService/{serviceName})" in {
      val request = HttpRequest(uri = "/getService/kakaoFavorites")

      request ~> routes ~> check {
        status should ===(StatusCodes.OK)
        contentType should ===(ContentTypes.`application/json`)

        val response = entityAs[JsValue]

        (response \\ "name").head should ===(JsString("kakaoFavorites"))
      }
    }
  }
}

