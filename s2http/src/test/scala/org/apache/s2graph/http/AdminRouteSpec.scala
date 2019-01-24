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
import play.api.libs.json.{JsString, JsValue, Json}

class AdminRoutesSpec extends WordSpec with Matchers with ScalaFutures with ScalatestRouteTest with S2GraphAdminRoute with BeforeAndAfterAll {
  import scala.collection.JavaConverters._

  val dbUrl = "jdbc:h2:file:./var/metastore_admin_route;MODE=MYSQL;AUTO_SERVER=true"
  val config =
    ConfigFactory.parseMap(Map("db.default.url" -> dbUrl).asJava)
  lazy val s2graph = new S2Graph(config.withFallback(ConfigFactory.load()))
  override val logger = LoggerFactory.getLogger(this.getClass)

  override def afterAll(): Unit = {
    s2graph.shutdown(true)
  }

  lazy val routes = adminRoute

  val serviceName = "kakaoFavorites"
  val columnName = "userName"

  "AdminRoute" should {
    "be able to create service (POST /createService)" in {
      val serviceParam = Json.obj(
        "serviceName" -> serviceName,
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
      val request = HttpRequest(uri = s"/getService/$serviceName")

      request ~> routes ~> check {
        status should ===(StatusCodes.OK)
        contentType should ===(ContentTypes.`application/json`)

        val response = entityAs[JsValue]

        (response \\ "name").head should ===(JsString("kakaoFavorites"))
      }
    }

    "be able to create serviceColumn (POST /createServiceColumn)" in {
      val serviceColumnParam = Json.obj(
        "serviceName" -> serviceName,
        "columnName" -> columnName,
        "columnType" -> "string",
        "props" -> Json.toJson(
          Seq(
            Json.obj("name" -> "age", "defaultValue" -> "-1", "dataType" -> "integer")
          )
        )
      )

      val serviceColumnEntity = Marshal(serviceColumnParam).to[MessageEntity].futureValue
      val request = Post("/createServiceColumn").withEntity(serviceColumnEntity)

      request ~> routes ~> check {
        status should ===(StatusCodes.Created)
        contentType should ===(ContentTypes.`application/json`)

        val response = entityAs[JsValue]

        (response \\ "serviceName").head should ===(JsString("kakaoFavorites"))
        (response \\ "columnName").head should ===(JsString("userName"))
        (response \\ "status").head should ===(JsString("ok"))
      }
    }
  }
}

