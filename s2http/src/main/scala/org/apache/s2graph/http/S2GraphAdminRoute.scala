package org.apache.s2graph.http

import akka.http.scaladsl.model._
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.{Management, S2Graph}

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._

import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.util.Try

object S2GraphAdminRoute {

  import scala.util._

  trait AdminMessageFormatter[T] {
    def toJson(msg: T): JsValue
  }

  import scala.language.reflectiveCalls

  type ToPlayJson = {def toJson: JsValue}

  object AdminMessageFormatter {

    implicit def toPlayJson[A <: ToPlayJson] = new AdminMessageFormatter[A] {
      def toJson(js: A) = js.toJson
    }

    implicit def fromPlayJson[T <: JsValue] = new AdminMessageFormatter[T] {
      def toJson(js: T) = js
    }
  }

  def toHttpEntity[A: AdminMessageFormatter](opt: Option[A], status: StatusCode = StatusCodes.OK, message: String = ""): HttpResponse = {
    val ev = implicitly[AdminMessageFormatter[A]]
    val res = opt.map(ev.toJson).getOrElse(Json.obj("message" -> message))

    HttpResponse(
      status = status,
      entity = HttpEntity(ContentTypes.`application/json`, res.toString)
    )
  }

  def toHttpEntity[A: AdminMessageFormatter](opt: Try[A]): HttpResponse = {
    val ev = implicitly[AdminMessageFormatter[A]]
    val (status, res) = opt match {
      case Success(m) => StatusCodes.Created -> Json.obj("status" -> "ok", "message" -> ev.toJson(m))
      case Failure(e) => StatusCodes.OK -> Json.obj("status" -> "failure", "message" -> e.toString)
    }

    toHttpEntity(Option(res), status = status)
  }
}

trait S2GraphAdminRoute {

  import S2GraphAdminRoute._

  val s2graph: S2Graph
  val logger = LoggerFactory.getLogger(this.getClass)

  lazy val management: Management = s2graph.management
  lazy val requestParser: RequestParser = new RequestParser(s2graph)

  // routes impl
  lazy val getLabel = path("getLabel" / Segment) { labelName =>
    val labelOpt = Management.findLabel(labelName)

    complete(toHttpEntity(labelOpt, message = s"Label not found: ${labelName}"))
  }

  lazy val getService = path("getService" / Segment) { serviceName =>
    val serviceOpt = Management.findService(serviceName)

    complete(toHttpEntity(serviceOpt, message = s"Service not found: ${serviceName}"))
  }

  lazy val createService = path("createService") {
    entity(as[String]) { body =>
      val params = Json.parse(body)

      val parseTry = Try(requestParser.toServiceElements(params))
      val serviceTry = for {
        (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) <- parseTry
        service <- management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
      } yield service

      complete(toHttpEntity(serviceTry))
    }
  }

  lazy val createLabel = path("createLabel") {
    entity(as[String]) { body =>
      val params = Json.parse(body)

      val labelTry = for {
        label <- requestParser.toLabelElements(params)
      } yield label

      complete(toHttpEntity(labelTry))
    }
  }

  // expose routes
  lazy val adminRoute: Route =
    get {
      concat(
        getService,
        getLabel
      )
    } ~ post {
      concat(
        createService,
        createLabel
      )
    }
}
