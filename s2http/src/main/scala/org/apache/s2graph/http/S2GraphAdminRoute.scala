package org.apache.s2graph.http

import akka.http.scaladsl.model._
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.{Management, S2Graph}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import org.apache.s2graph.core.Management.JsonModel.HTableParams
import org.apache.s2graph.core.schema._
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.util._

object S2GraphAdminRoute {

  trait AdminMessageFormatter[T] {
    def toJson(msg: T): JsValue
  }

  import scala.language.reflectiveCalls

  object AdminMessageFormatter {
    type ToPlayJson = {
      def toJson: JsValue
    }

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

  def toHttpEntity[A: AdminMessageFormatter](ls: Seq[A], status: StatusCode = StatusCodes.OK): HttpResponse = {
    val ev = implicitly[AdminMessageFormatter[A]]
    val res = ls.map(ev.toJson)

    HttpResponse(
      status = status,
      entity = HttpEntity(ContentTypes.`application/json`, res.toString)
    )
  }
}

trait S2GraphAdminRoute extends PlayJsonSupport {

  import S2GraphAdminRoute._

  val s2graph: S2Graph
  val logger = LoggerFactory.getLogger(this.getClass)

  lazy val management: Management = s2graph.management
  lazy val requestParser: RequestParser = new RequestParser(s2graph)

  // routes impl
  /* GET */
  //  GET /graphs/getService/:serviceName
  lazy val getService = path("getService" / Segment) { serviceName =>
    val serviceOpt = Management.findService(serviceName)

    complete(toHttpEntity(serviceOpt, message = s"Service not found: ${serviceName}"))
  }

  //  GET /graphs/getServiceColumn/:serviceName/:columnName
  lazy val getServiceColumn = path("getServiceColumn" / Segments) { params =>
    val (serviceName, columnName) = params match {
      case s :: c :: Nil => (s, c)
    }

    val ret = Management.findServiceColumn(serviceName, columnName)
    complete(toHttpEntity(ret, message = s"ServiceColumn not found: ${serviceName}, ${columnName}"))
  }

  //  GET /graphs/getLabel/:labelName
  lazy val getLabel = path("getLabel" / Segment) { labelName =>
    val labelOpt = Management.findLabel(labelName)

    complete(toHttpEntity(labelOpt, message = s"Label not found: ${labelName}"))
  }

  //  GET /graphs/getLabels/:serviceName
  lazy val getLabels = path("getLabels" / Segment) { serviceName =>
    val ret = Management.findLabels(serviceName)

    complete(toHttpEntity(ret))
  }

  /* POST */
  //  POST /graphs/createService
  lazy val createService = path("createService") {
    entity(as[JsValue]) { params =>

      val parseTry = Try(requestParser.toServiceElements(params))
      val serviceTry = for {
        (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) <- parseTry
        service <- management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
      } yield service

      complete(toHttpEntity(serviceTry))
    }
  }

  //  POST /graphs/createServiceColumn
  lazy val createServiceColumn = path("createServiceColumn") {
    entity(as[JsValue]) { params =>

      val parseTry = requestParser.toServiceColumnElements(params)
      val serviceColumnTry = for {
        (serviceName, columnName, columnType, props) <- parseTry
        serviceColumn <- Try(management.createServiceColumn(serviceName, columnName, columnType, props))
      } yield serviceColumn

      complete(toHttpEntity(serviceColumnTry))
    }
  }

  //  POST /graphs/createLabel
  lazy val createLabel = path("createLabel") {
    entity(as[JsValue]) { params =>
      val labelTry = requestParser.toLabelElements(params)

      complete(toHttpEntity(labelTry))
    }
  }

  //  POST /graphs/addIndex
  lazy val addIndex = path("addIndex") {
    entity(as[JsValue]) { params =>
      val labelTry = for {
        (labelName, indices) <- requestParser.toIndexElements(params)
        label <- Management.addIndex(labelName, indices)
      } yield label

      complete(toHttpEntity(labelTry))
    }
  }

  //  POST /graphs/addProp/:labelName
  lazy val addProp = path("addProp" / Segment) { labelName =>
    entity(as[JsValue]) { params =>
      val labelMetaTry = for {
        prop <- requestParser.toPropElements(params)
        labelMeta <- Management.addProp(labelName, prop)
      } yield labelMeta

      complete(toHttpEntity(labelMetaTry))
    }
  }

  //  POST /graphs/addServiceColumnProp/:serviceName/:columnName
  lazy val addServiceColumnProp = path("addServiceColumnProp" / Segments) { params =>
    val (serviceName, columnName, storeInGlobalIndex) = params match {
      case s :: c :: Nil => (s, c, false)
      case s :: c :: i :: Nil => (s, c, i.toBoolean)
    }

    entity(as[JsValue]) { params =>
      val columnMetaOpt = for {
        service <- Service.findByName(serviceName)
        serviceColumn <- ServiceColumn.find(service.id.get, columnName)
        prop <- requestParser.toPropElements(params).toOption
      } yield {
        ColumnMeta.findOrInsert(serviceColumn.id.get, prop.name, prop.dataType, prop.defaultValue, storeInGlobalIndex)
      }

      complete(toHttpEntity(columnMetaOpt, message = s"can`t find service with $serviceName or can`t find serviceColumn with $columnName"))
    }
  }

  //  POST /graphs/createHTable
  lazy val createHTable = path("createHTable") {
    entity(as[JsValue]) { params =>
      params.validate[HTableParams] match {
        case JsSuccess(hTableParams, _) => {
          management.createStorageTable(hTableParams.cluster, hTableParams.hTableName, List("e", "v"),
            hTableParams.preSplitSize, hTableParams.hTableTTL,
            hTableParams.compressionAlgorithm.getOrElse(Management.DefaultCompressionAlgorithm))

          complete(toHttpEntity(None, status = StatusCodes.OK, message = "created"))
        }
        case err@JsError(_) => complete(toHttpEntity(None, status = StatusCodes.BadRequest, message = Json.toJson(err).toString))
      }
    }
  }

  //  POST /graphs/copyLabel/:oldLabelName/:newLabelName
  lazy val copyLabel = path("copyLabel" / Segments) { params =>
    val (oldLabelName, newLabelName) = params match {
      case oldLabel :: newLabel :: Nil => (oldLabel, newLabel)
    }

    val copyTry = management.copyLabel(oldLabelName, newLabelName, Some(newLabelName))

    complete(toHttpEntity(copyTry))
  }

  //  POST /graphs/renameLabel/:oldLabelName/:newLabelName
  lazy val renameLabel = path("renameLabel" / Segments) { params =>
    val (oldLabelName, newLabelName) = params match {
      case oldLabel :: newLabel :: Nil => (oldLabel, newLabel)
    }

    Label.findByName(oldLabelName) match {
      case None => complete(toHttpEntity(None, status = StatusCodes.NotFound, message = s"Label $oldLabelName not found."))
      case Some(label) =>
        Management.updateLabelName(oldLabelName, newLabelName)

        complete(toHttpEntity(None, message = s"${label} was updated."))
    }
  }

  //  POST /graphs/swapLabels/:leftLabelName/:rightLabelName
  lazy val swapLabel = path("swapLabel" / Segments) { params =>
    val (leftLabelName, rightLabelName) = params match {
      case left :: right :: Nil => (left, right)
    }

    val left = Label.findByName(leftLabelName, useCache = false)
    val right = Label.findByName(rightLabelName, useCache = false)
    // verify same schema

    (left, right) match {
      case (Some(l), Some(r)) =>
        Management.swapLabelNames(leftLabelName, rightLabelName)

        complete(toHttpEntity(None, message = s"Labels were swapped."))
      case _ =>
        complete(toHttpEntity(None, status = StatusCodes.NotFound, message = s"Label ${leftLabelName} or ${rightLabelName} not found."))
    }
  }

  //  POST /graphs/updateHTable/:labelName/:newHTableName
  lazy val updateHTable = path("updateHTable" / Segments) { params =>
    val (labelName, newHTableName) = params match {
      case l :: h :: Nil => (l, h)
    }

    val updateTry = Management.updateHTable(labelName, newHTableName)

    complete(toHttpEntity(updateTry))
  }

  /* PUT */
  //  PUT /graphs/deleteLabelReally/:labelName
  lazy val deleteLabelReally = path("deleteLabelReally" / Segment) { labelName =>
    val ret = Management.deleteLabel(labelName).toOption

    complete(toHttpEntity(ret, message = s"Label not found: ${labelName}"))
  }

  //  PUT /graphs/markDeletedLabel/:labelName
  lazy val markDeletedLabel = path("markDeletedLabel" / Segment) { labelName =>
    val ret = Management.markDeletedLabel(labelName).toOption

    complete(toHttpEntity(ret, message = s"Label not found: ${labelName}"))
  }

  //  PUT /graphs/deleteServiceColumn/:serviceName/:columnName
  lazy val deleteServiceColumn = path("deleteServiceColumn" / Segments) { params =>
    val (serviceName, columnName) = params match {
      case s :: c :: Nil => (s, c)
    }

    val ret = Management.deleteColumn(serviceName, columnName).toOption

    complete(toHttpEntity(ret, message = s"ServiceColumn not found: ${serviceName}, ${columnName}"))
  }

  //  TODO:
  // delete service?
  //  PUT /graphs/loadCache

  // expose routes
  lazy val adminRoute: Route =
    get {
      concat(
        getService,
        getServiceColumn,
        getLabel,
        getLabels
      )
    } ~ post {
      concat(
        createService,
        createServiceColumn,
        createLabel,
        addIndex,
        addProp,
        addServiceColumnProp,
        createHTable,
        copyLabel,
        renameLabel,
        swapLabel,
        updateHTable
      )
    } ~ put {
      concat(
        deleteLabelReally,
        markDeletedLabel,
        deleteServiceColumn
      )
    }
}
