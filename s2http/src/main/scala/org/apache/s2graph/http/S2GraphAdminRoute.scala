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

object S2GraphAdminRoute extends PlayJsonSupport {
  def toHttpEntity[A: ToPlayJson](opt: Option[A], status: StatusCode = StatusCodes.OK, message: String = ""): HttpResponse = {
    val ev = implicitly[ToPlayJson[A]]
    val res = opt.map(ev.toJson).getOrElse(Json.obj("message" -> message))

    HttpResponse(
      status = status,
      entity = HttpEntity(ContentTypes.`application/json`, res.toString)
    )
  }

  def toHttpEntity[A: ToPlayJson](_try: Try[A]): HttpResponse = {
    val ev = implicitly[ToPlayJson[A]]
    val (status, res) = _try match {
      case Success(m) => StatusCodes.Created -> Json.obj("status" -> "ok", "message" -> ev.toJson(m))
      case Failure(e) => StatusCodes.OK -> Json.obj("status" -> "failure", "message" -> e.toString)
    }

    toHttpEntity(Option(res), status = status)
  }

  def toHttpEntity[A: ToPlayJson](ls: Seq[A], status: StatusCode): HttpResponse = {
    val ev = implicitly[ToPlayJson[A]]
    val res = JsArray(ls.map(ev.toJson))

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
  //  GET /admin/getService/:serviceName
  lazy val getService = path("getService" / Segment) { serviceName =>
    val serviceOpt = Management.findService(serviceName)

    complete(toHttpEntity(serviceOpt, message = s"Service not found: ${serviceName}"))
  }

  //  GET /admin/getServiceColumn/:serviceName/:columnName
  lazy val getServiceColumn = path("getServiceColumn" / Segment / Segment) { (serviceName, columnName) =>
    val ret = Management.findServiceColumn(serviceName, columnName)
    complete(toHttpEntity(ret, message = s"ServiceColumn not found: ${serviceName}, ${columnName}"))
  }

  //  GET /admin/getLabel/:labelName
  lazy val getLabel = path("getLabel" / Segment) { labelName =>
    val labelOpt = Management.findLabel(labelName)

    complete(toHttpEntity(labelOpt, message = s"Label not found: ${labelName}"))
  }

  //  GET /admin/getLabels/:serviceName
  lazy val getLabels = path("getLabels" / Segment) { serviceName =>
    val ret = Management.findLabels(serviceName)

    complete(toHttpEntity(ret, StatusCodes.OK))
  }

  /* POST */
  //  POST /admin/createService
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

  //  POST /admin/createServiceColumn
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

  //  POST /admin/createLabel
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

  //  POST /admin/addProp/:labelName
  lazy val addProp = path("addProp" / Segment) { labelName =>
    entity(as[JsValue]) { params =>
      val labelMetaTry = for {
        prop <- requestParser.toPropElements(params)
        labelMeta <- Management.addProp(labelName, prop)
      } yield labelMeta

      complete(toHttpEntity(labelMetaTry))
    }
  }

  //  POST /admin/addServiceColumnProp/:serviceName/:columnName
  lazy val addServiceColumnProp = path("addServiceColumnProp" / Segments) { params =>
    val (serviceName, columnName, storeInGlobalIndex) = params match {
      case s :: c :: Nil => (s, c, false)
      case s :: c :: i :: Nil => (s, c, i.toBoolean)
      case _ => throw new RuntimeException("Invalid Params")
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

  //  POST /admin/createHTable
  lazy val createHTable = path("createHTable") {
    entity(as[JsValue]) { params =>
      params.validate[HTableParams] match {
        case JsSuccess(hTableParams, _) => {
          management.createStorageTable(hTableParams.cluster, hTableParams.hTableName, List("e", "v"),
            hTableParams.preSplitSize, hTableParams.hTableTTL,
            hTableParams.compressionAlgorithm.getOrElse(Management.DefaultCompressionAlgorithm))

          complete(toHttpEntity(None: Option[JsValue], status = StatusCodes.OK, message = "created"))
        }
        case err@JsError(_) => complete(toHttpEntity(None: Option[JsValue], status = StatusCodes.BadRequest, message = Json.toJson(err).toString))
      }
    }
  }

  //  POST /admin/copyLabel/:oldLabelName/:newLabelName
  lazy val copyLabel = path("copyLabel" / Segment / Segment) { (oldLabelName, newLabelName) =>
    val copyTry = management.copyLabel(oldLabelName, newLabelName, Some(newLabelName))

    complete(toHttpEntity(copyTry))
  }

  //  POST /admin/renameLabel/:oldLabelName/:newLabelName
  lazy val renameLabel = path("renameLabel" / Segment / Segment) { (oldLabelName, newLabelName) =>
    Label.findByName(oldLabelName) match {
      case None => complete(toHttpEntity(None: Option[JsValue], status = StatusCodes.NotFound, message = s"Label $oldLabelName not found."))
      case Some(label) =>
        Management.updateLabelName(oldLabelName, newLabelName)

        complete(toHttpEntity(None: Option[JsValue], message = s"${label} was updated."))
    }
  }

  //  POST /admin/swapLabels/:leftLabelName/:rightLabelName
  lazy val swapLabel = path("swapLabel" / Segment / Segment) { (leftLabelName, rightLabelName) =>
    val left = Label.findByName(leftLabelName, useCache = false)
    val right = Label.findByName(rightLabelName, useCache = false)
    // verify same schema

    (left, right) match {
      case (Some(l), Some(r)) =>
        Management.swapLabelNames(leftLabelName, rightLabelName)
        complete(toHttpEntity(None: Option[JsValue], message = s"Labels were swapped."))
      case _ =>
        complete(toHttpEntity(None: Option[JsValue], status = StatusCodes.NotFound, message = s"Label ${leftLabelName} or ${rightLabelName} not found."))
    }
  }

  //  POST /admin/updateHTable/:labelName/:newHTableName
  lazy val updateHTable = path("updateHTable" / Segment / Segment) { (labelName, newHTableName) =>
    val updateTry = Management.updateHTable(labelName, newHTableName).map(Json.toJson(_))

    complete(toHttpEntity(updateTry))
  }

  /* PUT */
  //  PUT /admin/deleteLabelReally/:labelName
  lazy val deleteLabelReally = path("deleteLabelReally" / Segment) { labelName =>
    val ret = Management.deleteLabel(labelName).toOption

    complete(toHttpEntity(ret, message = s"Label not found: ${labelName}"))
  }

  //  PUT /admin/markDeletedLabel/:labelName
  lazy val markDeletedLabel = path("markDeletedLabel" / Segment) { labelName =>
    val ret = Management.markDeletedLabel(labelName).toOption.map(Json.toJson(_))

    complete(toHttpEntity(ret, message = s"Label not found: ${labelName}"))
  }

  //  PUT /admin/deleteServiceColumn/:serviceName/:columnName
  lazy val deleteServiceColumn = path("deleteServiceColumn" / Segment / Segment) { (serviceName, columnName) =>
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
