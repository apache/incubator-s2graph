package controllers

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.logger
import play.api.libs.json._
import play.api.mvc
import play.api.mvc.{Action, Controller}

import scala.util.{Failure, Success, Try}

object AdminController extends Controller with RequestParser {
  import ApplicationController._

  /**
   * admin message formatter
   * @tparam T
   */
  trait AdminMessageFormatter[T] {
    def toJson(msg: T): JsValue
  }

  object AdminMessageFormatter {
    implicit def jsValueToJson[T <: JsValue] = new AdminMessageFormatter[T] {
      def toJson(js: T) = js
    }

    implicit val stringToJson = new AdminMessageFormatter[String] {
      def toJson(js: String) = Json.obj("message" -> js)
    }
  }

  def format[T: AdminMessageFormatter](f: JsValue => play.mvc.Result)(message: T) = {
    val formatter = implicitly[AdminMessageFormatter[T]]
    f(formatter.toJson(message))
  }

  /**
   * ok response
   * @param message
   * @tparam T
   * @return
   */
  def ok[T: AdminMessageFormatter](message: T) = {
    val formatter = implicitly[AdminMessageFormatter[T]]
    Ok(formatter.toJson(message)).as(applicationJsonHeader)
  }

  /**
   * bad request response
   * @param message
   * @tparam T
   * @return
   */
  def bad[T: AdminMessageFormatter](message: T) = {
    val formatter = implicitly[AdminMessageFormatter[T]]
    BadRequest(formatter.toJson(message)).as(applicationJsonHeader)
  }

  /**
   * not found response
   * @param message
   * @tparam T
   * @return
   */
  def notFound[T: AdminMessageFormatter](message: T) = {
    val formatter = implicitly[AdminMessageFormatter[T]]
    NotFound(formatter.toJson(message)).as(applicationJsonHeader)
  }

  private[AdminController] def tryResponse[T, R: AdminMessageFormatter](res: Try[T])(callback: T => R): mvc.Result = res match {
    case Success(m) => ok(callback(m))
    case Failure(error) =>
      logger.error(error.getMessage, error)
      error match {
        case JsResultException(e) => bad(JsError.toFlatJson(e))
        case _ => bad(error.getMessage)
      }
  }

  def optionResponse[T, R: AdminMessageFormatter](res: Option[T])(callback: T => R): mvc.Result = res match {
    case Some(m) => ok(callback(m))
    case None => notFound("not found")
  }

  /**
   * load all model cache
   * @return
   */
  def loadCache() = Action { request =>
    val startTs = System.currentTimeMillis()

    if (!ApplicationController.isHealthy) {
      loadCacheInner()
    }

    ok(s"${System.currentTimeMillis() - startTs}")
  }

  def loadCacheInner() = {
    Service.findAll()
    ServiceColumn.findAll()
    Label.findAll()
    LabelMeta.findAll()
    LabelIndex.findAll()
    ColumnMeta.findAll()
  }

  /**
   * read
   */

  /**
   * get service info
   * @param serviceName
   * @return
   */
  def getService(serviceName: String) = Action { request =>
    val serviceOpt = Management.findService(serviceName)
    optionResponse(serviceOpt)(_.toJson)
  }

  /**
   * get label info
   * @param labelName
   * @return
   */
  def getLabel(labelName: String) = Action { request =>
    val labelOpt = Management.findLabel(labelName)
    optionResponse(labelOpt)(_.toJson)
  }

  /**
   * get all labels of service
   * @param serviceName
   * @return
   */
  def getLabels(serviceName: String) = Action { request =>
    Service.findByName(serviceName) match {
      case None => notFound(s"Service $serviceName not found")
      case Some(service) =>
        val src = Label.findBySrcServiceId(service.id.get)
        val tgt = Label.findByTgtServiceId(service.id.get)

        ok(Json.obj("from" -> src.map(_.toJson), "to" -> tgt.map(_.toJson)))
    }
  }

  /**
   * get service columns
   * @param serviceName
   * @param columnName
   * @return
   */
  def getServiceColumn(serviceName: String, columnName: String) = Action { request =>
    val serviceColumnOpt = for {
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName, useCache = false)
    } yield serviceColumn

    optionResponse(serviceColumnOpt)(_.toJson)
  }

  /**
   * create
   */

  /**
   * create service
   * @return
   */
  def createService() = Action(parse.json) { request =>
    val serviceTry = createServiceInner(request.body)
    tryResponse(serviceTry)(_.toJson)
  }

  def createServiceInner(jsValue: JsValue) = {
    val (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) = toServiceElements(jsValue)
    Management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
  }

  /**
   * create label
   * @return
   */
  def createLabel() = Action(parse.json) { request =>
    val ret = createLabelInner(request.body)
    tryResponse(ret)(_.toJson)
  }

  def createLabelInner(json: JsValue) = for {
    labelArgs <- toLabelElements(json)
    label <- (Management.createLabel _).tupled(labelArgs)
  } yield label

  /**
   * add index
   * @return
   */
  def addIndex() = Action(parse.json) { request =>
    val ret = addIndexInner(request.body)
    tryResponse(ret)(_.label + " is updated")
  }

  def addIndexInner(json: JsValue) = for {
    (labelName, indices) <- toIndexElements(json)
    label <- Management.addIndex(labelName, indices)
  } yield label

  /**
   * create service column
   * @return
   */
  def createServiceColumn() = Action(parse.json) { request =>
    val serviceColumnTry = createServiceColumnInner(request.body)
    tryResponse(serviceColumnTry) { (columns: Seq[ColumnMeta]) => Json.obj("metas" -> columns.map(_.toJson)) }
  }

  def createServiceColumnInner(jsValue: JsValue) = for {
    (serviceName, columnName, columnType, props) <- toServiceColumnElements(jsValue)
    serviceColumn <- Management.createServiceColumn(serviceName, columnName, columnType, props)
  } yield serviceColumn

  /**
   * delete
   */

  /**
   * delete label
   * @param labelName
   * @return
   */
  def deleteLabel(labelName: String) = Action { request =>
    val deleteLabelTry = deleteLabelInner(labelName)
    tryResponse(deleteLabelTry)(labelName => labelName + " is deleted")
  }

  def deleteLabelInner(labelName: String) = Management.deleteLabel(labelName)

  /**
   * delete servieColumn
   * @param serviceName
   * @param columnName
   * @return
   */
  def deleteServiceColumn(serviceName: String, columnName: String) = Action { request =>
    val serviceColumnTry = deleteServiceColumnInner(serviceName, columnName)
    tryResponse(serviceColumnTry)(columnName => columnName + " is deleted")
  }

  def deleteServiceColumnInner(serviceName: String, columnName: String) =
    Management.deleteColumn(serviceName, columnName)

  /**
   * update
   */

  /**
   * add Prop to label
   * @param labelName
   * @return
   */
  def addProp(labelName: String) = Action(parse.json) { request =>
    val labelMetaTry = addPropInner(labelName, request.body)
    tryResponse(labelMetaTry)(_.toJson)
  }

  def addPropInner(labelName: String, js: JsValue) = for {
    prop <- toPropElements(js)
    labelMeta <- Management.addProp(labelName, prop)
  } yield labelMeta

  /**
   * add prop to serviceColumn
   * @param serviceName
   * @param columnName
   * @return
   */
  def addServiceColumnProp(serviceName: String, columnName: String) = Action(parse.json) { request =>
    addServiceColumnPropInner(serviceName, columnName)(request.body) match {
      case None => bad(s"can`t find service with $serviceName or can`t find serviceColumn with $columnName")
      case Some(m) => Ok(m.toJson).as(applicationJsonHeader)
    }
  }

  def addServiceColumnPropInner(serviceName: String, columnName: String)(js: JsValue) = {
    for {
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
      prop <- toPropElements(js).toOption
    } yield {
      ColumnMeta.findOrInsert(serviceColumn.id.get, prop.name, prop.defaultValue)
    }
  }

  /**
   * add props to serviceColumn
   * @param serviecName
   * @param columnName
   * @return
   */
  def addServiceColumnProps(serviecName: String, columnName: String) = Action(parse.json) { request =>
    val jsObjs = request.body.asOpt[List[JsObject]].getOrElse(List.empty[JsObject])
    val newProps = for {
      js <- jsObjs
      newProp <- addServiceColumnPropInner(serviecName, columnName)(js)
    } yield newProp
    ok(s"${newProps.size} is added.")
  }

  /**
   * copy label
   * @param oldLabelName
   * @param newLabelName
   * @return
   */
  def copyLabel(oldLabelName: String, newLabelName: String) = Action { request =>
    val copyTry = Management.copyLabel(oldLabelName, newLabelName, Some(newLabelName))
    tryResponse(copyTry)(_.label + "created")
  }

  /**
   * rename label
   * @param oldLabelName
   * @param newLabelName
   * @return
   */
  def renameLabel(oldLabelName: String, newLabelName: String) = Action { request =>
    Label.findByName(oldLabelName) match {
      case None => NotFound.as(applicationJsonHeader)
      case Some(label) =>
        Management.updateLabelName(oldLabelName, newLabelName)
        ok(s"Label was updated")
    }
  }

  /**
   * update HTable for a label
   * @param labelName
   * @param newHTableName
   * @return
   */
  def updateHTable(labelName: String, newHTableName: String) = Action { request =>
    val updateTry = Management.updateHTable(labelName, newHTableName)
    tryResponse(updateTry)(_.toString + " label(s) updated.")
  }
}
