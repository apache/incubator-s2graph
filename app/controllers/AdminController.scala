package controllers

import com.daumkakao.s2graph.core.Management.JsonModel.Prop
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.mysqls._
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.mvc.{Action, Controller}
import play.api.{Logger, mvc}

import scala.util.{Failure, Success, Try}

object AdminController extends Controller with RequestParser {
  private def ok(msg: String) = Ok(Json.obj("message" -> msg)).as(QueryController.applicationJsonHeader)

  private def bad(msg: String) = BadRequest(Json.obj("message" -> msg)).as(QueryController.applicationJsonHeader)

  private def tryResponse[T](res: Try[T])(callback: T => String): mvc.Result = {
    res match {
      case Success(label) => ok(callback(label.asInstanceOf[T]))
      case Failure(error) =>
        Logger.error(error.getMessage, error)
        bad(error.getMessage)
    }
  }

  def loadCache() = Action { request =>
    val startTs = System.currentTimeMillis()

    if (!ApplicationController.isHealthy) {
      Service.findAll()
      ServiceColumn.findAll()
      Label.findAll()
      LabelMeta.findAll()
      LabelIndex.findAll()
      ColumnMeta.findAll()
    }

    ok(s"${System.currentTimeMillis() - startTs}")
  }

  /**
   * read
   */


  /**
   * get label info
   * @param labelName
   * @return
   */
  def getLabel(labelName: String) = Action { request =>
    Management.findLabel(labelName) match {
      case None => NotFound(Json.obj("message" -> s"not found $labelName")).as(QueryController.applicationJsonHeader)
      case Some(label) => Ok(label.toJson).as(QueryController.applicationJsonHeader)
    }
  }

  /**
   * get service info
   * @param serviceName
   * @return
   */
  def getService(serviceName: String) = Action { request =>
    Management.findService(serviceName) match {
      case None => NotFound
      case Some(service) => ok(s"${service.toJson} exist.")
    }
  }

  /**
   * get all labels of service
   * @param serviceName
   * @return
   */
  def getLabels(serviceName: String) = Action { request =>
    Service.findByName(serviceName) match {
      case None =>
        bad(s"service $serviceName is not found")
      case Some(service) =>
        val srcs = Label.findBySrcServiceId(service.id.get)
        val tgts = Label.findByTgtServiceId(service.id.get)
        val json = Json.obj("from" -> srcs.map(src => src.toJson), "to" -> tgts.map(tgt => tgt.toJson))
        Ok(json).as(QueryController.applicationJsonHeader)
    }
  }

  /**
   * get service columns
   * @param serviceName
   * @param columnName
   * @return
   */
  def getServiceColumn(serviceName: String, columnName: String) = Action { request =>
    val rets = for {
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName, useCache = false)
    } yield {
        serviceColumn
      }
    rets match {
      case None => NotFound
      case Some(serviceColumn) => Ok(serviceColumn.toJson).as(QueryController.applicationJsonHeader)
    }
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
    tryResponse(serviceTry)(_.serviceName + "is created")
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
    tryResponse(ret)(_.label + "is created")
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
    tryResponse(ret)(_.label + "is updated")
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
    createServiceColumnInner(request.body)
  }

  def createServiceColumnInner(jsValue: JsValue) = {
    try {
      val (serviceName, columnName, columnType, props) = toServiceColumnElements(jsValue)
      Management.createServiceColumn(serviceName, columnName, columnType, props.flatMap(p => Prop.unapply(p)))
      ok(s"$serviceName:$columnName is created.")
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        bad(e.toString)
    }
  }

  /**
   * delete
   */

  /**
   * delete label
   * @param labelName
   * @return
   */
  def deleteLabel(labelName: String) = Action { request =>
    deleteLabelInner(labelName)
  }

  def deleteLabelInner(labelName: String) = {
    Label.findByName(labelName) match {
      case None => NotFound
      case Some(label) =>
        val json = label.toJson
        label.deleteAll()
        ok(s"${json} is deleted")
    }
  }

  def deleteServiceColumnInner(serviceName: String, columnName: String) = {
    for {
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
    } {
      ServiceColumn.delete(serviceColumn.id.get)
    }
  }

  /**
   * delete servieColumn
   * @param serviceName
   * @param columnName
   * @return
   */
  def deleteServiceColumn(serviceName: String, columnName: String) = Action { request =>
    deleteServiceColumnInner(serviceName, columnName)
    ok(s"$serviceName:$columnName is deleted")
  }

  /**
   * update
   */

  /**
   * add Prop to label
   * @param labelName
   * @return
   */
  def addProp(labelName: String) = Action(parse.json) { request =>
    addPropInner(labelName)(request.body) match {
      case None =>
        bad(s"failed to add property on $labelName")
      case Some(p) =>
        Ok(p.toJson).as(QueryController.applicationJsonHeader)
    }
  }

  def addPropInner(labelName: String)(js: JsValue) = {
    val (propName, defaultValue, dataType, usedInIndex) = toPropElements(js)
    for (label <- Label.findByName(labelName)) yield {
      LabelMeta.findOrInsert(label.id.get, propName, defaultValue.toString, dataType)
    }
  }

  /**
   * add props to label
   * @param labelName
   * @return
   */
  def addProps(labelName: String) = Action(parse.json) { request =>
    val jsObjs = request.body.asOpt[List[JsObject]].getOrElse(List.empty[JsObject])
    val newProps = for {
      js <- jsObjs
      newProp <- addPropInner(labelName)(js)
    } yield newProp

    ok(s"${newProps.size} is added.")
  }

  /**
   * add prop to serviceColumn
   * @param serviceName
   * @param columnName
   * @return
   */
  def addServiceColumnProp(serviceName: String, columnName: String) = Action(parse.json) { request =>
    addServiceColumnPropInner(serviceName, columnName)(request.body) match {
      case None => bad(s"can`t find service with $serviceName or can`t find serviceColumn with $columnName")
      case Some(m) => Ok(m.toJson).as(QueryController.applicationJsonHeader)
    }
  }

  def addServiceColumnPropInner(serviceName: String, columnName: String)(js: JsValue) = {
    val (propName, defaultValue, dataType, usedInIndex) = toPropElements(js)
    for {
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
    } yield {
      ColumnMeta.findOrInsert(serviceColumn.id.get, propName, dataType)
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
    try {
      Management.copyLabel(oldLabelName, newLabelName, Some(newLabelName))
      ok(s"$oldLabelName is copied to $newLabelName")
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        bad(e.toString())
    }
  }

  /**
   * rename label
   * @param oldLabelName
   * @param newLabelName
   * @return
   */
  def renameLabel(oldLabelName: String, newLabelName: String) = Action { request =>
    Label.findByName(oldLabelName) match {
      case None => NotFound.as(QueryController.applicationJsonHeader)
      case Some(label) =>
        Management.updateLabelName(oldLabelName, newLabelName)
        ok(s"Label was updated")
    }
  }
}
