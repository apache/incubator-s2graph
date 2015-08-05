package controllers

import com.daumkakao.s2graph.core._

import scala.concurrent.Future

//import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.mysqls._
import play.api.Logger
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.mvc.{Action, Controller}

object AdminController extends Controller with RequestParser {
  /**
   * Management
   */
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

    Ok(s"${System.currentTimeMillis() - startTs}")
  }

  def getService(serviceName: String) = Action { request =>
    Management.findService(serviceName) match {
      case None => NotFound
      case Some(service) => Ok(s"${service.toJson} exist.").as(QueryController.applicationJsonHeader)
    }
  }

  def createService() = Action(parse.json) { request =>
    createServiceInner(request.body)
  }

  def createServiceInner(jsValue: JsValue) = {
    try {
      val (serviceName, cluster, tableName, preSplitSize, ttl) = toServiceElements(jsValue)
      val service = Management.createService(serviceName, cluster, tableName, preSplitSize, ttl)
      Ok(s"$service service created.\n").as(QueryController.applicationJsonHeader)
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        BadRequest(e.getMessage())
    }
  }

  def createLabel() = Action(parse.json) { request =>
    createLabelInner(request.body)
  }

  def createLabelInner(jsValue: JsValue) = {
    try {
      val (labelName, srcServiceName, srcColumnName, srcColumnType,
      tgtServiceName, tgtColumnName, tgtColumnType, isDirected,
      serviceName, idxProps, metaProps, consistencyLevel, hTableName, hTableTTL, schemaVersion, isAsync) =
        toLabelElements(jsValue)

      Management.createLabel(labelName, srcServiceName, srcColumnName, srcColumnType,
        tgtServiceName, tgtColumnName, tgtColumnType, isDirected, serviceName,
        idxProps.map { t => (t._1, t._2.toString, t._3) },
        metaProps.map { t => (t._1, t._2.toString, t._3) },
        consistencyLevel, hTableName, hTableTTL, schemaVersion, isAsync)
      Ok("Created\n").as(QueryController.applicationJsonHeader)
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        BadRequest(s"$e")
    }
  }

  def addIndex() = Action(parse.json) { request =>
    try {
      val (labelName, idxProps) = toIndexElements(request.body)
      Management.addIndex(labelName, idxProps)
      Ok("Created\n").as(QueryController.applicationJsonHeader)
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        BadRequest(s"$e")
    }
  }

  def getLabel(labelName: String) = Action { request =>
    Management.findLabel(labelName) match {
      case None => NotFound("NotFound\n").as(QueryController.applicationJsonHeader)
      case Some(label) =>
        Ok(s"${label.toJson}\n").as(QueryController.applicationJsonHeader)
    }
  }

  def getLabels(serviceName: String) = Action { request =>
    Service.findByName(serviceName) match {
      case None => BadRequest(s"create service first.").as(QueryController.applicationJsonHeader)
      case Some(service) =>
        val srcs = Label.findBySrcServiceId(service.id.get)
        val tgts = Label.findByTgtServiceId(service.id.get)
        val json = Json.obj("from" -> srcs.map(src => src.toJson), "to" -> tgts.map(tgt => tgt.toJson))
        Ok(s"$json\n").as(QueryController.applicationJsonHeader)
    }
  }



  def deleteLabel(labelName: String) = Action { request =>
    deleteLabelInner(labelName)
  }

  def deleteLabelInner(labelName: String) = {
    Label.findByName(labelName) match {
      case None => NotFound
      case Some(label) =>
        val json = label.toJson
        label.deleteAll()
        Ok(s"${json} is deleted.\n").as(QueryController.applicationJsonHeader)
    }
  }

  private def addPropInner(labelName: String)(js: JsValue) = {
    val (propName, defaultValue, dataType, usedInIndex) = toPropElements(js)
    for (label <- Label.findByName(labelName)) yield {
      LabelMeta.findOrInsert(label.id.get, propName, defaultValue.toString, dataType)
    }
  }

  def copyLabel(oldLabelName: String, newLabelName: String) = Action { request =>
    try {
      Management.copyLabel(oldLabelName, newLabelName, Some(newLabelName))
      Ok(s"Label is copied.\n")
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        BadRequest(s"$e")

    }
  }

  def renameLabel(oldLabelName: String, newLabelName: String) = Action { request =>
    Label.findByName(oldLabelName) match {
      case None => NotFound
      case Some(label) =>
        Management.updateLabelName(oldLabelName, newLabelName)
        Ok(s"Label was updated.\n")
    }
  }

  def addProp(labelName: String) = Action(parse.json) { request =>
    addPropInner(labelName)(request.body) match {
      case None =>
        BadRequest(s"failed to add property on $labelName")
      case Some(p) =>
        Ok(s"${p.toJson}")
    }
  }

  def addProps(labelName: String) = Action(parse.json) { request =>
    val jsObjs = request.body.asOpt[List[JsObject]].getOrElse(List.empty[JsObject])
    val newProps = for {
      js <- jsObjs
      newProp <- addPropInner(labelName)(js)
    } yield newProp
    Ok(s"${newProps.size} is added.")
  }

  def createServiceColumn() = Action(parse.json) { request =>
    createServiceColumnInner(request.body)
  }

  def createServiceColumnInner(jsValue: JsValue) = {
    try {
      val (serviceName, columnName, columnType, props) = toServiceColumnElements(jsValue)
      Management.createServiceColumn(serviceName, columnName, columnType, props)
      Ok("Created\n")
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        BadRequest(s"$e")
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

  def deleteServiceColumn(serviceName: String, columnName: String) = Action { request =>
    deleteServiceColumnInner(serviceName, columnName)
    Ok("deleted")
  }
  def getServiceColumn(serviceName: String, columnName: String) = Action { request =>
    val rets = for {
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName, useCache = false)
    } yield {
        serviceColumn
      }
    rets match {
      case None => NotFound
      case Some(serviceColumn) => Ok(s"$serviceColumn")
    }
  }

  private def addServiceColumnPropInner(serviceName: String, columnName: String)(js: JsValue) = {
    val (propName, defaultValue, dataType, usedInIndex) = toPropElements(js)
    for {
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
    } yield {
      ColumnMeta.findOrInsert(serviceColumn.id.get, propName, dataType)
    }
  }

  def addServiceColumnProp(serviceName: String, columnName: String) = Action(parse.json) { request =>
    addServiceColumnPropInner(serviceName, columnName)(request.body) match {
      case None => BadRequest(s"can`t find service with $serviceName or can`t find serviceColumn with $columnName")
      case Some(m) => Ok(s"$m")
    }
  }

  def addServiceColumnProps(serviecName: String, columnName: String) = Action(parse.json) { request =>
    val jsObjs = request.body.asOpt[List[JsObject]].getOrElse(List.empty[JsObject])
    val newProps = for {
      js <- jsObjs
      newProp <- addServiceColumnPropInner(serviecName, columnName)(js)
    } yield newProp
    Ok(s"${newProps.size} is added.")
  }

  /**
   * end of management
   */

  // get all labels belongs to this service.
  def labels(serviceName: String) = Action {
    Ok("not implemented yet.")
  }

  //  def page = Action {
  //    Ok(views.html.admin("S2Graph Admin Page"))
  //  }
  //
  //  def manager = Action {
  //    Ok(views.html.manager("S2Graph Manager Page"))
  //  }

  //
  //  def swapLabel(oldLabelName: String, newLabelName: String) = Action {
  //    Label.findByName(oldLabelName) match {
  //      case None => NotFound
  //      case Some(oldLabel) =>
  //        val ret = Label.copyLabel(oldLabel, newLabelName)
  //        Ok(s"$ret\n")
  //    }
  //  }
  //
  //  def allServices = Action {
  //    val svcs = Service.findAll()
  //    Ok(Json.toJson(svcs.map(svc => svc.toJson))).as(QueryController.applicationJsonHeader)
  //  }

  /**
   * never, ever exposes this to user.
   */
  //  def deleteEdges(zkAddr: String, tableName: String, labelIds: String, minTs: Long, maxTs: Long) = Action {
  //    val stats = Management.deleteEdgesByLabelIds(zkAddr, tableName, labelIds = labelIds, minTs = minTs, maxTs = maxTs, include = true)
  //    Ok(s"$stats\n")
  //  }
  //  def deleteAllEdges(zkAddr: String, tableName: String, minTs: Long, maxTs: Long) = Action {
  //    val labelIds = Label.findAllLabels(None, offset = 0, limit = Int.MaxValue).map(_.id.get).mkString(",")
  //    val stats = Management.deleteEdgesByLabelIds(zkAddr, tableName, labelIds = labelIds, minTs = minTs, maxTs = maxTs, include = false)
  //    Ok(s"$stats\n")
  //  }
}
