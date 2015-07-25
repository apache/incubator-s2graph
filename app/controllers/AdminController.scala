package controllers

import java.util.concurrent.TimeUnit

import com.daumkakao.s2graph.core._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

//import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.mysqls._
import play.api.Logger
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.mvc.{Action, Controller}

import scala.concurrent.ExecutionContext.Implicits.global

object AdminController extends Controller with RequestParser {
  /**
   * Management
   */
  def warmUpInner(): Long = {
    val startTs = System.currentTimeMillis()
    def timeElapsed() = System.currentTimeMillis() - startTs

    Service.findAll()
    ServiceColumn.findAll()
    Label.findAll()
    LabelMeta.findAll()
    LabelIndex.findAll()
    ColumnMeta.findAll()

    Logger.info(s"query start")

    val query1 = """ {"srcVertices":[{"serviceName":"talk_3rd_tab","columnName":"talk_user_id", "id":9410227}],"steps":[[{"label":"talk_friend_long_term_agg","direction":"out","offset":0,"limit":300,"scoring":{"score":1,"_timestamp":0}},{"label":"talk_3rd_tab_block","direction":"out","offset":0,"limit":100,"exclude":true}],[{"label": "talk_3rd_tab_like","direction":"out","offset":0,"limit":30}]]}  """
    val query2 = """ {"srcVertices":[{"serviceName":"talk_3rd_tab","columnName":"talk_user_id","id":169411847}],"steps":[[{"label":"talk_friend_long_term_agg","direction":"out","offset":0,"limit":300,"scoring":{"score":1,"_timestamp":0}},{"label":"talk_3rd_tab_block","direction":"out","offset":0,"limit":100,"exclude":true}],[{"label":"talk_3rd_tab_like","direction":"out","offset":0,"limit":30}]]} """
    val query3 = """ {"srcVertices":[{"serviceName":"kakaostory","columnName":"profile_id","ids":[65660695,52518350,53482175,68921330,71372434,52891890,42697491,24116197,54108362,66648693,22635568,68921330,60052312,72976756,33642081,45102779,62786599,46003816,57849639,26133825,4497097,60359770,24222929,71892788,70272489,59095164,25421798,22112977,13814590,32194371,21861285,67950112,65219587,32551393,54056293,54011860,7837026,30936312,65659859,31947078,53802286,72081911,55768373,21365239,47511539,54002603,28377885,16437985,59017673,1623772,60598255,71663202,6388145,53268221,54002603,68362659,43865963,72647027,71068920,25769244,16706997,36677771,39597780,26889008,68440446,21233115,62651426,51334055,42540591,68274266,47948176,65846523,38833161,73189243,63909308,31749988,46316220,6652728,45307242,51831124,66047440,51588095,16892711,48622597,33519830,48184709,59822730,71617645,49418049,71012408,58226587,59738903,73021597,14218219,54934491,46472435,12245561,55751365,14275836,69743448]}],"steps":[[{"label":"_s2graph_profile_id_talk_user_id","direction":"out","offset":0,"maxAttempt":10,"rpcTimeout":1000,"limit":1}]]}  """
    val query4 = """ {"srcVertices":[{"serviceName":"talk_3rd_tab","columnName":"talk_user_id","id":22262097}],"steps":[[{"label":"talk_friend_long_term_agg","direction":"out","offset":0,"limit":300,"scoring":{"score":1,"_timestamp":0}},{"label":"talk_3rd_tab_block","direction":"out","offset":0,"limit":100,"exclude":true}],[{"label":"talk_3rd_tab_like","direction":"out","offset":0,"limit":30}]]} """
    val query5 = """ {"srcVertices":[{"id":88326044371803393,"ids":null,"serviceName":"kakao","columnName":"service_user_id","props":null,"timestamp":null,"operation":null}],"steps":[[{"label":"_s2graph_service_user_id_talk_user_id","direction":"out","outputField":null,"offset":0,"limit":1,"interval":null,"duration":null,"scoring":null,"where":null}],[{"label":"talk_friend_long_term_agg","direction":"out","outputField":null,"offset":0,"limit":100,"interval":null,"duration":null,"scoring":{"_timestamp":0,"score":1},"where":"service_user_id != 0"}]]} """
    val query6 = """ {"srcVertices":[{"id":90689769447474624,"ids":null,"serviceName":"kakao","columnName":"service_user_id","props":null,"timestamp":null,"operation":null}],"steps":[[{"label":"_s2graph_service_user_id_talk_user_id","direction":"out","outputField":null,"offset":0,"limit":1,"interval":null,"duration":null,"scoring":null,"where":null}],[{"label":"talk_friend_long_term_agg","direction":"out","outputField":null,"offset":0,"limit":100,"interval":null,"duration":null,"scoring":{"_timestamp":0,"score":1},"where":"service_user_id != 0"}]]} """
    val query7 = """ {"srcVertices":[{"serviceName":"talk_3rd_tab","columnName":"talk_user_id","id":126113387}],"steps":[[{"label":"talk_friend_long_term_agg","direction":"out","offset":0,"limit":300,"scoring":{"score":1,"_timestamp":0}},{"label":"talk_3rd_tab_block","direction":"out","offset":0,"limit":100,"exclude":true}],[{"label":"talk_3rd_tab_like","direction":"out","offset":0,"limit":30}]]} """
    val query8 = """ {"srcVertices":[{"serviceName":"kakaostory","columnName":"profile_id","ids":[42426758,34295179,13345730,21883859,51288751,9795440,33609145,23191680,30536138,70281916,71697556,25237698,42078481,8395515,970928,42641108,36910488,35516083,42411812,29042434,6592296,17020889,2963519,1479531,33956065,45536128,20577307,2932850,9816492,49689221,45491660,71950921,62480810,34545282,39143763,25732777,17174763,28835548,32777456,10688850,14241823,34544627,51676180]}],"steps":[[{"label":"_s2graph_profile_id_talk_user_id","direction":"out","offset":0,"maxAttempt":10,"rpcTimeout":1000,"limit":1}]]} """

    val queries = Seq(query1, query2, query3, query4, query5, query6, query7, query8)
    val rand = new Random()

    while (timeElapsed() < 270000) {
      val numOfFuture =
        if (timeElapsed() < 60000) 4
        else if (timeElapsed() < 120000) 3
        else if (timeElapsed() < 180000) 2
        else 1

      val futures = (0 until numOfFuture).toList.map { _ =>
        QueryController.getEdgesInner(Json.parse(queries(rand.nextInt(queries.size))))
      }

      Await.result(Future.sequence(futures), Duration(10000, TimeUnit.MILLISECONDS))
    }


    val waitIdle = 30000

    Logger.info(s"query complete, wait ${waitIdle}")
    Thread.sleep(waitIdle)

    timeElapsed()
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

  def deleteVertexInner(serviceName: String, columnName: String) = {
    for {
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
    } {
      ServiceColumn.delete(serviceColumn.id.get)
    }
  }

  def deleteVertex(serviceName: String, columnName: String) = Action { request =>
    deleteVertexInner(serviceName, columnName)
    Ok("deleted")
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

  def createVertex() = Action(parse.json) { request =>
    createVertexInner(request.body)
  }

  def createVertexInner(jsValue: JsValue) = {
    try {
      val (serviceName, columnName, columnType, props) = toVertexElements(jsValue)
      Management.createVertex(serviceName, columnName, columnType, props)
      Ok("Created\n")
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        BadRequest(s"$e")
    }
  }

  def getVertex(serviceName: String, columnName: String) = Action { request =>
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

  private def addVertexPropInner(serviceName: String, columnName: String)(js: JsValue) = {
    val (propName, defaultValue, dataType, usedInIndex) = toPropElements(js)
    for {
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
    } yield {
      ColumnMeta.findOrInsert(serviceColumn.id.get, propName, dataType)
    }
  }

  def addVertexProp(serviceName: String, columnName: String) = Action(parse.json) { request =>
    addVertexPropInner(serviceName, columnName)(request.body) match {
      case None => BadRequest(s"can`t find service with $serviceName or can`t find serviceColumn with $columnName")
      case Some(m) => Ok(s"$m")
    }
  }

  def addVertexProps(serviecName: String, columnName: String) = Action(parse.json) { request =>
    val jsObjs = request.body.asOpt[List[JsObject]].getOrElse(List.empty[JsObject])
    val newProps = for {
      js <- jsObjs
      newProp <- addVertexPropInner(serviecName, columnName)(js)
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
