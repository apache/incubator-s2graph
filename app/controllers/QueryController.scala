package controllers


import com.daumkakao.s2graph.core.mysqls._
import config.Config

//import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.types2.{LabelWithDirection, VertexId}
import play.api.Logger
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, Controller, Result}

import scala.concurrent._

object QueryController extends Controller with RequestParser {

  import ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  /**
   * only for test
   */

  private def badQueryExceptionResults(ex: Exception) = Future.successful(BadRequest(s"""{"message": "${ex.getMessage}"}""").as(applicationJsonHeader))
  private def errorResults = Future.successful(Ok(s"${PostProcess.timeoutResults}\n").as(applicationJsonHeader))
  /**
   * end of only for test
   */

  val applicationJsonHeader = "application/json"
  val orderByKeys = Seq("weight")
//  val errorLogger = Logger("error")

  // select

  def getEdges() = withHeaderAsync(parse.json) { request =>
    getEdgesInner(request.body)
  }

  def getEdgesExcluded() = withHeaderAsync(parse.json) { request =>
    getEdgesExcludedInner(request.body)
  }

  private def getEdgesAsync(jsonQuery: JsValue)
                           (post: (Seq[QueryResult], Seq[QueryResult]) => JsValue): Future[Result] = {
    try {
      val queryTemplateId = (jsonQuery \ "steps").toString()
      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(s"$jsonQuery")
      val q = toQuery(jsonQuery)
//      KafkaAggregatorActor.enqueue(queryInTopic, q.templateId().toString)
      Logger.info(s"${q.templateId()}")

      val filterOutQueryResultsLs = q.filterOutQuery match {
        case Some(filterOutQuery) => Graph.getEdgesAsync(filterOutQuery)
        case None => Future.successful(Seq.empty)
      }
      for {
        queryResultsLs <- Graph.getEdgesAsync(q)
        filterOutResultsLs <- filterOutQueryResultsLs
      } yield {
        val json = post(queryResultsLs, filterOutResultsLs)
        Ok(json).as(applicationJsonHeader)
      }
//      val future = Graph.getEdgesAsync(q)
//      future map { queryParamEdgeWithScoreLs =>
//        val json = post(queryParamEdgeWithScoreLs)
//        Ok(json).as(applicationJsonHeader)
//      }
    } catch {
      case e: KGraphExceptions.BadQueryException =>
        errorLogger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      case e: Throwable =>
        errorLogger.error(s"$jsonQuery, $e", e)
        // watch tower
//        errorResults
//        Future.successful(Ok(s"${PostProcess.emptyResults}\n").as(applicationJsonHeader))
        errorResults
      //        Ok(s"\n").as(applicationJsonHeader)
    }
  }

  private def getEdgesExcludedAsync(jsonQuery: JsValue)(post: (Seq[QueryResult],
    Seq[QueryResult]) => JsValue): Future[Result] = {
    try {
      val queryTemplateId = (jsonQuery \ "steps").toString()

      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(s"$jsonQuery")
      val q = toQuery(jsonQuery)
//      KafkaAggregatorActor.enqueue(queryInTopic, q.templateId().toString)
      Logger.debug(s"${q.templateId()}")

      val filterOutQuery = Query(q.vertices, List(q.steps.last))

      for (exclude <- Graph.getEdgesAsync(filterOutQuery); queryResultLs <- Graph.getEdgesAsync(q)) yield {
        val json = post(queryResultLs, exclude)
        Ok(json).as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException =>
        errorLogger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      case e: Throwable =>
        errorLogger.error(s"$jsonQuery, $e", e)
        // watch tower
        errorResults
    }
  }

  def getEdgesInner(jsonQuery: JsValue) = {
    getEdgesAsync(jsonQuery)(PostProcess.toSimpleVertexArrJson)
  }

  def getEdgesExcludedInner(jsValue: JsValue) = {
    getEdgesExcludedAsync(jsValue)(PostProcess.toSimpleVertexArrJson)
  }

  def getEdgesWithGrouping() = withHeaderAsync(parse.json) { request =>
    getEdgesWithGroupingInner(request.body)
  }

  def getEdgesWithGroupingInner(jsonQuery: JsValue) = {
    getEdgesAsync(jsonQuery)(PostProcess.summarizeWithListFormatted)
  }

  def getEdgesExcludedWithGrouping() = withHeaderAsync(parse.json) { request =>
    getEdgesExcludedWithGroupingInner(request.body)
  }
  def getEdgesExcludedWithGroupingInner(jsonQuery: JsValue) = {
    getEdgesExcludedAsync(jsonQuery)(PostProcess.summarizeWithListExcludeFormatted)
  }


  def getEdgesGroupedInner(jsonQuery: JsValue) = {
    getEdgesAsync(jsonQuery)(PostProcess.summarizeWithList)
  }
  @deprecated(message = "deprecated", since = "0.2")
  def getEdgesGrouped() = withHeaderAsync(parse.json) { request =>
    getEdgesGroupedInner(request.body)
  }


  @deprecated(message = "deprecated", since = "0.2")
  def getEdgesGroupedExcluded() = withHeaderAsync(parse.json) { request =>
    getEdgesGroupedExcludedInner(request.body)
  }

  def getEdgesGroupedExcludedInner(jsonQuery: JsValue): Future[Result] = {
    try {
      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(jsonQuery.toString())
      val q = toQuery(jsonQuery)
      val filterOutQuery = Query(q.vertices, List(q.steps.last))
//      KafkaAggregatorActor.enqueue(queryInTopic, q.templateId().toString)
      Logger.debug(s"${q.templateId()}")

      for (exclude <- Graph.getEdgesAsync(filterOutQuery); queryResultLs <- Graph.getEdgesAsync(q)) yield {
        val json = PostProcess.summarizeWithListExclude(queryResultLs, exclude)
        Ok(json).as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException =>
        errorLogger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      //        Future.successful(BadRequest(request.body).as(applicationJsonHeader))
      case e: Throwable =>
        errorLogger.error(s"$jsonQuery, $e", e)
        errorResults
    }
  }
  @deprecated(message = "deprecated", since = "0.2")
  def getEdgesGroupedExcludedFormatted = withHeaderAsync(parse.json) { request =>
    getEdgesGroupedExcludedFormattedInner(request.body)
  }

  def getEdgesGroupedExcludedFormattedInner(jsonQuery: JsValue): Future[Result] = {
    try {
      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(jsonQuery.toString)
      val q = toQuery(jsonQuery)
      val filterOutQuery = Query(q.vertices, List(q.steps.last))
//      KafkaAggregatorActor.enqueue(queryInTopic, q.templateId().toString)
      Logger.debug(s"${q.templateId()}")

      for (exclude <- Graph.getEdgesAsync(filterOutQuery); queryResultLs <- Graph.getEdgesAsync(q)) yield {
        val json = PostProcess.summarizeWithListExcludeFormatted(queryResultLs, exclude)
        Ok(json).as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException =>
        errorLogger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      case e: Throwable =>
        errorLogger.error(s"$jsonQuery, $e", e)
        errorResults
    }
  }


  def getEdge(srcId: String, tgtId: String, labelName: String, direction: String) = Action.async { request =>
    if (!Config.IS_QUERY_SERVER) Future.successful(Unauthorized)
    val params = Json.arr(Json.obj("label" -> labelName, "direction" -> direction, "from" -> srcId, "to" -> tgtId))
    checkEdgesInner(params)
  }

  /**
   * Vertex
   */

  def checkEdgesInner(jsValue: JsValue) = {
    Logger.info(s"$jsValue")
    try {
      val params = jsValue.as[List[JsValue]]
      var isReverted = false
      val labelWithDirs = scala.collection.mutable.HashSet[LabelWithDirection]()
      val quads = for {
        param <- params
        labelName <- (param \ "label").asOpt[String]
        direction <- GraphUtil.toDir((param \ "direction").asOpt[String].getOrElse("out"))
        label <- Label.findByName(labelName)
        srcId <- jsValueToInnerVal((param \ "from").as[JsValue], label.srcColumnWithDir(direction.toInt).columnType, label.schemaVersion)
        tgtId <- jsValueToInnerVal((param \ "to").as[JsValue], label.tgtColumnWithDir(direction.toInt).columnType, label.schemaVersion)
      } yield {
          val labelWithDir = LabelWithDirection(label.id.get, direction)
          labelWithDirs += labelWithDir
          val (src, tgt, dir) = if (direction == 1) {
            isReverted = true
            (Vertex(VertexId(label.tgtColumnWithDir(direction.toInt).id.get, tgtId)),
              Vertex(VertexId(label.srcColumnWithDir(direction.toInt).id.get, srcId)), 0)
          } else {
            (Vertex(VertexId(label.srcColumnWithDir(direction.toInt).id.get, srcId)),
              Vertex(VertexId(label.tgtColumnWithDir(direction.toInt).id.get, tgtId)), 0)
          }

          Logger.debug(s"SrcVertex: $src")
          Logger.debug(s"TgtVertex: $tgt")
          Logger.debug(s"direction: $dir")
          (src, tgt, label, dir.toInt)
        }

      Graph.checkEdges(quads).map { case queryResultLs  =>
        val edgeJsons = for {
          queryResult <- queryResultLs
          (edge, score) <- queryResult.edgeWithScoreLs
          edgeJson <- PostProcess.edgeToJson(if (isReverted) edge.duplicateEdge else edge, score, queryResult)
        } yield edgeJson

        Ok(Json.toJson(edgeJsons)).as(applicationJsonHeader)
      }
    } catch {
      case e: Throwable =>
        errorLogger.error(s"$jsValue, $e", e)
        errorResults
    }
  }

  def checkEdges() = withHeaderAsync(parse.json) { request =>
    if (!Config.IS_QUERY_SERVER) Future.successful(Unauthorized)
    checkEdgesInner(request.body)
  }

  def getVertices() = withHeaderAsync(parse.json) { request =>
    if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)
    val jsonQuery = request.body
    val ts = System.currentTimeMillis()
    val props = "{}"
    try {
      val vertices = request.body.as[List[JsValue]].flatMap { js =>
        val serviceName = (js \ "serviceName").as[String]
        val columnName = (js \ "columnName").as[String]
        for (id <- (js \ "ids").asOpt[List[JsValue]].getOrElse(List.empty[JsValue])) yield {
          Management.toVertex(ts, "insert", id.toString, serviceName, columnName, props)
        }
      }
      Graph.getVerticesAsync(vertices) map { vertices =>
        val json = PostProcess.verticesToJson(vertices)
        Ok(s"$json\n").as(applicationJsonHeader)
      }
    } catch {
      case e : play.api.libs.json.JsResultException =>
        errorLogger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      case e: Exception =>
        errorLogger.error(s"$jsonQuery, $e", e)
        errorResults
    }
  }

}
