package controllers


import com.kakao.s2graph.core.GraphExceptions.BadQueryException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.rest.{RestCaller, RequestParser}
import com.kakao.s2graph.core.types.{LabelWithDirection, VertexId}
import com.kakao.s2graph.core.utils.logger
import config.Config
import play.api.libs.json.{JsArray, JsObject, JsValue, Json}
import play.api.mvc.{Action, Controller, Result}

import scala.concurrent._
import scala.language.postfixOps
import scala.util.Try

object QueryController extends Controller with JSONParser {

  import ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private val s2: Graph = com.kakao.s2graph.rest.Global.s2graph
  private val requestParser: RequestParser = com.kakao.s2graph.rest.Global.s2parser
  private val rest: RestCaller = com.kakao.s2graph.rest.Global.s2rest

  private def badQueryExceptionResults(ex: Exception) = Future.successful(BadRequest(Json.obj("message" -> ex.getMessage)).as(applicationJsonHeader))

  private def errorResults = Future.successful(Ok(PostProcess.timeoutResults).as(applicationJsonHeader))

  def getEdges() = withHeaderAsync(jsonParser) { request =>
    getEdgesInner(request.body)
  }

  def getEdgesExcluded = withHeaderAsync(jsonParser) { request =>
    getEdgesExcludedInner(request.body)
  }

  private def eachQuery(post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue)(q: Query): Future[JsValue] = {
    val filterOutQueryResultsLs = q.filterOutQuery match {
      case Some(filterOutQuery) => s2.getEdges(filterOutQuery)
      case None => Future.successful(Seq.empty)
    }

    for {
      queryResultsLs <- s2.getEdges(q)
      filterOutResultsLs <- filterOutQueryResultsLs
    } yield {
      val json = post(queryResultsLs, filterOutResultsLs)
      json
    }
  }

  private def calcSize(js: JsValue): Int = js match {
    case JsObject(obj) => (js \ "size").asOpt[Int].getOrElse(0)
    case JsArray(seq) => seq.map(js => (js \ "size").asOpt[Int].getOrElse(0)).sum
    case _ => 0
  }

  private def getEdgesAsync(jsonQuery: JsValue)
                           (post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue): Future[Result] = {
    if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)
    val fetch = eachQuery(post) _
//    logger.info(jsonQuery)

    Try {
      val future = jsonQuery match {
        case JsArray(arr) => Future.traverse(arr.map(requestParser.toQuery(_)))(fetch).map(JsArray)
        case obj@JsObject(_) => fetch(requestParser.toQuery(obj))
        case _ => throw BadQueryException("Cannot support")
      }

      future map { json => jsonResponse(json, "result_size" -> calcSize(json).toString) }

    } recover {
      case e: BadQueryException =>
        logger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      case e: Exception =>
        logger.error(s"$jsonQuery, $e", e)
        errorResults
    } get
  }

  @deprecated(message = "deprecated", since = "0.2")
  private def getEdgesExcludedAsync(jsonQuery: JsValue)
                                   (post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue): Future[Result] = {

    if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

    Try {
      val q = requestParser.toQuery(jsonQuery)
      val filterOutQuery = Query(q.vertices, Vector(q.steps.last))

      val fetchFuture = s2.getEdges(q)
      val excludeFuture = s2.getEdges(filterOutQuery)

      for {
        queryResultLs <- fetchFuture
        exclude <- excludeFuture
      } yield {
        val json = post(queryResultLs, exclude)
        jsonResponse(json, "result_size" -> calcSize(json).toString)
      }
    } recover {
      case e: BadQueryException =>
        logger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      case e: Exception =>
        logger.error(s"$jsonQuery, $e", e)
        errorResults
    } get
  }

  def getEdgesInner(jsonQuery: JsValue) = {
    getEdgesAsync(jsonQuery)(PostProcess.toSimpleVertexArrJson)
  }

  def getEdgesExcludedInner(jsValue: JsValue) = {
    getEdgesExcludedAsync(jsValue)(PostProcess.toSimpleVertexArrJson)
  }

  def getEdgesWithGrouping() = withHeaderAsync(jsonParser) { request =>
    getEdgesWithGroupingInner(request.body)
  }

  def getEdgesWithGroupingInner(jsonQuery: JsValue) = {
    getEdgesAsync(jsonQuery)(PostProcess.summarizeWithListFormatted)
  }

  def getEdgesExcludedWithGrouping() = withHeaderAsync(jsonParser) { request =>
    getEdgesExcludedWithGroupingInner(request.body)
  }

  def getEdgesExcludedWithGroupingInner(jsonQuery: JsValue) = {
    getEdgesExcludedAsync(jsonQuery)(PostProcess.summarizeWithListExcludeFormatted)
  }

  def getEdgesGroupedInner(jsonQuery: JsValue) = {
    getEdgesAsync(jsonQuery)(PostProcess.summarizeWithList)
  }

  @deprecated(message = "deprecated", since = "0.2")
  def getEdgesGrouped() = withHeaderAsync(jsonParser) { request =>
    getEdgesGroupedInner(request.body)
  }

  @deprecated(message = "deprecated", since = "0.2")
  def getEdgesGroupedExcluded() = withHeaderAsync(jsonParser) { request =>
    getEdgesGroupedExcludedInner(request.body)
  }

  @deprecated(message = "deprecated", since = "0.2")
  def getEdgesGroupedExcludedInner(jsonQuery: JsValue): Future[Result] = {
    if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

    Try {
      val q = requestParser.toQuery(jsonQuery)
      val filterOutQuery = Query(q.vertices, Vector(q.steps.last))

      val fetchFuture = s2.getEdges(q)
      val excludeFuture = s2.getEdges(filterOutQuery)

      for {
        queryResultLs <- fetchFuture
        exclude <- excludeFuture
      } yield {
        val json = PostProcess.summarizeWithListExclude(queryResultLs, exclude)
        jsonResponse(json, "result_size" -> calcSize(json).toString)
      }
    } recover {
      case e: BadQueryException =>
        logger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      case e: Exception =>
        logger.error(s"$jsonQuery, $e", e)
        errorResults
    } get
  }

  @deprecated(message = "deprecated", since = "0.2")
  def getEdgesGroupedExcludedFormatted = withHeaderAsync(jsonParser) { request =>
    getEdgesGroupedExcludedFormattedInner(request.body)
  }

  @deprecated(message = "deprecated", since = "0.2")
  def getEdgesGroupedExcludedFormattedInner(jsonQuery: JsValue): Future[Result] = {
    if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

    Try {
      val q = requestParser.toQuery(jsonQuery)
      val filterOutQuery = Query(q.vertices, Vector(q.steps.last))

      val fetchFuture = s2.getEdges(q)
      val excludeFuture = s2.getEdges(filterOutQuery)

      for {
        queryResultLs <- fetchFuture
        exclude <- excludeFuture
      } yield {
        val json = PostProcess.summarizeWithListExcludeFormatted(queryResultLs, exclude)
        jsonResponse(json, "result_size" -> calcSize(json).toString)
      }
    } recover {
      case e: BadQueryException =>
        logger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      case e: Exception =>
        logger.error(s"$jsonQuery, $e", e)
        errorResults
    } get
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
    try {
      val jsRes = rest.checkEdges(jsValue)
      jsRes.map { res =>
        jsonResponse(res, "result_size" -> rest.calcSize(res).toString)
      }
    } catch {
      case e: Exception =>
        logger.error(s"$jsValue, $e", e)
        errorResults
    }
  }

  def checkEdges() = withHeaderAsync(jsonParser) { request =>
    if (!Config.IS_QUERY_SERVER) Future.successful(Unauthorized)

    checkEdgesInner(request.body)
  }

  def getVertices() = withHeaderAsync(jsonParser) { request =>
    getVerticesInner(request.body)
  }

  def getVerticesInner(jsValue: JsValue) = {
    if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

    val jsonQuery = jsValue
    val ts = System.currentTimeMillis()
    val props = "{}"

    Try {
      val vertices = jsonQuery.as[List[JsValue]].flatMap { js =>
        val serviceName = (js \ "serviceName").as[String]
        val columnName = (js \ "columnName").as[String]
        for (id <- (js \ "ids").asOpt[List[JsValue]].getOrElse(List.empty[JsValue])) yield {
          Management.toVertex(ts, "insert", id.toString, serviceName, columnName, props)
        }
      }

      s2.getVertices(vertices) map { vertices =>
        val json = PostProcess.verticesToJson(vertices)
        jsonResponse(json, "result_size" -> calcSize(json).toString)
      }
    } recover {
      case e: play.api.libs.json.JsResultException =>
        logger.error(s"$jsonQuery, $e", e)
        badQueryExceptionResults(e)
      case e: Exception =>
        logger.error(s"$jsonQuery, $e", e)
        errorResults
    } get
  }
}
