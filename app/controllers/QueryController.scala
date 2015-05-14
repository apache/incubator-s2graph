package controllers


import com.codahale.metrics.Meter
import com.daumkakao.s2graph.core.HBaseElement._
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.models.{HLabel, HService}
import com.daumkakao.s2graph.rest.config.{Instrumented, Config}
import play.api.Logger

//import models.response.param.{ VertexQueryResponse, EdgeQueryResponse }
import play.api.libs.json.{ JsValue, Json }
import play.api.mvc.{ Action, Controller, Result }
import util.TestDataLoader
import scala.concurrent._

object QueryController extends Controller  with RequestParser with Instrumented {

//  import play.api.libs.concurrent.Execution.Implicits._
//  implicit val ex = ApplicationController.globalExecutionContext
  import ApplicationController._
//  implicit val ex = ApplicationController.globalExecutionContext
  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  //  implicit val context = Akka.system.dispatchers.lookup("akka.actor.multiget-context")
  /**
   * only for test
   */
  private val queryUseMultithread = true
  private val buildResultJson = true
  private val maxLength = 64 * 1024 + 255 + 2 + 1
  private val emptyResult = Seq(Seq.empty[(Edge, Double)])
  /**
   * end of only for test
   */

  val applicationJsonHeader = "application/json"
  val orderByKeys = Seq("weight")



  // select

  def getEdges() = withHeaderAsync(parse.json) { request =>
    getEdgesInner(request.body)
  }

  def getEdgesExcluded() = withHeaderAsync(parse.json) { request =>
    getEdgesExcludedInner(request.body)
  }

  private def getEdgesAsync(jsonQuery: JsValue)(post: Seq[Iterable[(Edge, Double)]] => JsValue): Future[Result] = {
    try {
      val queryTemplateId = (jsonQuery \ "steps").toString()
      getOrElseUpdateMetric(queryTemplateId)(metricRegistry.meter(queryTemplateId)).mark()

      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(s"$jsonQuery")
      val q = toQuery(jsonQuery)

      val future = Graph.getEdgesAsync(q)
      future map { edges =>
        val json = post(edges)
        Ok(s"${json}\n").as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException =>
        Logger.error(s"$e", e)
        Future { BadRequest.as(applicationJsonHeader) }
      case e: Throwable => Future {
        Logger.error(s"$e", e)
        // watch tower
        Ok(s"${post(emptyResult)}\n").as(applicationJsonHeader)
      }
    }
  }
  private def getEdgesExcludedAsync(jsonQuery: JsValue)(post: (Seq[Iterable[(Edge, Double)]],
    Seq[Iterable[(Edge, Double)]]) => JsValue): Future[Result] = {
    try {
      val queryTemplateId = (jsonQuery \ "steps").toString()
      getOrElseUpdateMetric[Meter](queryTemplateId)(metricRegistry.meter(queryTemplateId)).mark()

      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(s"$jsonQuery")
      val q = toQuery(jsonQuery)
      val mineQ = Query(q.vertices, List(q.steps.last))

      for (mine <- Graph.getEdgesAsync(mineQ); others <- Graph.getEdgesAsync(q)) yield {
        val json = post(mine, others)
        Ok(s"$json\n").as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException =>
        Logger.error(s"$e", e)
        Future { BadRequest.as(applicationJsonHeader) }
      case e: Throwable => Future {
        Logger.error(s"$e", e)
        // watch tower
        Ok(s"${post(emptyResult, emptyResult)}\n").as(applicationJsonHeader)
      }
    }
  }
  private def getEdgesInner(jsValue: JsValue) = {
    getEdgesAsync(jsValue)(PostProcess.toSimpleVertexArrJson)
  }
  private def getEdgesExcludedInner(jsValue: JsValue) = {
    getEdgesExcludedAsync(jsValue)(PostProcess.toSiimpleVertexArrJson)
  }

  def getEdgesWithGrouping() = withHeaderAsync(parse.json) { request =>
    getEdgesAsync(request.body)(PostProcess.summarizeWithListFormatted)
  }

  def getEdgesExcludedWithGrouping() = withHeaderAsync(parse.json) { request =>
    getEdgesExcludedAsync(request.body)(PostProcess.summarizeWithListExcludeFormatted)
  }


  @deprecated
  def getEdgesGrouped() = withHeaderAsync(parse.json) { request =>
    getEdgesAsync(request.body)(PostProcess.summarizeWithList)
  }
  @deprecated
  def getEdgesGroupedExcluded() = withHeaderAsync(parse.json) { request =>
    try {
      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(request.body.toString)
      val q = toQuery(request.body)
      val mineQ = Query(q.vertices, List(q.steps.last))

      for (mine <- Graph.getEdgesAsync(mineQ); others <- Graph.getEdgesAsync(q)) yield {
        val json = PostProcess.summarizeWithListExclude(mine, others)
        Ok(s"$json\n").as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException => Future { BadRequest(request.body).as(applicationJsonHeader) }
      case e: Throwable => Future {
        // watch tower
        Ok(s"${PostProcess.summarizeWithListExclude(emptyResult, emptyResult)}\n").as(applicationJsonHeader)
      }
    }
  }
  @deprecated
  def getEdgesGroupedExcludedFormatted() = withHeaderAsync(parse.json) { request =>
    try {
      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(request.body.toString)
      val q = toQuery(request.body)
      val mineQ = Query(q.vertices, List(q.steps.last))

      for (mine <- Graph.getEdgesAsync(mineQ); others <- Graph.getEdgesAsync(q)) yield {
        val json = PostProcess.summarizeWithListExcludeFormatted(mine, others)
        Ok(s"$json\n").as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException => Future { BadRequest(request.body).as(applicationJsonHeader) }
      case e: Throwable => Future {
        // watch tower
        Ok(s"${PostProcess.summarizeWithListExcludeFormatted(emptyResult, emptyResult)}\n").as(applicationJsonHeader)
      }
    }
  }

  @deprecated
  def getEdge(srcId: String, tgtId: String, labelName: String, direction: String) = Action.async {
    if (!Config.IS_QUERY_SERVER) Future { Unauthorized }
    try {
      val label = HLabel.findByName(labelName).get
      val dir = Management.tryOption(direction, GraphUtil.toDir)
      val srcVertexId = toInnerVal(srcId, label.srcColumnType)
      val tgtVertexId = toInnerVal(tgtId, label.tgtColumnType)
      val src = Vertex(CompositeId(label.srcColumn.id.get, srcVertexId, true, true), System.currentTimeMillis())
      val tgt = Vertex(CompositeId(label.tgtColumn.id.get, tgtVertexId, true, false), System.currentTimeMillis())
      Graph.getEdge(src, tgt, label, dir).map { edges =>
        val ret = edges.headOption.map { edge =>
          val json = PostProcess.edgeToJson(edge, 1.0)
          Ok(s"$json\n").as(applicationJsonHeader)
        }
        ret.getOrElse(NotFound(s"NotFound\n").as(applicationJsonHeader))
      }
    } catch {
      case e: Throwable => Future { BadRequest(e.toString()).as(applicationJsonHeader) }
    }
  }
  /**
   * Vertex
   */


  def getVertices() = withHeaderAsync(parse.json) { request =>
    if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

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
      case e @ (_: play.api.libs.json.JsResultException | _: RuntimeException) =>
        Future { BadRequest(e.getMessage()).as(applicationJsonHeader) }
      case e: Throwable => Future {
        // watch tower
        Ok(s"${PostProcess.verticesToJson(Seq.empty[Vertex])}").as(applicationJsonHeader)
      }
    }
  }

  /**
   * Only for test
   */
  def testGetEdges(label: String, limit: Int, friendCntStep: Int) = withHeaderAsync { request =>
    val rId = if (friendCntStep < 0) Some(TestDataLoader.randomId) else TestDataLoader.randomId(friendCntStep)
    if (rId.isEmpty) Future { NotFound.as(applicationJsonHeader) }
    else {
      val id = rId.get
      val l = HLabel.findByName(label).get
      val srcColumnName = l.srcColumn.columnName
      val srcServiceName = HService.findById(l.srcServiceId).serviceName
      val queryJson = s"""
    {
    "srcVertices": [{"serviceName": "$srcServiceName", "columnName": "$srcColumnName", "id":$id}],
    "steps": [
      [{"label": "$label", "direction": "out", "limit": $limit}]
    ]
	}
  """
      val json = Json.parse(queryJson)
      getEdgesAsync(json)(PostProcess.simple)
    }
  }
  def testGetEdges2(label1: String, limit1: Int, label2: String, limit2: Int) = withHeaderAsync { request =>
    val id = TestDataLoader.randomId.toString
    val l = HLabel.findByName(label1).get
    val srcColumnName = l.srcColumn.columnName
    val srcServiceName = HService.findById(l.srcServiceId).serviceName
    val queryJson = s"""
    {
    "srcVertices": [{"serviceName": "$srcServiceName", "columnName": "$srcColumnName", "id":$id}],
    "steps": [
      [{"label": "$label1", "direction": "out", "limit": $limit1}],
      [{"label": "$label2", "direction": "out", "limit": $limit2}]
    ]
	}
  """
    val json = Json.parse(queryJson)
    getEdgesAsync(json)(PostProcess.simple)
  }
  def testGetEdges3(label1: String, limit1: Int, label2: String, limit2: Int, label3: String, limit3: Int) = withHeaderAsync { request =>
    val id = TestDataLoader.randomId.toString
    val l = HLabel.findByName(label1).get
    val srcColumnName = l.srcColumn.columnName
    val srcServiceName = HService.findById(l.srcServiceId).serviceName
    val queryJson = s"""
    {
    "srcVertices": [{"serviceName": "$srcServiceName", "columnName": "$srcColumnName", "id":$id}],
    "steps": [
      [{"label": "$label1", "direction": "out", "limit": $limit1}],
      [{"label": "$label2", "direction": "out", "limit": $limit2}],
      [{"label": "$label3", "direction": "out", "limit": $limit3}]
    ]
	}
  """
    val json = Json.parse(queryJson)
    getEdgesAsync(json)(PostProcess.simple)
  }

  def ping() = withHeaderAsync { requst =>
    Future { Ok("Pong\n") }
  }
}