package controllers


import com.codahale.metrics.Meter

//import com.daumkakao.s2graph.core.mysqls._

import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.types2.{LabelWithDirection, VertexId, TargetVertexId, SourceVertexId}
import com.daumkakao.s2graph.rest.config.{Instrumented, Config}
import play.api.Logger

import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, Controller, Result}
import util.TestDataLoader
import scala.concurrent._

object QueryController extends Controller with RequestParser with Instrumented {

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

  private def getEdgesAsync(jsonQuery: JsValue)(post: (Seq[Iterable[(Edge, Double)]], Query) => JsValue): Future[Result] = {
    try {
      val queryTemplateId = (jsonQuery \ "steps").toString()
      getOrElseUpdateMetric(queryTemplateId)(metricRegistry.meter(queryTemplateId)).mark()

      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(s"$jsonQuery")
      val q = toQuery(jsonQuery)
      Logger.info(s"$q")
      val future = Graph.getEdgesAsync(q)
      future map { edges =>
        val json = post(edges, q)
        Ok(s"${json}\n").as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException =>
        Logger.error(s"$e", e)
        Future {
          BadRequest.as(applicationJsonHeader)
        }
      case e: Throwable => Future {
        Logger.error(s"$e", e)
        // watch tower
        Ok(s"\n").as(applicationJsonHeader)
      }
    }
  }

  private def getEdgesExcludedAsync(jsonQuery: JsValue)(post: (Seq[Iterable[(Edge, Double)]],
    Seq[Iterable[(Edge, Double)]], Query) => JsValue): Future[Result] = {
    try {
      val queryTemplateId = (jsonQuery \ "steps").toString()
      getOrElseUpdateMetric[Meter](queryTemplateId)(metricRegistry.meter(queryTemplateId)).mark()

      if (!Config.IS_QUERY_SERVER) Unauthorized.as(applicationJsonHeader)

      Logger.info(s"$jsonQuery")
      val q = toQuery(jsonQuery)
      val mineQ = Query(q.vertices, List(q.steps.last))

      for (mine <- Graph.getEdgesAsync(mineQ); others <- Graph.getEdgesAsync(q)) yield {
        val json = post(mine, others, q)
        Ok(s"$json\n").as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException =>
        Logger.error(s"$e", e)
        Future {
          BadRequest.as(applicationJsonHeader)
        }
      case e: Throwable => Future {
        Logger.error(s"$e", e)
        // watch tower
        Ok(s"\n").as(applicationJsonHeader)
      }
    }
  }

  private def getEdgesInner(jsonQuery: JsValue) = {
    getEdgesAsync(jsonQuery)(PostProcess.toSimpleVertexArrJson)
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
      case e: KGraphExceptions.BadQueryException => Future {
        BadRequest(request.body).as(applicationJsonHeader)
      }
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
        val json = PostProcess.summarizeWithListExcludeFormatted(mine, others, q)
        Ok(s"$json\n").as(applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.BadQueryException => Future {
        BadRequest(request.body).as(applicationJsonHeader)
      }
      case e: Throwable => Future {
        // watch tower
        Ok(s"\n").as(applicationJsonHeader)
      }
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
          var srcVertex = Vertex(VertexId(label.srcColumnWithDir(direction.toInt).id.get, srcId))
          var tgtVertex = Vertex(VertexId(label.tgtColumnWithDir(direction.toInt).id.get, tgtId))
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
      Graph.checkEdges(quads).map { case edgesWithScores =>
        val edgeJsons = for {
          edgesWithScore <- edgesWithScores
          (edge, score) <- edgesWithScore
          edgeJson <- PostProcess.edgeToJson(if (isReverted) edge.duplicateEdge else edge, score, Query())
        } yield edgeJson

        Ok(Json.toJson(edgeJsons)).as(applicationJsonHeader)
      }
    } catch {
      case e: Throwable => Future.successful(BadRequest(e.toString()).as(applicationJsonHeader))
    }
  }

  def checkEdges() = withHeaderAsync(parse.json) { request =>
    if (!Config.IS_QUERY_SERVER) Future.successful(Unauthorized)
    checkEdgesInner(request.body)
  }

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
      case e@(_: play.api.libs.json.JsResultException | _: RuntimeException) =>
        Future {
          BadRequest(e.getMessage()).as(applicationJsonHeader)
        }
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
    if (rId.isEmpty) Future {
      NotFound.as(applicationJsonHeader)
    }
    else {
      val id = rId.get
      val l = Label.findByName(label).get
      val srcColumnName = l.srcColumn.columnName
      val srcServiceName = Service.findById(l.srcServiceId).serviceName
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
    val l = Label.findByName(label1).get
    val srcColumnName = l.srcColumn.columnName
    val srcServiceName = Service.findById(l.srcServiceId).serviceName
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
    val l = Label.findByName(label1).get
    val srcColumnName = l.srcColumn.columnName
    val srcServiceName = Service.findById(l.srcServiceId).serviceName
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
    Future {
      Ok("Pong\n")
    }
  }
}