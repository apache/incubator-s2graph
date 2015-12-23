package com.kakao.s2graph.core.rest

import java.net.URL

import com.kakao.s2graph.core.GraphExceptions.BadQueryException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{Bucket, Experiment, Service}
import com.kakao.s2graph.core.utils.logger
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Public API only return Future.successful or Future.failed
  * Don't throw exception
  */
class RestCaller(graph: Graph)(implicit ec: ExecutionContext) {
  val s2Parser = new RequestParser(graph.config)

  /**
    * Public APIS
    */
  def experiment(contentsBody: JsValue, accessToken: String, experimentName: String, uuid: String): Future[(JsValue, String)] = {
    try {
      val bucketOpt = for {
        service <- Service.findByAccessToken(accessToken)
        experiment <- Experiment.findBy(service.id.get, experimentName)
        bucket <- experiment.findBucket(uuid)
      } yield bucket

      val bucket = bucketOpt.getOrElse(throw new RuntimeException("bucket is not found"))
      if (bucket.isGraphQuery) buildRequestInner(contentsBody, bucket, uuid).map(_ -> bucket.impressionId)
      else throw new RuntimeException("not supported yet")
    } catch {
      case e: Exception => Future.failed(e)
    }
  }

  def uriMatch(uri: String, jsQuery: JsValue): Future[JsValue] = {
    try {
      uri match {
        case "/graphs/getEdges" => getEdgesAsync(jsQuery)(PostProcess.toSimpleVertexArrJson)
        case "/graphs/getEdges/grouped" => getEdgesAsync(jsQuery)(PostProcess.summarizeWithListFormatted)
        case "/graphs/getEdgesExcluded" => getEdgesExcludedAsync(jsQuery)(PostProcess.toSimpleVertexArrJson)
        case "/graphs/getEdgesExcluded/grouped" => getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExcludeFormatted)
        case "/graphs/checkEdges" => checkEdges(jsQuery)
        case "/graphs/getEdgesGrouped" => getEdgesAsync(jsQuery)(PostProcess.summarizeWithList)
        case "/graphs/getEdgesGroupedExcluded" => getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExclude)
        case "/graphs/getEdgesGroupedExcludedFormatted" => getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExcludeFormatted)
        case "/graphs/getVertices" => getVertices(jsQuery)
        case _ => throw new RuntimeException("route is not found")
      }
    } catch {
      case e: Exception => Future.failed(e)
    }
  }

  def checkEdges(jsValue: JsValue): Future[JsValue] = {
    try {
      val (quads, isReverted) = s2Parser.toCheckEdgeParam(jsValue)

      graph.checkEdges(quads).map { case queryRequestWithResultLs =>
        val edgeJsons = for {
          queryRequestWithResult <- queryRequestWithResultLs
          (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
          edgeWithScore <- queryResult.edgeWithScoreLs
          (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
          convertedEdge = if (isReverted) edge.duplicateEdge else edge
          edgeJson = PostProcess.edgeToJson(convertedEdge, score, queryRequest.query, queryRequest.queryParam)
        } yield Json.toJson(edgeJson)

        Json.toJson(edgeJsons)
      }
    } catch {
      case e: Exception => Future.failed(e)
    }
  }

  /**
    * Private APIS
    */
  private def eachQuery(post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue)(q: Query): Future[JsValue] = {
    val filterOutQueryResultsLs = q.filterOutQuery match {
      case Some(filterOutQuery) => graph.getEdges(filterOutQuery)
      case None => Future.successful(Seq.empty)
    }

    for {
      queryResultsLs <- graph.getEdges(q)
      filterOutResultsLs <- filterOutQueryResultsLs
    } yield {
      val json = post(queryResultsLs, filterOutResultsLs)
      json
    }
  }

  private def getEdgesAsync(jsonQuery: JsValue)
                           (post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue): Future[JsValue] = {

    val fetch = eachQuery(post) _
    jsonQuery match {
      case JsArray(arr) => Future.traverse(arr.map(s2Parser.toQuery(_)))(fetch).map(JsArray)
      case obj@JsObject(_) => fetch(s2Parser.toQuery(obj))
      case _ => throw BadQueryException("Cannot support")
    }
  }

  private def getEdgesExcludedAsync(jsonQuery: JsValue)
                                   (post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue): Future[JsValue] = {
    val q = s2Parser.toQuery(jsonQuery)
    val filterOutQuery = Query(q.vertices, Vector(q.steps.last))

    val fetchFuture = graph.getEdges(q)
    val excludeFuture = graph.getEdges(filterOutQuery)

    for {
      queryResultLs <- fetchFuture
      exclude <- excludeFuture
    } yield {
      post(queryResultLs, exclude)
    }
  }

  private def getVertices(jsValue: JsValue) = {
    val jsonQuery = jsValue
    val ts = System.currentTimeMillis()
    val props = "{}"

    val vertices = jsonQuery.as[List[JsValue]].flatMap { js =>
      val serviceName = (js \ "serviceName").as[String]
      val columnName = (js \ "columnName").as[String]
      for (id <- (js \ "ids").asOpt[List[JsValue]].getOrElse(List.empty[JsValue])) yield {
        Management.toVertex(ts, "insert", id.toString, serviceName, columnName, props)
      }
    }

    graph.getVertices(vertices) map { vertices => PostProcess.verticesToJson(vertices) }
  }


  private def makeRequestJson(requestKeyJsonOpt: Option[JsValue], bucket: Bucket, uuid: String): JsValue = {
    var body = bucket.requestBody.replace("#uuid", uuid)
    for {
      requestKeyJson <- requestKeyJsonOpt
      jsObj <- requestKeyJson.asOpt[JsObject]
      (key, value) <- jsObj.fieldSet
    } {
      val replacement = value match {
        case JsString(s) => s
        case _ => value.toString
      }
      body = body.replace(key, replacement)
    }

    Try(Json.parse(body)).recover {
      case e: Exception =>
        throw new BadQueryException(s"wrong or missing template parameter: ${e.getMessage.takeWhile(_ != '\n')}")
    } get
  }

  private def buildRequestInner(contentsBody: JsValue, bucket: Bucket, uuid: String): Future[JsValue] = {
    if (bucket.isEmpty) Future.successful(PostProcess.emptyResults)
    else {
      val jsonBody = makeRequestJson(Option(contentsBody), bucket, uuid)
      val url = new URL(bucket.apiPath)
      val path = url.getPath()

      // dummy log for sampling
      val experimentLog = s"POST $path took -1 ms 200 -1 $jsonBody"

      logger.info(experimentLog)

      uriMatch(path, jsonBody)
    }
  }

  def calcSize(js: JsValue): Int = js match {
    case JsObject(obj) => (js \ "size").asOpt[Int].getOrElse(0)
    case JsArray(seq) => seq.map(js => (js \ "size").asOpt[Int].getOrElse(0)).sum
    case _ => 0
  }
}
