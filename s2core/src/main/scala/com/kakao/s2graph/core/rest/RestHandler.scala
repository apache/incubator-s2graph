package com.kakao.s2graph.core.rest

import java.net.URL

import com.kakao.s2graph.core.GraphExceptions.BadQueryException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{Bucket, Experiment, Service}
import com.kakao.s2graph.core.utils.logger
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


object RestHandler {
  case class HandlerResult(body: Future[JsValue], headers: (String, String)*)
}

/**
  * Public API, only return Future.successful or Future.failed
  * Don't throw exception
  */
class RestHandler(graph: Graph)(implicit ec: ExecutionContext) {

  import RestHandler._
  val requestParser = new RequestParser(graph.config)

  /**
    * Public APIS
    */
  def doPost(uri: String, body: String, impKeyOpt: => Option[String] = None): HandlerResult = {
    try {
      val jsQuery = Json.parse(body)

      uri match {
        case "/graphs/getEdges" => HandlerResult(getEdgesAsync(jsQuery)(PostProcess.toSimpleVertexArrJson))
        case "/graphs/getEdges/grouped" => HandlerResult(getEdgesAsync(jsQuery)(PostProcess.summarizeWithListFormatted))
        case "/graphs/getEdgesExcluded" => HandlerResult(getEdgesExcludedAsync(jsQuery)(PostProcess.toSimpleVertexArrJson))
        case "/graphs/getEdgesExcluded/grouped" => HandlerResult(getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExcludeFormatted))
        case "/graphs/checkEdges" => checkEdges(jsQuery)
        case "/graphs/getEdgesGrouped" => HandlerResult(getEdgesAsync(jsQuery)(PostProcess.summarizeWithList))
        case "/graphs/getEdgesGroupedExcluded" => HandlerResult(getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExclude))
        case "/graphs/getEdgesGroupedExcludedFormatted" => HandlerResult(getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExcludeFormatted))
        case "/graphs/getVertices" => HandlerResult(getVertices(jsQuery))
        case uri if uri.startsWith("/graphs/experiment") =>
          val Array(accessToken, experimentName, uuid) = uri.split("/").takeRight(3)
          experiment(jsQuery, accessToken, experimentName, uuid, impKeyOpt)
        case _ => throw new RuntimeException("route is not found")
      }
    } catch {
      case e: Exception => HandlerResult(Future.failed(e))
    }
  }

  // TODO: Refactor to doGet
  def checkEdges(jsValue: JsValue): HandlerResult = {
    try {
      val (quads, isReverted) = requestParser.toCheckEdgeParam(jsValue)

      HandlerResult(graph.checkEdges(quads).map { case queryRequestWithResultLs =>
        val edgeJsons = for {
          queryRequestWithResult <- queryRequestWithResultLs
          (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
          edgeWithScore <- queryResult.edgeWithScoreLs
          (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
          convertedEdge = if (isReverted) edge.duplicateEdge else edge
          edgeJson = PostProcess.edgeToJson(convertedEdge, score, queryRequest.query, queryRequest.queryParam)
        } yield Json.toJson(edgeJson)

        Json.toJson(edgeJsons)
      })
    } catch {
      case e: Exception => HandlerResult(Future.failed(e))
    }
  }


  /**
    * Private APIS
    */
  private def experiment(contentsBody: JsValue, accessToken: String, experimentName: String, uuid: String, impKeyOpt: => Option[String]): HandlerResult = {

    try {
      val bucketOpt = for {
        service <- Service.findByAccessToken(accessToken)
        experiment <- Experiment.findBy(service.id.get, experimentName)
        bucket <- experiment.findBucket(uuid, impKeyOpt)
      } yield bucket

      val bucket = bucketOpt.getOrElse(throw new RuntimeException("bucket is not found"))
      if (bucket.isGraphQuery) {
        val ret = buildRequestInner(contentsBody, bucket, uuid)
        HandlerResult(ret.body, Experiment.impressionKey -> bucket.impressionId)
      }
      else throw new RuntimeException("not supported yet")
    } catch {
      case e: Exception => HandlerResult(Future.failed(e))
    }
  }

  private def buildRequestInner(contentsBody: JsValue, bucket: Bucket, uuid: String): HandlerResult = {
    if (bucket.isEmpty) HandlerResult(Future.successful(PostProcess.emptyResults))
    else {
      val body = buildRequestBody(Option(contentsBody), bucket, uuid)
      val url = new URL(bucket.apiPath)
      val path = url.getPath

      // dummy log for sampling
      val experimentLog = s"POST $path took -1 ms 200 -1 $body"
      logger.debug(experimentLog)

      doPost(path, body)
    }
  }

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

  def getEdgesAsync(jsonQuery: JsValue)
                   (post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue): Future[JsValue] = {

    val fetch = eachQuery(post) _
    jsonQuery match {
      case JsArray(arr) => Future.traverse(arr.map(requestParser.toQuery(_)))(fetch).map(JsArray)
      case obj@JsObject(_) =>
        (obj \ "queries").asOpt[JsValue] match {
          case None => fetch(requestParser.toQuery(obj))
          case _ =>
            val multiQuery = requestParser.toMultiQuery(obj)
            val filterOutFuture = multiQuery.queryOption.filterOutQuery match {
              case Some(filterOutQuery) => graph.getEdges(filterOutQuery)
              case None => Future.successful(Seq.empty)
            }
            val futures = multiQuery.queries.zip(multiQuery.weights).map { case (query, weight) =>
              val filterOutQueryResultsLs = query.queryOption.filterOutQuery match {
                case Some(filterOutQuery) => graph.getEdges(filterOutQuery)
                case None => Future.successful(Seq.empty)
              }
              for {
                queryRequestWithResultLs <- graph.getEdges(query)
                filterOutResultsLs <- filterOutQueryResultsLs
              } yield {
                val newQueryRequestWithResult = for {
                  queryRequestWithResult <- queryRequestWithResultLs
                  queryResult = queryRequestWithResult.queryResult
                } yield {
                  val newEdgesWithScores = for {
                    edgeWithScore <- queryRequestWithResult.queryResult.edgeWithScoreLs
                  } yield {
                    edgeWithScore.copy(score = edgeWithScore.score * weight)
                  }
                  queryRequestWithResult.copy(queryResult = queryResult.copy(edgeWithScoreLs = newEdgesWithScores))
                }
                logger.debug(s"[Size]: ${newQueryRequestWithResult.map(_.queryResult.edgeWithScoreLs.size).sum}")
                (newQueryRequestWithResult, filterOutResultsLs)
              }
            }
            for {
              filterOut <- filterOutFuture
              resultWithExcludeLs <- Future.sequence(futures)
            } yield {
              PostProcess.toSimpleVertexArrJsonMulti(multiQuery.queryOption, resultWithExcludeLs, filterOut)
              //              val initial = (ListBuffer.empty[QueryRequestWithResult], ListBuffer.empty[QueryRequestWithResult])
              //              val (results, excludes) = resultWithExcludeLs.foldLeft(initial) { case ((prevResults, prevExcludes), (results, excludes)) =>
              //                (prevResults ++= results, prevExcludes ++= excludes)
              //              }
              //              PostProcess.toSimpleVertexArrJson(multiQuery.queryOption, results, excludes ++ filterOut)
            }
        }
      case _ => throw BadQueryException("Cannot support")
    }
  }

  private def getEdgesExcludedAsync(jsonQuery: JsValue)
                                   (post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue): Future[JsValue] = {
    val q = requestParser.toQuery(jsonQuery)
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

  private def buildRequestBody(requestKeyJsonOpt: Option[JsValue], bucket: Bucket, uuid: String): String = {
    var body = bucket.requestBody.replace("#uuid", uuid)

    //    // replace variable
    //    body = TemplateHelper.replaceVariable(System.currentTimeMillis(), body)

    // replace param
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

    body
  }

  def calcSize(js: JsValue): Int = js match {
    case JsObject(obj) => (js \ "size").asOpt[Int].getOrElse(0)
    case JsArray(seq) => seq.map(js => (js \ "size").asOpt[Int].getOrElse(0)).sum
    case _ => 0
  }
}
