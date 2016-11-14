/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.rest

import java.net.URL

import org.apache.s2graph.core.GraphExceptions.{BadQueryException, LabelNotExistException}
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Bucket, Experiment, Service}
import org.apache.s2graph.core.utils.logger
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}

object RestHandler {
  trait CanLookup[A] {
    def lookup(m: A, key: String): Option[String]
  }

  object CanLookup {
   implicit val oneTupleLookup = new CanLookup[(String, String)] {
      override def lookup(m: (String, String), key: String) =
        if (m._1 == key) Option(m._2) else None
    }
    implicit val hashMapLookup = new CanLookup[Map[String, String]] {
      override def lookup(m: Map[String, String], key: String): Option[String] = m.get(key)
    }
  }

  case class HandlerResult(body: Future[JsValue], headers: (String, String)*)
}

/**
  * Public API, only return Future.successful or Future.failed
  * Don't throw exception
  */
class RestHandler(graph: Graph)(implicit ec: ExecutionContext) {

  import RestHandler._
  val requestParser = new RequestParser(graph)


  /**
    * Public APIS
    */
  def doPost[A](uri: String, body: String, headers: A)(implicit ev: CanLookup[A]): HandlerResult = {
    val impKeyOpt = ev.lookup(headers, Experiment.ImpressionKey)
    val impIdOpt = ev.lookup(headers, Experiment.ImpressionId)

    try {
      val jsQuery = Json.parse(body)

      uri match {
//        case "/graphs/getEdges" => HandlerResult(getEdgesAsync(jsQuery, impIdOpt)(PostProcess.toSimpleVertexArrJson))
        case "/graphs/getEdges" => HandlerResult(getEdgesAsync(jsQuery, impIdOpt)(PostProcess.toJson))
//        case "/graphs/getEdges/grouped" => HandlerResult(getEdgesAsync(jsQuery)(PostProcess.summarizeWithListFormatted))
//        case "/graphs/getEdgesExcluded" => HandlerResult(getEdgesExcludedAsync(jsQuery)(PostProcess.toSimpleVertexArrJson))
//        case "/graphs/getEdgesExcluded/grouped" => HandlerResult(getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExcludeFormatted))
        case "/graphs/checkEdges" => checkEdges(jsQuery)
//        case "/graphs/getEdgesGrouped" => HandlerResult(getEdgesAsync(jsQuery)(PostProcess.summarizeWithList))
//        case "/graphs/getEdgesGroupedExcluded" => HandlerResult(getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExclude))
//        case "/graphs/getEdgesGroupedExcludedFormatted" => HandlerResult(getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExcludeFormatted))
        case "/graphs/getVertices" => HandlerResult(getVertices(jsQuery))
        case "/graphs/experiments" => experiments(jsQuery)
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

      HandlerResult(graph.checkEdges(quads).map { case stepResult =>
        PostProcess.toJson(graph, QueryOption(), stepResult)
      })
    } catch {
      case e: Exception => HandlerResult(Future.failed(e))
    }
  }


  private def experiments(jsQuery: JsValue): HandlerResult = {
    val params: Seq[RequestParser.ExperimentParam] = requestParser.parseExperiment(jsQuery)

    val results = params map { case (body, token, experimentName, uuid, impKeyOpt) =>
      val handlerResult = experiment(body, token, experimentName, uuid, impKeyOpt)
      val future = handlerResult.body.recover {
        case e: Exception => PostProcess.emptyResults ++ Json.obj("error" -> Json.obj("reason" -> e.getMessage))
      }

      future
    }

    val result = Future.sequence(results).map(JsArray)
    HandlerResult(body = result)
  }

  private def experiment(contentsBody: JsValue, accessToken: String, experimentName: String, uuid: String, impKeyOpt: => Option[String] = None): HandlerResult = {
    try {
      val bucketOpt = for {
        service <- Service.findByAccessToken(accessToken)
        experiment <- Experiment.findBy(service.id.get, experimentName)
        bucket <- experiment.findBucket(uuid, impKeyOpt)
      } yield bucket

      val bucket = bucketOpt.getOrElse(throw new RuntimeException("bucket is not found"))
      if (bucket.isGraphQuery) {
        val ret = buildRequestInner(contentsBody, bucket, uuid)
        HandlerResult(ret.body, Experiment.ImpressionKey -> bucket.impressionId)
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

      doPost(path, body, Experiment.ImpressionId -> bucket.impressionId)
    }
  }

  def getEdgesAsync(jsonQuery: JsValue, impIdOpt: Option[String] = None)
                   (post: (Graph, QueryOption, StepResult) => JsValue): Future[JsValue] = {
    jsonQuery match {
      case obj@JsObject(_) =>
        (obj \ "queries").asOpt[JsValue] match {
          case None =>
            val query = requestParser.toQuery(obj, impIdOpt)
            graph.getEdges(query).map(post(graph, query.queryOption, _))
          case _ =>
            val multiQuery = requestParser.toMultiQuery(obj, impIdOpt)
            graph.getEdgesMultiQuery(multiQuery).map(post(graph, multiQuery.queryOption, _))
        }

      case JsArray(arr) =>
        val queries = arr.map(requestParser.toQuery(_, impIdOpt))
        val weights = queries.map(_ => 1.0)
        val multiQuery = MultiQuery(queries, weights, QueryOption(), jsonQuery)
        graph.getEdgesMultiQuery(multiQuery).map(post(graph, multiQuery.queryOption, _))

      case _ => throw BadQueryException("Cannot support")
    }
  }

  private def getVertices(jsValue: JsValue) = {
    val jsonQuery = jsValue

    val vertices = jsonQuery.as[List[JsValue]].flatMap { js =>
      val serviceName = (js \ "serviceName").as[String]
      val columnName = (js \ "columnName").as[String]
      for {
        idJson <- (js \ "ids").asOpt[List[JsValue]].getOrElse(List.empty[JsValue])
        id <- jsValueToAny(idJson)
      } yield {
        Vertex.toVertex(serviceName, columnName, id)
      }
    }

    graph.getVertices(vertices) map { vertices => PostProcess.verticesToJson(vertices) }
  }


  private def buildRequestBody(requestKeyJsonOpt: Option[JsValue], bucket: Bucket, uuid: String): String = {
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

    body
  }

  def calcSize(js: JsValue): Int = js match {
    case JsObject(obj) => (js \ "size").asOpt[Int].getOrElse(0)
    case JsArray(seq) => seq.map(js => (js \ "size").asOpt[Int].getOrElse(0)).sum
    case _ => 0
  }
}
