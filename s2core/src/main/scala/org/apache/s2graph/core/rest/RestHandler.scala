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
import org.apache.s2graph.core.schema.{Bucket, Experiment, Service}
import org.apache.s2graph.core.utils.logger
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

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
class RestHandler(graph: S2GraphLike)(implicit ec: ExecutionContext) {

  import RestHandler._
  val requestParser = new RequestParser(graph)
  val querySampleRate: Double = graph.config.getDouble("query.log.sample.rate")

  /**
    * Public APIS
    */
  def doPost[A](uri: String, body: String, headers: A)(implicit ev: CanLookup[A]): HandlerResult = {
    val impKeyOpt = ev.lookup(headers, Experiment.ImpressionKey)
    val impIdOpt = ev.lookup(headers, Experiment.ImpressionId)

    try {
      val jsQuery = Json.parse(body)

      uri match {
        case "/graphs/getEdges" => HandlerResult(getEdgesAsync(jsQuery, impIdOpt)(PostProcess.toJson(Option(jsQuery))))
        case "/graphs/checkEdges" => checkEdges(jsQuery)
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
      val edges = requestParser.toCheckEdgeParam(jsValue)

      HandlerResult(graph.checkEdges(edges).map { case stepResult =>
        val jsArray = for {
          s2EdgeWithScore <- stepResult.edgeWithScores
//          json <- PostProcess.s2EdgeToJsValue(QueryOption(), s2EdgeWithScore)
          json = PostProcess.s2EdgeToJsValue(QueryOption(), s2EdgeWithScore)
        } yield json
        Json.toJson(jsArray)
      })
    } catch {
      case e: Exception =>
        logger.error(s"RestHandler#checkEdges error: $e")
        HandlerResult(Future.failed(e))
    }
  }


  /**
    * Private APIS
    */
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

  def experiment(contentsBody: JsValue, accessToken: String, experimentName: String, uuid: String, impKeyOpt: => Option[String] = None): HandlerResult = {
    try {
      val bucketOpt = for {
        service <- Service.findByAccessToken(accessToken)
        experiment <- Experiment.findBy(service.id.get, experimentName)
        bucket <- experiment.findBucket(uuid, impKeyOpt)
      } yield bucket

      val bucket = bucketOpt.getOrElse(throw new RuntimeException(s"bucket is not found. $accessToken, $experimentName, $uuid, $impKeyOpt"))
      if (bucket.isGraphQuery) {
        val ret = buildRequestInner(contentsBody, bucket, uuid)

        logQuery(Json.obj(
          "type" -> "experiment",
          "time" -> System.currentTimeMillis(),
          "body" -> contentsBody,
          "uri" -> Seq("graphs", "experiment", accessToken, experimentName, uuid).mkString("/"),
          "accessToken" -> accessToken,
          "experimentName" -> experimentName,
          "uuid" -> uuid,
          "impressionId" -> bucket.impressionId
        ))

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
                   (post: (S2GraphLike, QueryOption, StepResult) => JsValue): Future[JsValue] = {

    def query(obj: JsValue): Future[JsValue] = {
      (obj \ "queries").asOpt[JsValue] match {
        case None =>
          val s2Query = requestParser.toQuery(obj, impIdOpt)
          graph.getEdges(s2Query).map(post(graph, s2Query.queryOption, _))
        case _ =>
          val multiQuery = requestParser.toMultiQuery(obj, impIdOpt)
          graph.getEdgesMultiQuery(multiQuery).map(post(graph, multiQuery.queryOption, _))
      }
    }

    logQuery(Json.obj(
      "type" -> "getEdges",
      "time" -> System.currentTimeMillis(),
      "body" -> jsonQuery,
      "uri" -> "graphs/getEdges"
    ))

    val unionQuery = (jsonQuery \ "union").asOpt[JsObject]
    unionQuery match {
      case None => jsonQuery match {
        case obj@JsObject(_) => query(obj)
        case JsArray(arr) =>
          val res = arr.map(js => query(js.as[JsObject]))
          Future.sequence(res).map(JsArray)
        case _ => throw BadQueryException("Cannot support")
      }

      case Some(jsUnion) =>
        val (keys, queries) = jsUnion.value.unzip
        val futures = queries.map(query)
        Future.sequence(futures).map(res => JsObject(keys.zip(res).toSeq))
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
        graph.toVertex(serviceName, columnName, id)
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
      val escaped = Json.stringify(value)
      val replacement = value match {
        case _: JsString => escaped.slice(1, escaped.length - 1)
        case _ => escaped
      }

      body = body.replace(key, replacement)
    }

    body
  }

  def calcSize(js: JsValue): Int =
    (js \\ "size") map { sizeJs => sizeJs.asOpt[Int].getOrElse(0) } sum

  def logQuery(queryJson: => JsObject): Unit = {
    if (scala.util.Random.nextDouble() < querySampleRate) {
      logger.query(queryJson.toString)
    }
  }
}
