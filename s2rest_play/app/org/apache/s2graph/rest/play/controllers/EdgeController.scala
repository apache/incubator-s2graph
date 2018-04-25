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

package org.apache.s2graph.rest.play.controllers

import com.fasterxml.jackson.databind.JsonMappingException
import org.apache.s2graph.core.ExceptionHandler.KafkaMessage
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.storage.{IncrementResponse, MutateResponse}
import org.apache.s2graph.rest.play.actors.QueueActor
import org.apache.s2graph.rest.play.config.Config
import play.api.libs.json._
import play.api.mvc.{Controller, Result}

import scala.collection.Seq
import scala.concurrent.Future

object EdgeController extends Controller {

  import ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._

  private val s2: S2Graph = org.apache.s2graph.rest.play.Global.s2graph
  private val requestParser: RequestParser = org.apache.s2graph.rest.play.Global.s2parser
  private val walLogHandler: ExceptionHandler = org.apache.s2graph.rest.play.Global.wallLogHandler

  private def enqueue(topic: String, elem: GraphElement, tsv: String, publishJson: Boolean = false) = {
    val kafkaMessage = ExceptionHandler.toKafkaMessage(topic, elem, Option(tsv), publishJson)
    walLogHandler.enqueue(kafkaMessage)
  }

  private def publish(graphElem: GraphElement, tsv: String) = {
    val kafkaTopic = toKafkaTopic(graphElem.isAsync)

    graphElem match {
      case v: S2VertexLike =>
        enqueue(kafkaTopic, graphElem, tsv)
      case e: S2EdgeLike =>
        e.innerLabel.extraOptions.get("walLog") match {
          case None =>
            enqueue(kafkaTopic, e, tsv)
          case Some(walLogOpt) =>
            (walLogOpt \ "method").get match {
              case JsString("drop") => // pass
              case JsString("sample") =>
                val rate = (walLogOpt \ "rate").as[Double]
                if (scala.util.Random.nextDouble() < rate) {
                  enqueue(kafkaTopic, e, tsv)
                }
              case _ =>
                enqueue(kafkaTopic, e, tsv)
            }
        }
      case _ => logger.error(s"Unknown graph element type: ${graphElem}")
    }
  }

  private def toDeleteAllFailMessages(srcVertices: Seq[S2VertexLike], labels: Seq[Label], dir: Int, ts: Long ) = {
    for {
      vertex <- srcVertices
      id = vertex.id.toString
      label <- labels
    } yield {
      val tsv = Seq(ts, "deleteAll", "e", id, id, label.label, "{}", GraphUtil.fromOp(dir.toByte)).mkString("\t")
      ExceptionHandler.toKafkaMessage(Config.KAFKA_MUTATE_FAIL_TOPIC, tsv)
    }
  }

  private def publishFailTopic(kafkaMessages: Seq[KafkaMessage]): Unit ={
    kafkaMessages.foreach(walLogHandler.enqueue)
  }

  def mutateElementsWithFailLog(elements: Seq[(GraphElement, String)]) ={
    val result = s2.mutateElements(elements.map(_._1), true)
    result onComplete { results =>
      results.get.zip(elements).map {
        case (r: MutateResponse, (e: S2EdgeLike, tsv: String)) if !r.isSuccess =>
          val kafkaMessages = if(e.getOp() == GraphUtil.operations("deleteAll")){
            toDeleteAllFailMessages(Seq(e.srcVertex), Seq(e.innerLabel), e.getDir(), e.ts)
          } else{
            Seq(ExceptionHandler.toKafkaMessage(Config.KAFKA_MUTATE_FAIL_TOPIC, e, Some(tsv)))
          }
          publishFailTopic(kafkaMessages)
        case _ =>
      }
    }
    result
  }

  private def tryMutate(elementsWithTsv: Seq[(GraphElement, String)], withWait: Boolean): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)
    else {
      elementsWithTsv.foreach { case (graphElem, tsv) =>
        publish(graphElem, tsv)
      }

      if (elementsWithTsv.isEmpty) Future.successful(jsonResponse(JsArray()))
      else {
        val elementWithIdxs = elementsWithTsv.zipWithIndex
        if (withWait) {
          val (elementSync, elementAsync) = elementWithIdxs.partition { case ((element, tsv), idx) =>
            !skipElement(element.isAsync)
          }
          val retToSkip = elementAsync.map(_._2 -> MutateResponse.Success)
          val elementsToStore = elementSync.map(_._1)
          val elementsIdxToStore = elementSync.map(_._2)
          mutateElementsWithFailLog(elementsToStore).map { rets =>
            elementsIdxToStore.zip(rets) ++ retToSkip
          }.map { rets =>
            Json.toJson(rets.sortBy(_._1).map(_._2.isSuccess))
          }.map(jsonResponse(_))
        } else {
          val rets = elementWithIdxs.map { case ((element, tsv), idx) =>
            if (!skipElement(element.isAsync)) QueueActor.router ! ((element, tsv))
            true
          }
          Future.successful(jsonResponse(Json.toJson(rets)))
        }
      }
    }
  }

  def mutateJsonFormat(jsValue: JsValue, operation: String, withWait: Boolean = false): Future[Result] = {
    logger.debug(s"$jsValue")

    try {
      val edgesWithTsv = requestParser.parseJsonFormat(jsValue, operation)
      tryMutate(edgesWithTsv, withWait)
    } catch {
      case e: JsonMappingException =>
        logger.malformed(jsValue, e)
        Future.successful(BadRequest(s"${e.getMessage}"))
      case e: GraphExceptions.JsonParseException  =>
        logger.malformed(jsValue, e)
        Future.successful(BadRequest(s"${e.getMessage}"))
      case e: Exception =>
        logger.malformed(jsValue, e)
        Future.failed(e)
    }
  }

  def mutateBulkFormat(str: String, withWait: Boolean = false): Future[Result] = {
    logger.debug(s"$str")

    try {
      val elementsWithTsv = requestParser.parseBulkFormat(str)
      tryMutate(elementsWithTsv, withWait)
    } catch {
      case e: JsonMappingException =>
        logger.malformed(str, e)
        Future.successful(BadRequest(s"${e.getMessage}"))
      case e: GraphExceptions.JsonParseException  =>
        logger.malformed(str, e)
        Future.successful(BadRequest(s"${e.getMessage}"))
      case e: Exception =>
        logger.malformed(str, e)
        Future.failed(e)
    }
  }

  def mutateBulk() = withHeaderAsync(parse.text) { request =>
    mutateBulkFormat(request.body, withWait = false)
  }

  def mutateBulkWithWait() = withHeaderAsync(parse.text) { request =>
    mutateBulkFormat(request.body, withWait = true)
  }

  def inserts() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "insert")
  }

  def insertsWithWait() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "insert", withWait = true)
  }

  def insertsBulk() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "insertBulk")
  }

  def deletes() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "delete")
  }

  def deletesWithWait() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "delete", withWait = true)
  }

  def updates() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "update")
  }

  def updatesWithWait() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "update", withWait = true)
  }

  def increments() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "increment")
  }

  def incrementsWithWait() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "increment", withWait = true)
  }

  def incrementCounts() = withHeaderAsync(jsonParser) { request =>
    val jsValue = request.body
    val edgesWithTsv = requestParser.parseJsonFormat(jsValue, "incrementCount")

    val edges = for {
      (e, _tsv) <- edgesWithTsv if !skipElement(e.isAsync)
    } yield e

    if (edges.isEmpty) Future.successful(jsonResponse(JsArray()))
    else {

      s2.incrementCounts(edges, withWait = true).map { results =>
        val json = results.map { case IncrementResponse(isSuccess, afterCount, beforeCount) =>
          Json.obj("success" -> isSuccess, "result" -> afterCount, "_count" -> beforeCount)
        }

        jsonResponse(Json.toJson(json))
      }
    }
  }

  def deleteAll() = withHeaderAsync(jsonParser) { request =>
    deleteAllInner(request.body, withWait = true)
  }

  def deleteAllWithOutWait() = withHeaderAsync(jsonParser) { request =>
    deleteAllInner(request.body, withWait = false)
  }

  def deleteAllInner(jsValue: JsValue, withWait: Boolean) = {

    /* logging for delete all request */
    def enqueueLogMessage(ids: Seq[JsValue], labels: Seq[Label], ts: Long, direction: String, topicOpt: Option[String]) = {
      val kafkaMessages = for {
        id <- ids
        label <- labels
      } yield {
        val tsv = Seq(ts, "deleteAll", "e", RequestParser.jsToStr(id), RequestParser.jsToStr(id), label.label, "{}", direction).mkString("\t")
        val topic = topicOpt.getOrElse { toKafkaTopic(label.isAsync) }

        ExceptionHandler.toKafkaMessage(topic, tsv)
      }

      kafkaMessages.foreach(walLogHandler.enqueue)
    }

    def deleteEach(labels: Seq[Label], direction: String, ids: Seq[JsValue],
                   ts: Long, vertices: Seq[S2VertexLike]) = {

      val future = s2.deleteAllAdjacentEdges(vertices.toList, labels, GraphUtil.directions(direction), ts)
      if (withWait) {
        future onComplete {
          case ret =>
            if (!ret.get) {
              val messages = toDeleteAllFailMessages(vertices.toList, labels, GraphUtil.directions(direction), ts)
              publishFailTopic(messages)
            }
        }
        future
      } else {
        Future.successful(true)
      }
    }

    val deleteFutures = jsValue.as[Seq[JsValue]].map { json =>
      val (_labels, direction, ids, ts, vertices) = requestParser.toDeleteParam(json)
      val srcVertices = vertices
      enqueueLogMessage(ids, _labels, ts, direction, None)
      val labels = _labels.filterNot(e => skipElement(e.isAsync))

      if (labels.isEmpty || ids.isEmpty) Future.successful(true)
      else deleteEach(labels, direction, ids, ts, srcVertices)
    }

    val deleteResults = Future.sequence(deleteFutures)
    deleteResults.map { rst =>
      logger.debug(s"deleteAllInner: $rst")
      Ok(s"deleted... ${rst.toString()}")
    }
  }
}
