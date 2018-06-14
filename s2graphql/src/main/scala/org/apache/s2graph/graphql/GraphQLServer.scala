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

package org.apache.s2graph.graphql

import java.util.concurrent.Executors

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.utils.SafeUpdateCache
import org.apache.s2graph.graphql.repository.GraphRepository
import org.apache.s2graph.graphql.types.SchemaDef
import org.slf4j.LoggerFactory
import sangria.ast.Document
import sangria.execution._
import sangria.execution.deferred.DeferredResolver
import sangria.marshalling.sprayJson._
import sangria.parser.{QueryParser, SyntaxError}
import sangria.schema.Schema
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object GraphQLServer {
  val className = Schema.getClass.getName
  val logger = LoggerFactory.getLogger(this.getClass)

  // Init s2graph
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread * 2)

  implicit val ec = ExecutionContext.fromExecutor(threadPool)

  val config = ConfigFactory.load()
  val s2graph = new S2Graph(config)
  val schemaCacheTTL = Try(config.getInt("schemaCacheTTL")).getOrElse(-1)
  val s2Repository = new GraphRepository(s2graph)
  val schemaConfig = ConfigFactory.parseMap(Map(
    SafeUpdateCache.MaxSizeKey -> 1, SafeUpdateCache.TtlKey -> schemaCacheTTL
  ).asJava)

  val schemaCache = new SafeUpdateCache(schemaConfig)

  def updateEdgeFetcher(requestJSON: spray.json.JsValue)(implicit e: ExecutionContext): Try[Unit] = {
    val ret = Try {
      val spray.json.JsObject(fields) = requestJSON
      val spray.json.JsString(labelName) = fields("label")
      val jsOptions = fields("options")

      s2graph.management.updateEdgeFetcher(labelName, jsOptions.compactPrint)
    }

    ret
  }

  /**
    * In development mode(schemaCacheTTL = -1),
    * a new schema is created for each request.
    */
  logger.info(s"schemaCacheTTL: ${schemaCacheTTL}")

  private def createNewSchema(): Schema[GraphRepository, Any] = {
    val newSchema = new SchemaDef(s2Repository).S2GraphSchema
    logger.info(s"Schema updated: ${System.currentTimeMillis()}")

    newSchema
  }

  def formatError(error: Throwable): JsValue = error match {
    case syntaxError: SyntaxError ⇒
      JsObject("errors" → JsArray(
        JsObject(
          "message" → JsString(syntaxError.getMessage),
          "locations" → JsArray(JsObject(
            "line" → JsNumber(syntaxError.originalError.position.line),
            "column" → JsNumber(syntaxError.originalError.position.column))))))

    case NonFatal(e) ⇒ formatError(e.toString)
    case e ⇒ throw e
  }

  def formatError(message: String): JsObject =
    JsObject("errors" → JsArray(JsObject("message" → JsString(message))))

  def onEvictSchema(o: AnyRef): Unit = {
    logger.info("Schema Evicted")
  }

  def executeGraphQLQuery(query: Document, op: Option[String], vars: JsObject)(implicit e: ExecutionContext) = {
    import GraphRepository._

    val cacheKey = className + "s2Schema"
    val s2schema = schemaCache.withCache(cacheKey, broadcast = false, onEvict = onEvictSchema)(createNewSchema())
    val resolver: DeferredResolver[GraphRepository] = DeferredResolver.fetchers(vertexFetcher, edgeFetcher)

    Executor.execute(
      s2schema,
      query,
      s2Repository,
      variables = vars,
      operationName = op,
      deferredResolver = resolver
    ).map((res: spray.json.JsValue) => OK -> res)
      .recover {
        case error: QueryAnalysisError =>
          logger.error("Error on execute", error)
          BadRequest -> error.resolveError
        case error: ErrorWithResolver =>
          logger.error("Error on execute", error)
          InternalServerError -> error.resolveError
      }
  }
}

