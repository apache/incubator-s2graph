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

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.graphql.middleware.{GraphFormatted}
import org.apache.s2graph.core.{S2GraphLike}
import org.apache.s2graph.core.utils.SafeUpdateCache
import org.apache.s2graph.graphql.repository.GraphRepository
import org.apache.s2graph.graphql.types.SchemaDef
import org.slf4j.LoggerFactory
import sangria.ast.Document
import sangria.execution._
import sangria.execution.deferred.DeferredResolver
import sangria.marshalling.sprayJson._
import sangria.parser.{SyntaxError}
import sangria.schema.Schema
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext}
import scala.util.control.NonFatal
import scala.util._

object GraphQLServer {
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
}

class GraphQLServer(s2graph: S2GraphLike, schemaCacheTTL: Int = 60) {
  val logger = LoggerFactory.getLogger(this.getClass)

  val schemaConfig = ConfigFactory.parseMap(Map(
    SafeUpdateCache.MaxSizeKey -> 1, SafeUpdateCache.TtlKey -> schemaCacheTTL
  ).asJava)

  // Manage schema instance lifecycle
  val schemaCache = new SafeUpdateCache(schemaConfig)(s2graph.ec)

  def updateEdgeFetcher(requestJSON: spray.json.JsValue)(implicit e: ExecutionContext): Try[Unit] = {
    val ret = Try {
      val spray.json.JsObject(fields) = requestJSON
      val spray.json.JsString(labelName) = fields("label")
      val jsOptions = fields("options")

      s2graph.management.updateEdgeFetcher(labelName, jsOptions.compactPrint)
    }

    ret
  }

  val schemaCacheKey = Schema.getClass.getName + "s2Schema"

  schemaCache.put(schemaCacheKey, createNewSchema(true))

  /**
    * In development mode(schemaCacheTTL = 1),
    * a new schema is created for each request.
    */

  private def createNewSchema(withAdmin: Boolean): (SchemaDef, GraphRepository) = {
    logger.info(s"Schema update start")

    val ts = System.currentTimeMillis()

    val s2Repository = new GraphRepository(s2graph)
    val newSchema = new SchemaDef(s2Repository, withAdmin)

    logger.info(s"Schema updated: ${(System.currentTimeMillis() - ts) / 1000} sec")

    newSchema -> s2Repository
  }

  def onEvictSchema(o: AnyRef): Unit = {
    logger.info("Schema Evicted")
  }

  val TransformMiddleWare = List(org.apache.s2graph.graphql.middleware.Transform())

  def executeQuery(query: Document, op: Option[String], vars: JsObject)(implicit e: ExecutionContext) = {
    import GraphRepository._

    val (schemaDef, s2Repository) =
      schemaCache.withCache(schemaCacheKey, broadcast = false, onEvict = onEvictSchema)(createNewSchema(true))

    val resolver: DeferredResolver[GraphRepository] = DeferredResolver.fetchers(vertexFetcher, edgeFetcher)

    val includeGrpaph = vars.fields.get("includeGraph").contains(spray.json.JsBoolean(true))
    val middleWares = if (includeGrpaph) GraphFormatted :: TransformMiddleWare else TransformMiddleWare

    Executor.execute(
      schemaDef.schema,
      query,
      s2Repository,
      variables = vars,
      operationName = op,
      deferredResolver = resolver,
      middleware = middleWares
    )
  }

}

