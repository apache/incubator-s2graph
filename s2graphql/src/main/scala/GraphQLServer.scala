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

package org.apache.s2graph

import java.util.concurrent.Executors

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.utils.SafeUpdateCache
import sangria.ast.Document
import sangria.execution._
import sangria.marshalling.sprayJson._
import sangria.parser.QueryParser
import sangria.renderer.SchemaRenderer
import sangria.schema.Schema
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object GraphQLServer {

  // Init s2graph
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread * 2)

  implicit val ec = ExecutionContext.fromExecutor(threadPool)

  val config = ConfigFactory.load()
  val s2graph = new S2Graph(config)
  val schemaCacheTTL = Try(config.getInt("schemaCacheTTL")).getOrElse(-1)
  val s2Repository = new GraphRepository(s2graph)
  val schemaCache = new SafeUpdateCache[Schema[GraphRepository, Any]]("schema", maxSize = 1, ttl = schemaCacheTTL)

  def endpoint(requestJSON: spray.json.JsValue)(implicit e: ExecutionContext): Route = {

    val spray.json.JsObject(fields) = requestJSON
    val spray.json.JsString(query) = fields("query")

    val operation = fields.get("operationName") collect {
      case spray.json.JsString(op) => op
    }

    val vars = fields.get("variables") match {
      case Some(obj: spray.json.JsObject) => obj
      case _ => spray.json.JsObject.empty
    }

    QueryParser.parse(query) match {
      case Success(queryAst) => complete(executeGraphQLQuery(queryAst, operation, vars))
      case Failure(error) => complete(BadRequest -> spray.json.JsObject("error" -> JsString(error.getMessage)))
    }
  }

  /**
    * In development mode(schemaCacheTTL = -1),
    * a new schema is created for each request.
    */
  println(s"schemaCacheTTL: ${schemaCacheTTL}")

  private def createNewSchema(): Schema[GraphRepository, Any] = {
    println(s"Schema updated: ${System.currentTimeMillis()}")

    val s2Type = new S2Type(s2Repository)
    val newSchema = new SchemaDef(s2Type).S2GraphSchema

    println(SchemaRenderer.renderSchema(newSchema))
    println("-" * 80)

    newSchema
  }

  private def executeGraphQLQuery(query: Document, op: Option[String], vars: JsObject)(implicit e: ExecutionContext) = {
    val s2schema = schemaCache.withCache("s2Schema")(createNewSchema())

    Executor.execute(
      s2schema,
      query,
      s2Repository,
      variables = vars,
      operationName = op
    )
      .map((res: spray.json.JsValue) => OK -> res)
      .recover {
        case error: QueryAnalysisError => BadRequest -> error.resolveError
        case error: ErrorWithResolver => InternalServerError -> error.resolveError
      }
  }
}
