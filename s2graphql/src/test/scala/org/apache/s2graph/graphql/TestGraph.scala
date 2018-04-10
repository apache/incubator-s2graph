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

import com.typesafe.config.Config
import org.apache.s2graph
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core.mysqls.{Label, Model, Service}
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.{Management, S2Graph}
import org.apache.s2graph.graphql
import org.apache.s2graph.graphql.repository.GraphRepository
import org.apache.s2graph.graphql.types.SchemaDef
import play.api.libs.json._
import sangria.ast.Document
import sangria.execution.Executor
import sangria.execution.deferred.DeferredResolver
import sangria.renderer.SchemaRenderer
import sangria.schema.Schema

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util._

object TestGraph {

}

trait TestGraph {

  def open(): Unit

  def cleanup(): Unit

  def repository: GraphRepository

  def schema: Schema[GraphRepository, Any]

  def showSchema: String

  import GraphRepository._

  val resolver: DeferredResolver[GraphRepository] = DeferredResolver.fetchers(vertexFetcher, edgeFetcher)

  def queryAsJs(query: Document): JsValue = {
    implicit val playJsonMarshaller = sangria.marshalling.playJson.PlayJsonResultMarshaller
    Await.result(
      Executor.execute(schema, query, repository, deferredResolver = resolver),
      Duration("10 sec")
    )
  }

  def queryAsRaw(query: Document, graph: TestGraph): Any = {
    Await.result(
      Executor.execute(schema, query, repository, deferredResolver = resolver),
      Duration("10 sec")
    )
  }
}

class EmptyGraph(config: Config) extends TestGraph {
  Model.apply(config)

  lazy val graph = new S2Graph(config)(scala.concurrent.ExecutionContext.Implicits.global)
  lazy val management = new Management(graph)
  lazy val s2Repository = new GraphRepository(graph)

  override def cleanup(): Unit = graph.shutdown(true)

  override def schema: Schema[GraphRepository, Any] = new SchemaDef(s2Repository).S2GraphSchema

  override def showSchema: String = SchemaRenderer.renderSchema(schema)

  override def repository: GraphRepository = s2Repository

  override def open(): Unit = {
    Model.shutdown(true)
  }

}

class BasicGraph(config: Config) extends EmptyGraph(config) {
  // Init test data
  val serviceName = "kakao"
  val labelName = "friends"
  val columnName = "user"

  Management.deleteService(serviceName)
  val serviceTry: Try[Service] =
    management.createService(
      serviceName,
      "localhost",
      s"${serviceName}_table",
      1,
      None
    )

  val serviceColumnTry = serviceTry.map { _ =>
    management.createServiceColumn(
      serviceName,
      columnName,
      "string",
      List(
        Prop("age", "0", "int"),
        Prop("gender", "", "string")
      )
    )
  }

  Management.deleteLabel(labelName)
  val labelTry: Try[Label] =
    management.createLabel(
      labelName,
      serviceName, columnName, "string",
      serviceName, columnName, "string",
      true, serviceName,
      Nil,
      Seq(Prop("score", "0", "int")),
      "strong"
    )
}
