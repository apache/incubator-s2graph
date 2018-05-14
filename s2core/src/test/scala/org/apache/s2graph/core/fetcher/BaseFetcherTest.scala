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

package org.apache.s2graph.core.fetcher

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema.{Label, Service, ServiceColumn}
import org.scalatest._

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

trait BaseFetcherTest extends FunSuite with Matchers with BeforeAndAfterAll {
  var graph: S2Graph = _
  var parser: RequestParser = _
  var management: Management = _
  var config: Config = _

  override def beforeAll = {
    config = ConfigFactory.load()
    graph = new S2Graph(config)(ExecutionContext.Implicits.global)
    management = new Management(graph)
    parser = new RequestParser(graph)
  }

  override def afterAll(): Unit = {
    graph.shutdown()
  }

  def queryEdgeFetcher(service: Service,
                       serviceColumn: ServiceColumn,
                       label: Label,
                       srcVertices: Seq[String]): StepResult = {

    val vertices = srcVertices.map(graph.elementBuilder.toVertex(service.serviceName, serviceColumn.columnName, _))

    val queryParam = QueryParam(labelName = label.label, limit = 10)

    val query = Query.toQuery(srcVertices = vertices, queryParams = Seq(queryParam))
    Await.result(graph.getEdges(query), Duration("60 seconds"))
  }

  def initEdgeFetcher(serviceName: String,
                      columnName: String,
                      labelName: String,
                      options: Option[String]): (Service, ServiceColumn, Label) = {
    val service = management.createService(serviceName, "localhost", "s2graph_htable", -1, None).get
    val serviceColumn =
      management.createServiceColumn(serviceName, columnName, "string", Nil)

    Label.findByName(labelName, useCache = false).foreach { label => Label.delete(label.id.get) }

    val label = management.createLabel(
      labelName,
      service.serviceName,
      serviceColumn.columnName,
      serviceColumn.columnType,
      service.serviceName,
      serviceColumn.columnName,
      serviceColumn.columnType,
      service.serviceName,
      Seq.empty[Index],
      Seq(Prop(name = "score", defaultValue = "0.0", dataType = "double")),
      isDirected = true,
      consistencyLevel =  "strong",
      hTableName = None,
      hTableTTL = None,
      schemaVersion = "v3",
      compressionAlgorithm =  "gz",
      options = options
    ).get

    management.updateEdgeFetcher(label, options)

    (service, serviceColumn, Label.findById(label.id.get, useCache = false))
  }
}
