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

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.{Query, QueryParam, ResourceManager}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class EdgeFetcherTest extends IntegrateCommon {

  import scala.collection.JavaConverters._

  test("MemoryModelFetcher") {
    // 1. create label.
    // 2. importLabel.
    // 3. fetch.
    val service = management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
    val serviceColumn =
      management.createServiceColumn("s2graph", "user", "string", Seq(Prop("age", "0", "int", true)))
    val labelName = "fetcher_test"
    val options =
      s"""{
         |
                     | "importer": {
         |   "${ResourceManager.ClassNameKey}": "org.apache.s2graph.core.utils.IdentityImporter"
         | },
         | "fetcher": {
         |   "${ResourceManager.ClassNameKey}": "org.apache.s2graph.core.fetcher.MemoryModelEdgeFetcher"
         | }
         |}""".stripMargin

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
      Seq.empty[Prop],
      isDirected = true,
      consistencyLevel =  "strong",
      hTableName = None,
      hTableTTL = None,
      schemaVersion = "v3",
      compressionAlgorithm =  "gz",
      options = Option(options)
    ).get

    graph.management.updateEdgeFetcher(label, Option(options))


    val vertex = graph.elementBuilder.toVertex(service.serviceName, serviceColumn.columnName, "daewon")
    val queryParam = QueryParam(labelName = labelName)

    val query = Query.toQuery(srcVertices = Seq(vertex), queryParams = Seq(queryParam))
    val stepResult = Await.result(graph.getEdges(query), Duration("60 seconds"))

    stepResult.edgeWithScores.foreach { es =>
      println(es.edge)
    }
  }
}
