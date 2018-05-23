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

package org.apache.s2graph.core.fetcher.sql

import org.apache.s2graph.core.Management.JsonModel._
import org.apache.s2graph.core._
import org.apache.s2graph.core.fetcher.BaseFetcherTest
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.storage.jdbc._

import scala.concurrent._
import scala.concurrent.duration._

class JdbcStorageTest extends BaseFetcherTest {
  implicit lazy val ec = graph.ec

  val edgeFetcherName = classOf[JdbcEdgeFetcher].getName
  val edgeMutatorName = classOf[JdbcEdgeMutator].getName

  val options =
    s"""
       |{
       |  "fetcher": {
       |    "className": "${edgeFetcherName}",
       |    "url": "jdbc:h2:file:/tmp/s2graph/metastore;MODE=MYSQL",
       |    "driver": "org.h2.Driver",
       |    "password": "sa",
       |    "user": "sa"
       |  },
       |  "mutator": {
       |    "className": "${edgeMutatorName}",
       |    "url": "jdbc:h2:file:/tmp/s2graph/metastore;MODE=MYSQL",
       |    "driver": "org.h2.Driver",
       |    "password": "sa",
       |    "user": "sa"
       |  }
       |}
       """.stripMargin

  val serviceName = "s2graph"
  val columnName = "user"

  var service: Service = _
  var serviceColumn: ServiceColumn = _

  override def beforeAll: Unit = {
    super.beforeAll

    service = management.createService(serviceName, "localhost", "s2graph_htable", -1, None).get
    serviceColumn = management.createServiceColumn(serviceName, columnName, "string", Nil)
  }

  def clearLabel(labelName: String): Unit = {
    Label.findByName(labelName, useCache = false).foreach { label =>
      label.labelMetaSet.foreach { lm =>
        LabelMeta.delete(lm.id.get)
      }
      Label.delete(label.id.get)
    }
  }

  def createLabel(labelName: String,
                  consistencyLevel: String,
                  props: Seq[Prop],
                  indices: Seq[Index]): Label = {

    clearLabel(labelName)

    val label = management.createLabel(
      labelName,
      service.serviceName, serviceColumn.columnName, serviceColumn.columnType,
      service.serviceName, serviceColumn.columnName, serviceColumn.columnType,
      service.serviceName,
      indices,
      props,
      isDirected = true, consistencyLevel = consistencyLevel, hTableName = None, hTableTTL = None,
      schemaVersion = "v3", compressionAlgorithm = "gz", options = Option(options),
      initFetcherWithOptions = true
    ).get

    management.updateEdgeFetcher(label, Option(options))
    label
  }

  def normalise(s: String): String = s.split("\n").map(_.trim).filterNot(_.isEmpty).mkString("\n")

  test("CreateLabel - label A: {strong, defaultIndex}") {
    val props = Seq(
      Prop(name = "score", defaultValue = "0.0", dataType = "double"),
      Prop(name = "age", defaultValue = "0", dataType = "int")
    )
    val indices = Nil

    val label = createLabel("A", "strong", props, indices)

    val expectedColumn = Seq("_timestamp", "_from", "_to", "age", "score")
    val actualColumn = JdbcStorage.affectedColumns(label)

    actualColumn shouldBe expectedColumn

    val expectedSchema =
      """
    CREATE TABLE `_EDGE_STORE_A`(
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `_timestamp` TIMESTAMP NOT NULL default CURRENT_TIMESTAMP,
      `_from` varchar(256) NOT NULL,
      `_to` varchar(256) NOT NULL,
      PRIMARY KEY (`id`),
      `age` int(32),
      `score` double,
      KEY `A__PK` (`_timestamp`),
      UNIQUE KEY `A_from` (`_from`,`_to`),
      UNIQUE KEY `A_to` (`_to`,`_from`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    val actualSchema = JdbcStorage.showSchema(label)
    println(actualSchema)

    normalise(actualSchema) shouldBe normalise(expectedSchema)
  }

  test("CreateLabel - label B: {strong, (age, score) index}") {
    val props = Seq(
      Prop(name = "score", defaultValue = "0.0", dataType = "double"),
      Prop(name = "age", defaultValue = "0", dataType = "int")
    )
    val indices = Seq(
      Index("idx_age_score", Seq("age", "score"))
    )

    val label = createLabel("B", "strong", props, indices)

    val expectedColumn = Seq("_timestamp", "_from", "_to", "age", "score")
    val actualColumn = JdbcStorage.affectedColumns(label)

    actualColumn shouldBe expectedColumn

    val expectedSchema =
      """
      CREATE TABLE `_EDGE_STORE_B`(
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `_timestamp` TIMESTAMP NOT NULL default CURRENT_TIMESTAMP,
        `_from` varchar(256) NOT NULL,
        `_to` varchar(256) NOT NULL,
        PRIMARY KEY (`id`),
        `age` int(32),
        `score` double,
        KEY `B_idx_age_score` (`age`, `score`),
        UNIQUE KEY `B_from` (`_from`,`_to`),
        UNIQUE KEY `B_to` (`_to`,`_from`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
      """

    val actualSchema = JdbcStorage.showSchema(label)
    normalise(actualSchema) shouldBe normalise(expectedSchema)
  }

  test("CreateLabel - label C: {weak, defaultIndex}") {
    val props = Seq(
      Prop(name = "score", defaultValue = "0.0", dataType = "double"),
      Prop(name = "age", defaultValue = "0", dataType = "int")
    )
    val indices = Nil

    val label = createLabel("C", "weak", props, indices)

    val expectedColumn = Seq("_timestamp", "_from", "_to", "age", "score")
    val actualColumn = JdbcStorage.affectedColumns(label)

    actualColumn shouldBe expectedColumn

    val expectedSchema =
      """
      CREATE TABLE `_EDGE_STORE_C`(
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `_timestamp` TIMESTAMP NOT NULL default CURRENT_TIMESTAMP,
        `_from` varchar(256) NOT NULL,
        `_to` varchar(256) NOT NULL,
        PRIMARY KEY (`id`),
        `age` int(32),
        `score` double,
        KEY `C__PK` (`_timestamp`),
        KEY `C_from` (`_from`,`_to`),
        KEY `C_to` (`_to`,`_from`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
      """

    val actualSchema = JdbcStorage.showSchema(label)
    normalise(actualSchema) shouldBe normalise(expectedSchema)
  }

  test("Mutate and fetch edges - label A") {
    val label = Label.findByName("A", useCache = false).get

    JdbcStorage.dropTable(label)
    JdbcStorage.createTable(label)

    val fetcher = graph.getEdgeFetcher(label)
    val mutator = graph.getEdgeMutator(label)

    val edgeElric =
      graph.toEdge("daewon", "elric", label.label, "out", Map("score" -> 90), ts = 10)

    val edgeRain =
      graph.toEdge("daewon", "rain", label.label, "out", Map("score" -> 50), ts = 5)

    val edgeShon =
      graph.toEdge("daewon", "shon", label.label, "out", Map("score" -> 100), ts = 15)

    val edgeElricUpdated =
      graph.toEdge("daewon", "elric", label.label, "out", Map("score" -> 200), ts = 20)

    val insertEdges = Seq(edgeShon, edgeElric, edgeRain)
    val updateEdges = Seq(edgeElricUpdated)

    Await.ready(mutator.mutateStrongEdges("", insertEdges, true), Duration("10 sec"))
    Await.ready(mutator.mutateStrongEdges("", updateEdges, true), Duration("10 sec"))

    val qp = QueryParam(labelName = label.label, offset = 0, limit = 10)
    val vertex = edgeElric.srcVertex
    val step = Step(Seq(qp))
    val q = Query(Seq(vertex), steps = Vector(step))
    val qr = QueryRequest(q, 0, vertex, qp)

    val fetchedEdges = Await.result(
      fetcher.fetches(Seq(qr), Map.empty), Duration("10 sec")).flatMap(_.edgeWithScores.map(_.edge)
    )

    fetchedEdges shouldBe Seq(edgeElricUpdated, edgeShon, edgeRain) // order by timestamp desc
  }

  test("Mutate and fetch edges - label C") {
    val label = Label.findByName("C", useCache = false).get

    JdbcStorage.dropTable(label)
    JdbcStorage.createTable(label)

    val fetcher = graph.getEdgeFetcher(label)
    val mutator = graph.getEdgeMutator(label)

    val edgeShon =
      graph.toEdge("daewon", "shon", label.label, "out", Map("score" -> 90), ts = 10)

    val edgeShon2 =
      graph.toEdge("daewon", "shon", label.label, "out", Map("score" -> 50), ts = 5)

    val insertEdges = Seq(edgeShon, edgeShon2)

    Await.ready(mutator.mutateStrongEdges("", insertEdges, true), Duration("10 sec"))

    val qp = QueryParam(labelName = label.label, offset = 0, limit = 10, duplicatePolicy = DuplicatePolicy.Raw)
    val vertex = edgeShon.srcVertex
    val step = Step(Seq(qp))
    val q = Query(Seq(vertex), steps = Vector(step))
    val qr = QueryRequest(q, 0, vertex, qp)

    val fetchedEdges = Await.result(
      fetcher.fetches(Seq(qr), Map.empty), Duration("10 sec")).flatMap(_.edgeWithScores.map(_.edge)
    )

    fetchedEdges shouldBe Seq(edgeShon, edgeShon2)
  }
}
