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
package org.apache.s2graph.loader.subscriber

import java.util

import org.apache.s2graph.core.Management
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.mysqls.Label
import org.apache.s2graph.core.storage.CanSKeyValue
import org.apache.s2graph.core.types.HBaseType
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.Try

class TransferToHFileTest extends FunSuite with Matchers with BeforeAndAfterAll {

  import TransferToHFile._

  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _

  /* TransferHFile parameters */
  val options = GraphFileOptions(
    zkQuorum = "localhost",
    dbUrl = "jdbc:h2:file:./var/metastore;MODE=MYSQL",
    dbUser = "sa",
    dbPassword = "sa",
    tableName = "s2graph",
    maxHFilePerRegionServer = 1,
    compressionAlgorithm = "gz",
    buildDegree = false,
    autoEdgeCreate = false)

  val s2Config = Management.toConfig(options.toConfigParams)

  val tableName = options.tableName
  val schemaVersion = HBaseType.DEFAULT_VERSION
  val compressionAlgorithm: String = options.compressionAlgorithm

  override def beforeAll(): Unit = {
    // initialize spark context.
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)

    GraphSubscriberHelper.apply(s2Config)
  }

  override def afterAll(): Unit = {
    GraphSubscriberHelper.g.shutdown()
    if (sc != null) {
      sc.stop()
    }
  }


  test("test TransferToHFile Local.") {
    import scala.collection.JavaConverters._
    import org.apache.s2graph.core.storage.CanSKeyValue._

    /* initialize model for test */
    val management = GraphSubscriberHelper.management

    val service = management.createService(serviceName = "s2graph", cluster = "localhost",
      hTableName = "s2graph", preSplitSize = -1, hTableTTL = -1, compressionAlgorithm = "gz")

    val serviceColumn = management.createServiceColumn(service.serviceName, "user", "string", new util.ArrayList[Prop]())

    Try {
      management.createLabel("friends", serviceColumn, serviceColumn, isDirected = true,
        serviceName = service.serviceName, indices = new java.util.ArrayList[Index],
        props = Seq(Prop("since", "0", "long"), Prop("score", "0", "integer")).asJava, consistencyLevel = "strong", hTableName = tableName,
        hTableTTL = -1, schemaVersion = schemaVersion, compressionAlgorithm = compressionAlgorithm, options = "")
    }

    val label = Label.findByName("friends").getOrElse(throw new IllegalArgumentException("friends label is not initialized."))
    /* end of initialize model */

    val bulkEdgeString = "1416236400000\tinsert\tedge\ta\tb\tfriends\t{\"since\":1316236400000,\"score\":10}"
    val input = sc.parallelize(Seq(bulkEdgeString))

    val kvs = TransferToHFile.generateKeyValues(sc, s2Config, input, options)

    val ls = kvs.map(kv => CanSKeyValue.hbaseKeyValue.toSKeyValue(kv)).collect().toList

    val serDe = GraphSubscriberHelper.g.defaultStorage.serDe

    //    val snapshotEdgeOpt = serDe.indexEdgeDeserializer(label.schemaVersion).fromKeyValues(Seq(ls.head), None)
    //    val indexEdgeOpt = serDe.indexEdgeDeserializer(label.schemaVersion).fromKeyValues(Seq(ls.last), None)

    val bulkEdge = GraphSubscriberHelper.g.elementBuilder.toGraphElement(bulkEdgeString, options.labelMapping).get

    val indexEdges = ls.flatMap { kv =>
      serDe.indexEdgeDeserializer(label.schemaVersion).fromKeyValues(Seq(kv), None)
    }

    val indexEdge = indexEdges.head

    bulkEdge shouldBe(indexEdge)
  }
}
