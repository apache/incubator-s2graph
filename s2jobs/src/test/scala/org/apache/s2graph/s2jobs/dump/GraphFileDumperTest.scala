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

//package org.apache.s2graph.s2jobs.dump
//
//import org.apache.s2graph.core._
//import org.apache.s2graph.core.types.HBaseType
//import org.apache.s2graph.s2jobs.S2GraphHelper
//import org.apache.s2graph.s2jobs.loader.GraphFileOptions
//import org.apache.spark.{SparkConf, SparkContext}
//import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
//import play.api.libs.json.Json
//
//class GraphFileDumperTest extends FunSuite with Matchers with BeforeAndAfterAll {
//  private val master = "local[2]"
//  private val appName = "example-spark"
//
//  private var sc: SparkContext = _
//  val options = GraphFileOptions(
//    input = "/tmp/imei-20.txt",
//    tempDir = "/tmp/bulkload_tmp",
//    output = "/tmp/s2graph_bulkload",
//    zkQuorum = "localhost",
//    dbUrl = "jdbc:h2:file:./var/metastore;MODE=MYSQL",
//    dbUser = "sa",
//    dbPassword = "sa",
//    dbDriver = "org.h2.Driver",
//    tableName = "s2graph",
//    maxHFilePerRegionServer = 1,
//    numRegions = 3,
//    compressionAlgorithm = "NONE",
//    buildDegree = false,
//    autoEdgeCreate = false)
//
//  val s2Config = Management.toConfig(options.toConfigParams)
//
//  val tableName = options.tableName
//  val schemaVersion = HBaseType.DEFAULT_VERSION
//  val compressionAlgorithm: String = options.compressionAlgorithm
//  var s2: S2Graph = _
//
//  override def beforeAll(): Unit = {
//    // initialize spark context.
//    val conf = new SparkConf()
//      .setMaster(master)
//      .setAppName(appName)
//
//    sc = new SparkContext(conf)
//
//    s2 = S2GraphHelper.initS2Graph(s2Config)
//  }
//
//  override def afterAll(): Unit = {
//    if (sc != null) sc.stop()
//    if (s2 != null) s2.shutdown()
//  }
//
//  test("test dump.") {
//    implicit val graph = s2
//    val snapshotPath = "/usr/local/var/hbase"
//    val restorePath = "/tmp/hbase_restore"
//    val snapshotTableNames = Seq("s2graph-snapshot")
//
//    val cellLs = HFileDumper.toKeyValue(sc, snapshotPath, restorePath,
//      snapshotTableNames, columnFamily = "v")
//
//    val kvsLs = cellLs.map(CanGraphElement.cellsToSKeyValues).collect()
//
//    val elements = kvsLs.flatMap { kvs =>
//      CanGraphElement.sKeyValueToGraphElement(s2)(kvs)
//    }
//
//    elements.foreach { element =>
//      val v = element.asInstanceOf[S2VertexLike]
//      val json = Json.prettyPrint(PostProcess.s2VertexToJson(v).get)
//
//      println(json)
//    }
//
//  }
//}
