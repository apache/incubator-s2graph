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

package org.apache.s2graph.s2jobs.loader

import java.io.{File, PrintWriter}
import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.util.ToolRunner
import org.apache.s2graph.core.{Management, PostProcess, S2Graph, S2VertexLike}
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.mysqls.{Label, ServiceColumn}
import org.apache.s2graph.core.storage.CanSKeyValue
import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.s2jobs.S2GraphHelper
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import play.api.libs.json.Json

import scala.io.Source
import scala.util.Try

object GraphFileGeneratorTest {
  def initTestEdgeSchema(s2: S2Graph, tableName: String,
                         schemaVersion: String = HBaseType.DEFAULT_VERSION,
                         compressionAlgorithm: String = "none"): Label = {
    import scala.collection.JavaConverters._
    /* initialize model for test */
    val management = s2.management

    val service = management.createService(serviceName = "s2graph", cluster = "localhost",
      hTableName = "s2graph", preSplitSize = -1, hTableTTL = -1, compressionAlgorithm = "gz")

    val serviceColumn = management.createServiceColumn(service.serviceName, "user", "string", Nil)

    Try {
      management.createLabel("friends", serviceColumn, serviceColumn, isDirected = true,
        serviceName = service.serviceName, indices = new java.util.ArrayList[Index],
        props = Seq(Prop("since", "0", "long"), Prop("score", "0", "integer")).asJava, consistencyLevel = "strong", hTableName = tableName,
        hTableTTL = -1, schemaVersion = schemaVersion, compressionAlgorithm = compressionAlgorithm, options = "")
    }

    Label.findByName("friends").getOrElse(throw new IllegalArgumentException("friends label is not initialized."))
  }

  def initTestVertexSchema(s2: S2Graph): ServiceColumn = {
    import scala.collection.JavaConverters._
    /* initialize model for test */
    val management = s2.management

    val service = management.createService(serviceName = "device_profile", cluster = "localhost",
      hTableName = "s2graph", preSplitSize = -1, hTableTTL = -1, compressionAlgorithm = "gz")

    management.createServiceColumn(service.serviceName, "imei", "string",
      Seq(
        Prop(name = "first_time", defaultValue = "''", dataType = "string"),
        Prop(name = "last_time", defaultValue = "''", dataType = "string"),
        Prop(name = "total_active_days", defaultValue = "-1", dataType = "integer"),
        Prop(name = "query_amount", defaultValue = "-1", dataType = "integer"),
        Prop(name = "active_months", defaultValue = "-1", dataType = "integer"),
        Prop(name = "fua", defaultValue = "''", dataType = "string"),
        Prop(name = "location_often_province", defaultValue = "''", dataType = "string"),
        Prop(name = "location_often_city", defaultValue = "''", dataType = "string"),
        Prop(name = "location_often_days", defaultValue = "-1", dataType = "integer"),
        Prop(name = "location_last_province", defaultValue = "''", dataType = "string"),
        Prop(name = "location_last_city", defaultValue = "''", dataType = "string"),
        Prop(name = "fimei_legality", defaultValue = "-1", dataType = "integer")
      ))
  }

  def writeToFile(fileName: String)(lines: Seq[String]): Unit = {
    val writer = new PrintWriter(fileName)
    lines.foreach(line => writer.write(line + "\n"))
    writer.close
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete) throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}

class GraphFileGeneratorTest extends FunSuite with Matchers with BeforeAndAfterAll {
  import GraphFileGeneratorTest._
  import scala.collection.JavaConverters._

  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _
  val options = GraphFileOptions(
    input = "/tmp/imei-20.txt",
    tempDir = "/tmp/bulkload_tmp",
    output = "/tmp/s2graph_bulkload",
    zkQuorum = "localhost",
    dbUrl = "jdbc:h2:file:./var/metastore;MODE=MYSQL",
    dbUser = "sa",
    dbPassword = "sa",
    dbDriver = "org.h2.Driver",
    tableName = "s2graph",
    maxHFilePerRegionServer = 1,
    numRegions = 3,
    compressionAlgorithm = "NONE",
    buildDegree = false,
    autoEdgeCreate = false)

  val s2Config = Management.toConfig(options.toConfigParams)

  val tableName = options.tableName
  val schemaVersion = HBaseType.DEFAULT_VERSION
  val compressionAlgorithm: String = options.compressionAlgorithm
  var s2: S2Graph = _

  override def beforeAll(): Unit = {
    // initialize spark context.
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)

    s2 = S2GraphHelper.initS2Graph(s2Config)
  }

  override def afterAll(): Unit = {
    if (sc != null) sc.stop()
    if (s2 != null) s2.shutdown()
  }

  test("test generateKeyValues edge only.") {
    import scala.collection.JavaConverters._
    import org.apache.s2graph.core.storage.CanSKeyValue._

    val label = initTestEdgeSchema(s2, tableName, schemaVersion, compressionAlgorithm)
    /* end of initialize model */

    val bulkEdgeString = "1416236400000\tinsert\tedge\ta\tb\tfriends\t{\"since\":1316236400000,\"score\":10}"
    val input = sc.parallelize(Seq(bulkEdgeString))

    val kvs = HFileGenerator.transfer(sc, s2Config, input, options)

    val ls = kvs.map(kv => CanSKeyValue.hbaseKeyValue.toSKeyValue(kv)).collect().toList

    val serDe = s2.defaultStorage.serDe

    val bulkEdge = s2.elementBuilder.toGraphElement(bulkEdgeString, options.labelMapping).get

    val indexEdges = ls.flatMap { kv =>
      serDe.indexEdgeDeserializer(label.schemaVersion).fromKeyValues(Seq(kv), None)
    }

    val indexEdge = indexEdges.head

    println(indexEdge)
    println(bulkEdge)

    bulkEdge shouldBe(indexEdge)
  }


  test("test generateKeyValues vertex only.") {
    val serviceColumn = initTestVertexSchema(s2)
    val bulkVertexString = "20171201\tinsert\tvertex\t800188448586078\tdevice_profile\timei\t{\"first_time\":\"20171025\",\"last_time\":\"20171112\",\"total_active_days\":14,\"query_amount\":1526.0,\"active_months\":2,\"fua\":\"M5+Note\",\"location_often_province\":\"广东省\",\"location_often_city\":\"深圳市\",\"location_often_days\":6,\"location_last_province\":\"广东省\",\"location_last_city\":\"深圳市\",\"fimei_legality\":3}"
    val bulkVertex = s2.elementBuilder.toGraphElement(bulkVertexString, options.labelMapping).get

    val input = sc.parallelize(Seq(bulkVertexString))

    val kvs = HFileGenerator.transfer(sc, s2Config, input, options)

    val ls = kvs.map(kv => CanSKeyValue.hbaseKeyValue.toSKeyValue(kv)).collect().toList

    val serDe = s2.defaultStorage.serDe

    val vertex = serDe.vertexDeserializer(serviceColumn.schemaVersion).fromKeyValues(ls, None).get

    PostProcess.s2VertexToJson(vertex).foreach { jsValue =>
      println(Json.prettyPrint(jsValue))
    }

    bulkVertex shouldBe(vertex)
  }

  test("test generateHFile vertex only.") {
    val serviceColumn = initTestVertexSchema(s2)
//    val lines = Source.fromFile("/tmp/imei-20.txt").getLines().toSeq
    val bulkVertexString = "20171201\tinsert\tvertex\t800188448586078\ts2graph\timei\t{\"first_time\":\"20171025\",\"last_time\":\"20171112\",\"total_active_days\":14,\"query_amount\":1526.0,\"active_months\":2,\"fua\":\"M5+Note\",\"location_often_province\":\"广东省\",\"location_often_city\":\"深圳市\",\"location_often_days\":6,\"location_last_province\":\"广东省\",\"location_last_city\":\"深圳市\",\"fimei_legality\":3}"
    val lines = Seq(bulkVertexString)
    val input = sc.parallelize(lines)

    val kvs = HFileGenerator.transfer(sc, s2Config, input, options)
    println(kvs.count())
  }

  // this test case expect options.input already exist with valid bulk load format.
  test("bulk load and fetch vertex: spark mode") {
    val serviceColumn = initTestVertexSchema(s2)

    deleteRecursively(new File(options.tempDir))
    deleteRecursively(new File(options.output))

    val bulkVertexLs = Source.fromFile(options.input).getLines().toSeq
    val input = sc.parallelize(bulkVertexLs)

    HFileGenerator.generate(sc, s2Config, input, options)

    val hfileArgs = Array(options.output, options.tableName)
    val hbaseConfig = HBaseConfiguration.create()

    val ret = ToolRunner.run(hbaseConfig, new LoadIncrementalHFiles(hbaseConfig), hfileArgs)

    val s2Vertices = s2.vertices().asScala.toSeq.map(_.asInstanceOf[S2VertexLike])
    val json = PostProcess.verticesToJson(s2Vertices)

    println(Json.prettyPrint(json))
  }

  // this test case expect options.input already exist with valid bulk load format.
  test("bulk load and fetch vertex: mr mode") {
    val serviceColumn = initTestVertexSchema(s2)

    deleteRecursively(new File(options.tempDir))
    deleteRecursively(new File(options.output))

    val bulkVertexLs = Source.fromFile(options.input).getLines().toSeq
    val input = sc.parallelize(bulkVertexLs)

    HFileMRGenerator.generate(sc, s2Config, input, options)

    val hfileArgs = Array(options.output, options.tableName)
    val hbaseConfig = HBaseConfiguration.create()

    val ret = ToolRunner.run(hbaseConfig, new LoadIncrementalHFiles(hbaseConfig), hfileArgs)
    val s2Vertices = s2.vertices().asScala.toSeq.map(_.asInstanceOf[S2VertexLike])
    val json = PostProcess.verticesToJson(s2Vertices)

    println(Json.prettyPrint(json))
  }
}
