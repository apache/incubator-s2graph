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

package org.apache.s2graph.s2jobs.task

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.s2graph.core.{GraphUtil, S2EdgeLike, S2VertexLike}
import org.apache.s2graph.core.storage.hbase.{AsynchbaseStorage, AsynchbaseStorageManagement}
import org.apache.s2graph.s2jobs.serde.reader.RowBulkFormatReader
import org.apache.s2graph.s2jobs.serde.writer.{S2EdgeDataFrameWriter, S2VertexDataFrameWriter}
import org.apache.s2graph.s2jobs.{BaseSparkTest, S2GraphHelper}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class SourceTest extends BaseSparkTest {
  //TODO: props to valid string.
  def s2VertexToDataFrame(vertices: Seq[String]): DataFrame = {
    import spark.sqlContext.implicits._
    val elements = vertices.flatMap(s2.elementBuilder.toVertex(_))

    elements.map { v =>
      (v.ts, GraphUtil.fromOp(v.op),
        "v", v.innerId.toIdString(), v.serviceName, v.columnName, "{}")
    }.toDF(S2VertexDataFrameWriter.Fields: _*)
  }

  def s2EdgeToDataFrame(edges: Seq[String]): DataFrame = {
    import spark.sqlContext.implicits._
    val elements = edges.flatMap(s2.elementBuilder.toEdge(_))

    elements.map { e =>
      (e.getTs(),
        e.getOperation(),
        "e",
        e.srcVertex.innerIdVal.toString,
        e.tgtVertex.innerIdVal.toString,
        e.label(),
        "{}",
        e.getDirection())
    }.toDF(S2EdgeDataFrameWriter.Fields: _*)
  }

  test("S2GraphSource edge toDF") {
    val label = initTestEdgeSchema(s2, tableName, schemaVersion, compressionAlgorithm)
    val snapshotTableName = options.tableName + "-snapshot"

    // 1. run S2GraphSink to build(not actually load by using LoadIncrementalLoad) bulk load file.
    val bulkEdges = Seq(
      "1416236400000\tinsert\tedge\ta\tb\tfriends\t{\"since\":1316236400000,\"score\":10}",
      "1416236400000\tinsert\tedge\ta\tc\tfriends\t{\"since\":1316236400000,\"score\":10}"
    )
    val df = s2EdgeToDataFrame(bulkEdges)

    val reader = new RowBulkFormatReader

    val inputEdges = df.collect().flatMap(reader.read(s2)(_))
      .sortBy(_.asInstanceOf[S2EdgeLike].tgtVertex.innerId.toIdString())

    val args = options.toCommand.grouped(2).map { kv =>
      kv.head -> kv.last
    }.toMap ++ Map("writeMethod" -> "bulk", "runLoadIncrementalHFiles" -> "true")

    val conf = TaskConf("test", "sql", Seq("input"), args)

    val sink = new S2GraphSink("testQuery", conf)
    sink.write(df)

    // 2. create snapshot if snapshot is not exist to test TableSnapshotInputFormat.
    s2.defaultStorage.management.asInstanceOf[AsynchbaseStorageManagement].withAdmin(s2.config) { admin =>
      import scala.collection.JavaConverters._
      if (admin.listSnapshots(snapshotTableName).asScala.toSet(snapshotTableName))
        admin.deleteSnapshot(snapshotTableName)

      admin.snapshot(snapshotTableName, TableName.valueOf(options.tableName))
    }

    // 3. Decode S2GraphSource to parse HFile
    val metaAndHBaseArgs = options.toConfigParams
    val hbaseConfig = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)

    val dumpArgs = Map(
      "hbase.rootdir" -> hbaseConfig.get("hbase.rootdir"),
      "restore.path" -> "/tmp/hbase_restore",
      "hbase.table.names" -> s"${snapshotTableName}",
      "hbase.table.cf" -> "e",
      "element.type" -> "IndexEdge"
    ) ++ metaAndHBaseArgs

    val dumpConf = TaskConf("dump", "sql", Seq("input"), dumpArgs)
    val source = new S2GraphSource(dumpConf)
    val realDF = source.toDF(spark)
    val outputEdges = realDF.collect().flatMap(reader.read(s2)(_))
      .sortBy(_.asInstanceOf[S2EdgeLike].tgtVertex.innerId.toIdString())

    inputEdges.foreach { e => println(s"[Input]: $e")}
    outputEdges.foreach { e => println(s"[Output]: $e")}

    inputEdges shouldBe outputEdges
  }

  test("S2GraphSource vertex toDF") {
    val column = initTestVertexSchema(s2)
    val snapshotTableName = options.tableName + "-snapshot"

    val bulkVertices = Seq(
      s"1416236400000\tinsert\tvertex\tc\t${column.service.serviceName}\t${column.columnName}\t{}",
      s"1416236400000\tinsert\tvertex\td\t${column.service.serviceName}\t${column.columnName}\t{}"
    )
    val df = s2VertexToDataFrame(bulkVertices)

    val reader = new RowBulkFormatReader

    val input = df.collect().flatMap(reader.read(s2)(_))
      .sortBy(_.asInstanceOf[S2VertexLike].innerId.toIdString())

    val args = options.toCommand.grouped(2).map { kv =>
      kv.head -> kv.last
    }.toMap ++ Map("writeMethod" -> "bulk", "runLoadIncrementalHFiles" -> "true")

    val conf = TaskConf("test", "sql", Seq("input"), args)

    val sink = new S2GraphSink("testQuery", conf)
    sink.write(df)

    // 2. create snapshot if snapshot is not exist to test TableSnapshotInputFormat.
    s2.defaultStorage.management.asInstanceOf[AsynchbaseStorageManagement].withAdmin(s2.config) { admin =>
      import scala.collection.JavaConverters._
      if (admin.listSnapshots(snapshotTableName).asScala.toSet(snapshotTableName))
        admin.deleteSnapshot(snapshotTableName)

      admin.snapshot(snapshotTableName, TableName.valueOf(options.tableName))
    }

    // 3. Decode S2GraphSource to parse HFile
    val metaAndHBaseArgs = options.toConfigParams
    val hbaseConfig = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)

    val dumpArgs = Map(
      "hbase.rootdir" -> hbaseConfig.get("hbase.rootdir"),
      "restore.path" -> "/tmp/hbase_restore",
      "hbase.table.names" -> s"${snapshotTableName}",
      "hbase.table.cf" -> "v",
      "element.type" -> "Vertex"
    ) ++ metaAndHBaseArgs

    val dumpConf = TaskConf("dump", "sql", Seq("input"), dumpArgs)
    val source = new S2GraphSource(dumpConf)
    val realDF = source.toDF(spark)

    val output = realDF.collect().flatMap(reader.read(s2)(_))
      .sortBy(_.asInstanceOf[S2VertexLike].innerId.toIdString())

    input.foreach { e => println(s"[Input]: $e")}
    output.foreach { e => println(s"[Output]: $e")}

    input shouldBe output
  }
}
