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
import org.apache.s2graph.core.S2EdgeLike
import org.apache.s2graph.core.storage.hbase.{AsynchbaseStorage, AsynchbaseStorageManagement}
import org.apache.s2graph.s2jobs.serde.reader.RowBulkFormatReader
import org.apache.s2graph.s2jobs.{BaseSparkTest, S2GraphHelper}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class SourceTest extends BaseSparkTest {
  def toDataFrame(edges: Seq[String]): DataFrame = {
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
    }.toDF("timestamp", "operation", "element", "from", "to", "label", "props", "direction")
  }


  test("S2GraphSource toDF") {
    val label = initTestEdgeSchema(s2, tableName, schemaVersion, compressionAlgorithm)
    val snapshotTableName = options.tableName + "-snapshot"

    // 1. run S2GraphSink to build(not actually load by using LoadIncrementalLoad) bulk load file.
    val bulkEdgeString = "1416236400000\tinsert\tedge\ta\tb\tfriends\t{\"since\":1316236400000,\"score\":10}"
    val df = toDataFrame(Seq(bulkEdgeString))

    val reader = new RowBulkFormatReader

    val inputEdges = df.collect().flatMap(reader.read(s2)(_))

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

    inputEdges.foreach { e => println(s"[Input]: $e")}
    outputEdges.foreach { e => println(s"[Output]: $e")}

    inputEdges shouldBe outputEdges
  }
}
