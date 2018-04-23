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
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorageManagement
import org.apache.s2graph.core.{GraphElement, S2EdgeLike, S2VertexLike}
import org.apache.s2graph.s2jobs.serde.reader.RowBulkFormatReader
import org.apache.s2graph.s2jobs.{BaseSparkTest, S2GraphHelper, Schema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class SourceTest extends BaseSparkTest {
  //TODO: props to valid string.
  def toDataFrame(elements: Seq[String], schema: StructType): DataFrame = {
    val ls = elements.flatMap(s2.elementBuilder.toGraphElement(_)).map { element =>
      S2GraphHelper.graphElementToSparkSqlRow(s2, element)
    }
    val rdd = spark.sparkContext.parallelize(ls)
    spark.sqlContext.createDataFrame(rdd, schema)
  }

  def runCheck(data: Seq[String],
               schema: StructType,
               columnFamily: String,
               elementType: String): (Seq[GraphElement], Seq[GraphElement]) = {
    val snapshotTableName = options.tableName + "-snapshot"

    val df = toDataFrame(data, schema)

    val reader = new RowBulkFormatReader

    val input = df.collect().flatMap(reader.read(s2)(_))

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
      "hbase.table.cf" -> columnFamily,
      "element.type" -> elementType
    ) ++ metaAndHBaseArgs

    val dumpConf = TaskConf("dump", "sql", Seq("input"), dumpArgs)
    val source = new S2GraphSource(dumpConf)
    val realDF = source.toDF(spark)

    realDF.printSchema()

    val output = realDF.collect().flatMap(reader.read(s2)(_))

    (input, output)
  }

  test("S2GraphSource edge toDF") {
    val column = initTestVertexSchema(s2)
    val label = initTestEdgeSchema(s2, tableName, schemaVersion, compressionAlgorithm)

    val bulkEdges = Seq(
      s"1416236400000\tinsert\tedge\ta\tb\t${label.label}\t{}",
      s"1416236400000\tinsert\tedge\ta\tc\t${label.label}\t{}"
    )

    val (_inputEdges, _outputEdges) = runCheck(bulkEdges, Schema.EdgeSchema, "e", "IndexEdge")
    val inputEdges = _inputEdges.sortBy(_.asInstanceOf[S2EdgeLike].tgtVertex.innerId.toIdString())
    val outputEdges = _outputEdges.sortBy(_.asInstanceOf[S2EdgeLike].tgtVertex.innerId.toIdString())

    inputEdges.foreach { e => println(s"[Input]: $e")}
    outputEdges.foreach { e => println(s"[Output]: $e")}

    inputEdges shouldBe outputEdges
  }

  ignore("S2GraphSource vertex toDF") {
    val column = initTestVertexSchema(s2)
    val label = initTestEdgeSchema(s2, tableName, schemaVersion, compressionAlgorithm)

    val bulkVertices = Seq(
      s"1416236400000\tinsert\tvertex\tc\t${column.service.serviceName}\t${column.columnName}\t{}",
      s"1416236400000\tinsert\tvertex\td\t${column.service.serviceName}\t${column.columnName}\t{}"
    )

    val (_inputVertices, _outputVertices) = runCheck(bulkVertices, Schema.VertexSchema, "v", "Vertex")
    val inputVertices = _inputVertices.sortBy(_.asInstanceOf[S2VertexLike].innerId.toIdString())
    val outputVertices = _outputVertices.sortBy(_.asInstanceOf[S2VertexLike].innerId.toIdString())

    inputVertices.foreach { v => println(s"[Input]: $v")}
    outputVertices.foreach { v => println(s"[Output]: $v")}

    inputVertices shouldBe outputVertices
  }
}
