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

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.ToolRunner
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorageManagement
import org.apache.s2graph.s2jobs.serde.reader.TsvBulkFormatReader
import org.apache.s2graph.s2jobs.serde.writer.KeyValueWriter
import org.apache.s2graph.s2jobs.spark.{FamilyHFileWriteOptions, HBaseContext, KeyFamilyQualifier}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HFileGenerator extends RawFileGenerator[String, KeyValue] {

  import scala.collection.JavaConverters._

  def getTableStartKeys(hbaseConfig: Configuration, tableName: TableName): Array[Array[Byte]] = {
    val conn = ConnectionFactory.createConnection(hbaseConfig)
    val regionLocator = conn.getRegionLocator(tableName)

    regionLocator.getStartKeys
  }

  def toHBaseConfig(graphFileOptions: GraphFileOptions): Configuration = {
    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum", graphFileOptions.zkQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, graphFileOptions.tableName)

    hbaseConf
  }

  def getStartKeys(numRegions: Int): Array[Array[Byte]] = {
    val startKey = AsynchbaseStorageManagement.getStartKey(numRegions)
    val endKey = AsynchbaseStorageManagement.getEndKey(numRegions)
    if (numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions")
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key")
    }
    val empty = new Array[Byte](0)

    if (numRegions == 3) {
      Array(empty, startKey, endKey)
    } else {
      val splitKeys: Array[Array[Byte]] = Bytes.split(startKey, endKey, numRegions - 3)
      if (splitKeys == null || splitKeys.length != numRegions - 1) {
        throw new IllegalArgumentException("Unable to split key range into enough regions")
      }
      Array(empty) ++ splitKeys.toSeq
    }
  }

  def generateHFile(sc: SparkContext,
                    s2Config: Config,
                    kvs: RDD[KeyValue],
                    options: GraphFileOptions): Unit = {
    val hbaseConfig = toHBaseConfig(options)

    val startKeys =
      if (options.incrementalLoad) {
        // need hbase connection to existing table to figure out the ranges of regions.
        getTableStartKeys(hbaseConfig, TableName.valueOf(options.tableName))
      } else {
        // otherwise we do not need to initialize Connection to hbase cluster.
        // only numRegions determine region's pre-split.
        getStartKeys(numRegions = options.numRegions)
      }

    val hbaseSc = new HBaseContext(sc, hbaseConfig)

    def flatMap(kv: KeyValue): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
      val k = new KeyFamilyQualifier(CellUtil.cloneRow(kv), CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv))
      val v = CellUtil.cloneValue(kv)
      Seq((k -> v)).toIterator
    }

    val compressionAlgorithmClass = Algorithm.valueOf(options.compressionAlgorithm).getName.toUpperCase
    val familyOptions = new FamilyHFileWriteOptions(compressionAlgorithmClass,
      BloomType.ROW.name().toUpperCase, 32768, DataBlockEncoding.FAST_DIFF.name().toUpperCase)

    val familyOptionsMap = Map("e".getBytes("UTF-8") -> familyOptions, "v".getBytes("UTF-8") -> familyOptions)


    hbaseSc.bulkLoad(kvs, TableName.valueOf(options.tableName), startKeys, flatMap, options.output, familyOptionsMap.asJava)
  }

  override def generate(sc: SparkContext,
                        config: Config,
                        rdd: RDD[String],
                        options: GraphFileOptions): Unit = {
    val transformer = new SparkBulkLoaderTransformer(config, options)

    implicit val reader = new TsvBulkFormatReader
    implicit val writer = new KeyValueWriter(options.autoEdgeCreate, options.skipError)

    val kvs = transformer.transform(rdd).flatMap(kvs => kvs)

    HFileGenerator.generateHFile(sc, config, kvs, options)
  }

  def loadIncrementalHFiles(options: GraphFileOptions): Int = {
    /* LoadIncrementHFiles */
    val hfileArgs = Array(options.output, options.tableName)
    val hbaseConfig = HBaseConfiguration.create()
    ToolRunner.run(hbaseConfig, new LoadIncrementalHFiles(hbaseConfig), hfileArgs)
  }

  def tableSnapshotDump(ss: SparkSession,
           config: Config,
           snapshotPath: String,
           restorePath: String,
           tableNames: Seq[String],
           columnFamily: String = "e",
           elementType: String = "IndexEdge",
           batchSize: Int = 1000,
           labelMapping: Map[String, String] = Map.empty,
           buildDegree: Boolean = false): RDD[Seq[Cell]] = {
    val cf = Bytes.toBytes(columnFamily)

    val hbaseConfig = HBaseConfiguration.create(ss.sparkContext.hadoopConfiguration)
    hbaseConfig.set("hbase.rootdir", snapshotPath)

    val initial = ss.sparkContext.parallelize(Seq.empty[Seq[Cell]])
    tableNames.foldLeft(initial) { case (prev, tableName) =>
      val scan = new Scan
      scan.addFamily(cf)
      scan.setBatch(batchSize)
      scan.setMaxVersions(1)
      TableSnapshotInputFormatImpl.setInput(hbaseConfig, tableName, new Path(restorePath))
      hbaseConfig.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()))

      val job = Job.getInstance(hbaseConfig, "Decode index edge from " + tableName)
      val current = ss.sparkContext.newAPIHadoopRDD(job.getConfiguration,
        classOf[TableSnapshotInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]).map(_._2.listCells().asScala.toSeq)

      prev ++ current
    }
  }
}

