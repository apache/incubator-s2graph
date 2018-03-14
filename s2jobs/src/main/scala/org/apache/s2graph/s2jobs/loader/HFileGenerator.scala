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
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, KeyValue, TableName}
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorageManagement
import org.apache.s2graph.core.types.{InnerValLikeWithTs, SourceVertexId}
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.s2jobs.S2GraphHelper
import org.apache.s2graph.s2jobs.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hbase.async.PutRequest
import play.api.libs.json.Json

object HFileGenerator extends RawFileGenerator {

  import scala.collection.JavaConverters._

  private def insertBulkForLoaderAsync(s2: S2Graph, edge: S2Edge, createRelEdges: Boolean = true): List[PutRequest] = {
    val relEdges = if (createRelEdges) edge.relatedEdges else List(edge)

    buildPutRequests(s2, edge.toSnapshotEdge) ++ relEdges.toList.flatMap { e =>
      e.edgesWithIndex.flatMap { indexEdge => buildPutRequests(s2, indexEdge) }
    }
  }

  def buildPutRequests(s2: S2Graph, snapshotEdge: SnapshotEdge): List[PutRequest] = {
    val kvs = s2.getStorage(snapshotEdge.label).serDe.snapshotEdgeSerializer(snapshotEdge).toKeyValues.toList
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }
  }

  def buildPutRequests(s2: S2Graph, indexEdge: IndexEdge): List[PutRequest] = {
    val kvs = s2.getStorage(indexEdge.label).serDe.indexEdgeSerializer(indexEdge).toKeyValues.toList
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }
  }

  def buildDegreePutRequests(s2: S2Graph, vertexId: String, labelName: String, direction: String, degreeVal: Long): List[PutRequest] = {
    val label = Label.findByName(labelName).getOrElse(throw new RuntimeException(s"$labelName is not found in DB."))
    val dir = GraphUtil.directions(direction)
    val innerVal = JSONParser.jsValueToInnerVal(Json.toJson(vertexId), label.srcColumnWithDir(dir).columnType, label.schemaVersion).getOrElse {
      throw new RuntimeException(s"$vertexId can not be converted into innerval")
    }
    val vertex = s2.elementBuilder.newVertex(SourceVertexId(label.srcColumn, innerVal))

    val ts = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timestamp -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion))
    val edge = s2.elementBuilder.newEdge(vertex, vertex, label, dir, propsWithTs = propsWithTs)

    edge.edgesWithIndex.flatMap { indexEdge =>
      s2.getStorage(indexEdge.label).serDe.indexEdgeSerializer(indexEdge).toKeyValues.map { kv =>
        new PutRequest(kv.table, kv.row, kv.cf, Array.empty[Byte], Bytes.toBytes(degreeVal), kv.timestamp)
      }
    }
  }

  def toKeyValues(s2: S2Graph, degreeKeyVals: Seq[(DegreeKey, Long)]): Iterator[KeyValue] = {
    val kvs = for {
      (key, value) <- degreeKeyVals
      putRequest <- buildDegreePutRequests(s2, key.vertexIdStr, key.labelName, key.direction, value)
    } yield {
      val p = putRequest
      val kv = new KeyValue(p.key(), p.family(), p.qualifier, p.timestamp, p.value)
      kv
    }
    kvs.toIterator
  }

  def toKeyValues(s2: S2Graph, strs: Seq[String], labelMapping: Map[String, String], autoEdgeCreate: Boolean): Iterator[KeyValue] = {
    val kvList = new java.util.ArrayList[KeyValue]
    for (s <- strs) {
      val elementList = s2.elementBuilder.toGraphElement(s, labelMapping).toSeq
      for (element <- elementList) {
        if (element.isInstanceOf[S2Edge]) {
          val edge = element.asInstanceOf[S2Edge]
          val putRequestList = insertBulkForLoaderAsync(s2, edge, autoEdgeCreate)
          for (p <- putRequestList) {
            val kv = new KeyValue(p.key(), p.family(), p.qualifier, p.timestamp, p.value)
            kvList.add(kv)
          }
        } else if (element.isInstanceOf[S2Vertex]) {
          val vertex = element.asInstanceOf[S2Vertex]
          val putRequestList = s2.getStorage(vertex.service).serDe.vertexSerializer(vertex).toKeyValues.map { kv =>
            new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp)
          }
          for (p <- putRequestList) {
            val kv = new KeyValue(p.key(), p.family(), p.qualifier, p.timestamp, p.value)
            kvList.add(kv)
          }
        }
      }
    }
    kvList.iterator().asScala
  }


  def getTableStartKeys(hbaseConfig: Configuration, tableName: TableName): Array[Array[Byte]] = {
    val conn = ConnectionFactory.createConnection(hbaseConfig)
    val regionLocator = conn.getRegionLocator(tableName)
    regionLocator.getStartKeys
  }

  def toHBaseConfig(graphFileOptions: GraphFileOptions): Configuration = {
    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum", graphFileOptions.zkQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, graphFileOptions.tableName)
//    hbaseConf.set("hadoop.tmp.dir", s"/tmp/${graphFileOptions.tableName}")

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

  def transfer(sc: SparkContext,
               s2Config: Config,
               input: RDD[String],
               graphFileOptions: GraphFileOptions): RDD[KeyValue] = {
    val kvs = input.mapPartitions { iter =>
      val s2 = S2GraphHelper.initS2Graph(s2Config)

      val s = toKeyValues(s2, iter.toSeq, graphFileOptions.labelMapping, graphFileOptions.autoEdgeCreate)
      s
    }

    if (!graphFileOptions.buildDegree) kvs
    else {
      kvs ++ buildDegrees(input, graphFileOptions.labelMapping, graphFileOptions.autoEdgeCreate).reduceByKey { (agg, current) =>
        agg + current
      }.mapPartitions { iter =>
        val s2 = S2GraphHelper.initS2Graph(s2Config)

        toKeyValues(s2, iter.toSeq)
      }
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
                        _options: GraphFileOptions): Unit = {
    val kvs = transfer(sc, config, rdd, _options)
    generateHFile(sc, config, kvs, _options)
  }
}
