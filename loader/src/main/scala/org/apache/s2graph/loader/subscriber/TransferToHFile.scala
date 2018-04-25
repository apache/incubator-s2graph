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

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema.{Label, LabelMeta}
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorageManagement
import org.apache.s2graph.core.types.{InnerValLikeWithTs, SourceVertexId}
import org.apache.s2graph.loader.spark.{FamilyHFileWriteOptions, HBaseContext, KeyFamilyQualifier}
import org.apache.s2graph.spark.spark.SparkApp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hbase.async.PutRequest
import play.api.libs.json.Json

import scala.collection.JavaConversions._


object TransferToHFile extends SparkApp {

  var options:GraphFileOptions = _

  case class GraphFileOptions(input: String = "",
                              tmpPath: String = s"/tmp/bulkload",
                              zkQuorum: String = "",
                              tableName: String = "",
                              dbUrl: String = "",
                              dbUser: String = "",
                              dbPassword: String = "",
                              maxHFilePerRegionServer: Int = 1,
                              numRegions: Int = 3,
                              labelMapping: Map[String, String] = Map.empty[String, String],
                              autoEdgeCreate: Boolean = false,
                              buildDegree: Boolean = false,
                              incrementalLoad: Boolean = false,
                              compressionAlgorithm: String = "NONE") {
    def toConfigParams = {
      Map(
        "hbase.zookeeper.quorum" -> zkQuorum,
        "db.default.url" -> dbUrl,
        "db.default.user" -> dbUser,
        "db.default.password" -> dbPassword
      )
    }
  }

  val parser = new scopt.OptionParser[GraphFileOptions]("run") {

    opt[String]('i', "input").required().action( (x, c) =>
      c.copy(input = x) ).text("hdfs path for tsv file(bulk format)")

    opt[String]('m', "tmpPath").required().action( (x, c) =>
      c.copy(tmpPath = x) ).text("temp hdfs path for storing HFiles")

    opt[String]('z', "zkQuorum").required().action( (x, c) =>
      c.copy(zkQuorum = x) ).text("zookeeper config for hbase")

    opt[String]('t', "table").required().action( (x, c) =>
      c.copy(tableName = x) ).text("table name for this bulk upload.")

    opt[String]('c', "dbUrl").required().action( (x, c) =>
      c.copy(dbUrl = x)).text("jdbc connection url.")

    opt[String]('u', "dbUser").required().action( (x, c) =>
      c.copy(dbUser = x)).text("database user name.")

    opt[String]('p', "dbPassword").required().action( (x, c) =>
      c.copy(dbPassword = x)).text("database password.")

    opt[Int]('h', "maxHFilePerRegionServer").action ( (x, c) =>
      c.copy(maxHFilePerRegionServer = x)).text("maximum number of HFile per RegionServer."
    )

    opt[Int]('n', "numRegions").action ( (x, c) =>
      c.copy(numRegions = x)).text("total numRegions(pre-split size) on table."
    )

    opt[String]('l', "labelMapping").action( (x, c) =>
      c.copy(labelMapping = toLabelMapping(x)) ).text("mapping info to change the label from source (originalLabel:newLabel)")

    opt[Boolean]('d', "buildDegree").action( (x, c) =>
      c.copy(buildDegree = x)).text("generate degree values")

    opt[Boolean]('a', "autoEdgeCreate").action( (x, c) =>
      c.copy(autoEdgeCreate = x)).text("generate reverse edge automatically")

    opt[Boolean]('c', "incrementalLoad").action( (x, c) =>
      c.copy(incrementalLoad = x)).text("whether incremental bulkload which append data on existing table or not."
    )
  }

  //TODO: Process AtomicIncrementRequest too.
  /** build key values */
  case class DegreeKey(vertexIdStr: String, labelName: String, direction: String)

  private def toLabelMapping(lableMapping: String): Map[String, String] = {
    (for {
      token <- lableMapping.split(",")
      inner = token.split(":") if inner.length == 2
    } yield {
      (inner.head, inner.last)
    }).toMap
  }

  private def insertBulkForLoaderAsync(edge: S2Edge, createRelEdges: Boolean = true): List[PutRequest] = {
    val relEdges = if (createRelEdges) edge.relatedEdges else List(edge)
    buildPutRequests(edge.toSnapshotEdge) ++ relEdges.toList.flatMap { e =>
      e.edgesWithIndex.flatMap { indexEdge => buildPutRequests(indexEdge) }
    }
  }

  def buildDegrees(msgs: RDD[String], labelMapping: Map[String, String], edgeAutoCreate: Boolean) = {
    val filtered = msgs.filter { case msg =>
      val tokens = GraphUtil.split(msg)
      tokens(2) == "e" || tokens(2) == "edge"
    }
    for {
      msg <- filtered
      tokens = GraphUtil.split(msg)
      tempDirection = if (tokens.length == 7) "out" else tokens(7)
      direction = if (tempDirection != "out" && tempDirection != "in") "out" else tempDirection
      reverseDirection = if (direction == "out") "in" else "out"
      convertedLabelName = labelMapping.get(tokens(5)).getOrElse(tokens(5))
      (vertexIdStr, vertexIdStrReversed) = (tokens(3), tokens(4))
      degreeKey = DegreeKey(vertexIdStr, convertedLabelName, direction)
      degreeKeyReversed = DegreeKey(vertexIdStrReversed, convertedLabelName, reverseDirection)
      extra = if (edgeAutoCreate) List(degreeKeyReversed -> 1L) else Nil
      output <- List(degreeKey -> 1L) ++ extra
    } yield output
  }

  def buildPutRequests(snapshotEdge: SnapshotEdge): List[PutRequest] = {
    val kvs = GraphSubscriberHelper.g.getStorage(snapshotEdge.label).serDe.snapshotEdgeSerializer(snapshotEdge).toKeyValues.toList
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }
  }

  def buildPutRequests(indexEdge: IndexEdge): List[PutRequest] = {
    val kvs = GraphSubscriberHelper.g.getStorage(indexEdge.label).serDe.indexEdgeSerializer(indexEdge).toKeyValues.toList
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }
  }

  def buildDegreePutRequests(vertexId: String, labelName: String, direction: String, degreeVal: Long): List[PutRequest] = {
    val label = Label.findByName(labelName).getOrElse(throw new RuntimeException(s"$labelName is not found in DB."))
    val dir = GraphUtil.directions(direction)
    val innerVal = JSONParser.jsValueToInnerVal(Json.toJson(vertexId), label.srcColumnWithDir(dir).columnType, label.schemaVersion).getOrElse {
      throw new RuntimeException(s"$vertexId can not be converted into innerval")
    }
    val vertex = GraphSubscriberHelper.builder.newVertex(SourceVertexId(label.srcColumn, innerVal))

    val ts = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timestamp -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion))
    val edge = GraphSubscriberHelper.builder.newEdge(vertex, vertex, label, dir, propsWithTs = propsWithTs)

    edge.edgesWithIndex.flatMap { indexEdge =>
      GraphSubscriberHelper.g.getStorage(indexEdge.label).serDe.indexEdgeSerializer(indexEdge).toKeyValues.map { kv =>
        new PutRequest(kv.table, kv.row, kv.cf, Array.empty[Byte], Bytes.toBytes(degreeVal), kv.timestamp)
      }
    }
  }

  def toKeyValues(degreeKeyVals: Seq[(DegreeKey, Long)]): Iterator[KeyValue] = {
    val kvs = for {
      (key, value) <- degreeKeyVals
      putRequest <- buildDegreePutRequests(key.vertexIdStr, key.labelName, key.direction, value)
    } yield {
      val p = putRequest
      val kv = new KeyValue(p.key(), p.family(), p.qualifier, p.timestamp, p.value)
      kv
    }
    kvs.toIterator
  }

  def toKeyValues(strs: Seq[String], labelMapping: Map[String, String], autoEdgeCreate: Boolean): Iterator[KeyValue] = {
    val kvList = new java.util.ArrayList[KeyValue]
    for (s <- strs) {
      val elementList = GraphSubscriberHelper.g.elementBuilder.toGraphElement(s, labelMapping).toSeq
      for (element <- elementList) {
        if (element.isInstanceOf[S2Edge]) {
          val edge = element.asInstanceOf[S2Edge]
          val putRequestList = insertBulkForLoaderAsync(edge, autoEdgeCreate)
          for (p <- putRequestList) {
            val kv = new KeyValue(p.key(), p.family(), p.qualifier, p.timestamp, p.value)
            kvList.add(kv)
          }
        } else if (element.isInstanceOf[S2Vertex]) {
          val vertex = element.asInstanceOf[S2Vertex]
          val putRequestList = GraphSubscriberHelper.g.getStorage(vertex.service).serDe.vertexSerializer(vertex).toKeyValues.map { kv =>
            new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp)
          }
          for (p <- putRequestList) {
            val kv = new KeyValue(p.key(), p.family(), p.qualifier, p.timestamp, p.value)
            kvList.add(kv)
          }
        }
      }
    }
    kvList.iterator()
  }

  def generateKeyValues(sc: SparkContext,
                        s2Config: Config,
                        input: RDD[String],
                        graphFileOptions: GraphFileOptions): RDD[KeyValue] = {
    val kvs = input.mapPartitions { iter =>
      GraphSubscriberHelper.apply(s2Config)

      toKeyValues(iter.toSeq, graphFileOptions.labelMapping, graphFileOptions.autoEdgeCreate)
    }

    if (!graphFileOptions.buildDegree) kvs
    else {
      kvs ++ buildDegrees(input, graphFileOptions.labelMapping, graphFileOptions.autoEdgeCreate).reduceByKey { (agg, current) =>
        agg + current
      }.mapPartitions { iter =>
        GraphSubscriberHelper.apply(s2Config)

        toKeyValues(iter.toSeq)
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


    hbaseSc.bulkLoad(kvs, TableName.valueOf(options.tableName), startKeys, flatMap, options.tmpPath, familyOptionsMap)
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
    hbaseConf.set("hadoop.tmp.dir", s"/tmp/${graphFileOptions.tableName}")

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

  override def run() = {
    parser.parse(args, GraphFileOptions()) match {
      case Some(o) => options = o
      case None =>
        parser.showUsage()
        throw new IllegalArgumentException("failed to parse options...")
    }

    println(s">>> Options: ${options}")
    val s2Config = Management.toConfig(options.toConfigParams)

    val conf = sparkConf(s"TransferToHFile")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.mb", "24")

    val sc = new SparkContext(conf)
    val rdd = sc.textFile(options.input)

    GraphSubscriberHelper.apply(s2Config)

    val merged = TransferToHFile.generateKeyValues(sc, s2Config, rdd, options)
    generateHFile(sc, s2Config, merged, options)
//    /* set up hbase init */
//    val hbaseSc = new HBaseContext(sc, toHBaseConfig(options))
//
//    def flatMap(kv: KeyValue): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
//      val k = new KeyFamilyQualifier(CellUtil.cloneRow(kv), CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv))
//      val v = CellUtil.cloneValue(kv)
//      Seq((k -> v)).toIterator
//    }
//
//    val familyOptions = new FamilyHFileWriteOptions(Algorithm.LZ4.getName.toUpperCase,
//      BloomType.ROW.name().toUpperCase, 32768, DataBlockEncoding.FAST_DIFF.name().toUpperCase)
//    val familyOptionsMap = Map("e".getBytes("UTF-8") -> familyOptions, "v".getBytes("UTF-8") -> familyOptions)
//
//    val startKeys = getStartKeys(numRegions = options.numRegions)
//    hbaseSc.bulkLoad(merged, TableName.valueOf(options.tableName), startKeys, flatMap, options.tmpPath, familyOptionsMap)
  }

}
