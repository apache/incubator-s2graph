package subscriber


import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{LabelMeta, Label}
import com.kakao.s2graph.core.types.{InnerValLikeWithTs, LabelWithDirection, SourceVertexId}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import org.hbase.async.{PutRequest}
import play.api.libs.json.Json
import s2.spark.{SparkApp}
import spark.{FamilyHFileWriteOptions, KeyFamilyQualifier, HBaseContext}
import scala.collection.JavaConversions._


object TransferToHFile extends SparkApp with JSONParser {

  val usages =
    s"""
       |create HFiles for hbase table on zkQuorum specified.
       |note that hbase table is created already and pre-splitted properly.
       |
       |params:
       |0. input: hdfs path for tsv file(bulk format).
       |1. output: hdfs path for storing HFiles.
       |2. zkQuorum: running hbase cluster zkQuorum.
       |3. tableName: table name for this bulk upload.
       |4. dbUrl: db url for parsing to graph element.
     """.stripMargin

  //TODO: Process AtomicIncrementRequest too.
  /** build key values */
  case class DegreeKey(vertexIdStr: String, labelName: String, direction: String)

  private def insertBulkForLoaderAsync(edge: Edge, createRelEdges: Boolean = true): List[PutRequest] = {
    val relEdges = if (createRelEdges) edge.relatedEdges else List(edge)
    buildPutRequests(edge.toSnapshotEdge) ++ relEdges.toList.flatMap { e =>
      e.edgesWithIndex.flatMap { indexEdge => buildPutRequests(indexEdge) }
    }
  }

  def buildDegrees(msgs: RDD[String], labelMapping: Map[String, String], edgeAutoCreate: Boolean) = {
    for {
      msg <- msgs
      tokens = GraphUtil.split(msg)
      if tokens(2) == "e" || tokens(2) == "edge"
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
    val kvs = GraphSubscriberHelper.g.storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues.toList
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }
  }
  def buildPutRequests(indexEdge: IndexEdge): List[PutRequest] = {
    val kvs = GraphSubscriberHelper.g.storage.indexEdgeSerializer(indexEdge).toKeyValues.toList
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }
  }
  def buildDegreePutRequests(vertexId: String, labelName: String, direction: String, degreeVal: Long): List[PutRequest] = {
    val label = Label.findByName(labelName).getOrElse(throw new RuntimeException(s"$labelName is not found in DB."))
    val dir = GraphUtil.directions(direction)
    val innerVal = jsValueToInnerVal(Json.toJson(vertexId), label.srcColumnWithDir(dir).columnType, label.schemaVersion).getOrElse {
      throw new RuntimeException(s"$vertexId can not be converted into innerval")
    }
    val vertex = Vertex(SourceVertexId(label.srcColumn.id.get, innerVal))

    val ts = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion))
    val labelWithDir = LabelWithDirection(label.id.get, dir)
    val edge = Edge(vertex, vertex, labelWithDir, propsWithTs=propsWithTs)

    edge.edgesWithIndex.flatMap { indexEdge =>
      GraphSubscriberHelper.g.storage.indexEdgeSerializer(indexEdge).toKeyValues.map { kv =>
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
    val kvs = for {
      s <- strs
      element <- Graph.toGraphElement(s, labelMapping).toSeq if element.isInstanceOf[Edge]
      edge = element.asInstanceOf[Edge]
      putRequest <- insertBulkForLoaderAsync(edge, autoEdgeCreate)
    } yield {
        val p = putRequest
        val kv = new KeyValue(p.key(), p.family(), p.qualifier, p.timestamp, p.value)

        //        println(s"[Edge]: $edge\n[Put]: $p\n[KeyValue]: ${kv.getRow.toList}, ${kv.getQualifier.toList}, ${kv.getValue.toList}, ${kv.getTimestamp}")

        kv
      }
    kvs.toIterator
  }


  override def run() = {
    val input = args(0)
    val tmpPath = args(1)
    val zkQuorum = args(2)
    val tableName = args(3)
    val dbUrl = args(4)
    val maxHFilePerResionServer = args(5).toInt
    val labelMapping = if (args.length >= 7) GraphSubscriberHelper.toLabelMapping(args(6)) else Map.empty[String, String]
    val autoEdgeCreate = if (args.length >= 8) args(7).toBoolean else false
    val buildDegree = if (args.length >= 9) args(8).toBoolean else true
    val compressionAlgorithm = if (args.length >= 10) args(9) else "lz4"
    val conf = sparkConf(s"$input: TransferToHFile")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.mb", "24")

    val sc = new SparkContext(conf)

    Management.createTable(zkQuorum, tableName, List("e", "v"), maxHFilePerResionServer, None, compressionAlgorithm)

    /** set up hbase init */
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hbaseConf.set("hadoop.tmp.dir", s"/tmp/$tableName")


    val rdd = sc.textFile(input)


    val kvs = rdd.mapPartitions { iter =>
      val phase = System.getProperty("phase")
      GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")
      toKeyValues(iter.toSeq, labelMapping, autoEdgeCreate)
    }
    //
    //    val newRDD = if (!buildDegree) new HFileRDD(kvs)
    //    else {
    //      val degreeKVs = buildDegrees(rdd, labelMapping, autoEdgeCreate).reduceByKey { (agg, current) =>
    //        agg + current
    //      }.mapPartitions { iter =>
    //        val phase = System.getProperty("phase")
    //        GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")
    //        toKeyValues(iter.toSeq)
    //      }
    //      new HFileRDD(kvs ++ degreeKVs)
    //    }
    //
    //    newRDD.toHFile(hbaseConf, zkQuorum, tableName, maxHFilePerResionServer, tmpPath)
    val merged = if (!buildDegree) kvs
    else {
      kvs ++ buildDegrees(rdd, labelMapping, autoEdgeCreate).reduceByKey { (agg, current) =>
        agg + current
      }.mapPartitions { iter =>
        val phase = System.getProperty("phase")
        GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")
        toKeyValues(iter.toSeq)
      }
    }

    val hbaseSc = new HBaseContext(sc, hbaseConf)
    def flatMap(kv: KeyValue): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
      val k = new KeyFamilyQualifier(kv.getRow(), kv.getFamily(), kv.getQualifier())
      val v = kv.getValue()
      Seq((k -> v)).toIterator
    }
    val familyOptions = new FamilyHFileWriteOptions(Algorithm.LZ4.getName.toUpperCase,
      BloomType.ROW.name().toUpperCase, 32768, DataBlockEncoding.FAST_DIFF.name().toUpperCase)
    val familyOptionsMap = Map("e".getBytes() -> familyOptions, "v".getBytes() -> familyOptions)

    hbaseSc.bulkLoad(merged, TableName.valueOf(tableName), flatMap, tmpPath, familyOptionsMap)
  }

}
