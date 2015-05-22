package com.daumkakao.s2graph.core

import HBaseElement._
import com.daumkakao.s2graph.core.models._
import play.api.libs.json._
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Durability}
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding

/**
 * This is designed to be bridge between rest to s2core.
 * s2core never use this for finding models.
 */
object Management extends JSONParser {

  val hardLimit = 10000
  val defaultLimit = 100
//  def getSequence(tableName: String) = {
//    HBaseModel.getSequence(tableName)
//  }
  def createService(serviceName: String,
                    cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int]): HService = {
    val service = HService.findOrInsert(serviceName, cluster, hTableName, preSplitSize, hTableTTL)
    service
  }
  def findService(serviceName: String) = {
    HService.findByName(serviceName, useCache = false)
  }
  def deleteService(serviceName: String) = {
    HService.findByName(serviceName).foreach { service =>
      service.deleteAll()
    }
  }

  def createLabel(label: String,
    srcServiceName: String,
    srcColumnName: String,
    srcColumnType: String,
    tgtServiceName: String,
    tgtColumnName: String,
    tgtColumnType: String,
    isDirected: Boolean = true,
    serviceName: String,
    indexProps: Seq[(String, JsValue)],
    props: Seq[(String, JsValue)],
    consistencyLevel: String,
    hTableName: Option[String],
    hTableTTL: Option[Int]): HLabel = {

    val idxProps = for ((k, v) <- indexProps; (innerVal, dataType) = toInnerVal(v)) yield (k, innerVal, dataType, true)
    val metaProps = for ((k, v) <- props; (innerVal, dataType) = toInnerVal(v)) yield (k, innerVal, dataType, false)

    val indexPropsWithType =
      for ((k, v) <- indexProps) yield {
        val (innerVal, dataType) = toInnerVal(v)
        (k, innerVal, dataType)
      }

    val labelOpt = HLabel.findByName(label, useCache = false)

    labelOpt match {
      case Some(l) =>
        throw new KGraphExceptions.LabelAlreadyExistException(s"Label name ${l.label} already exist.")
      case None =>
        HLabel.insertAll(label,
          srcServiceName, srcColumnName, srcColumnType,
          tgtServiceName, tgtColumnName, tgtColumnType,
          isDirected, serviceName, idxProps ++ metaProps, consistencyLevel, hTableName, hTableTTL)
        HLabel.findByName(label, useCache = false).get
    }
  }
  def findLabel(labelName: String): Option[HLabel] = {
    HLabel.findByName(labelName, useCache = false)
  }
  def deleteLabel(labelName: String) = {
    HLabel.findByName(labelName, useCache = false).foreach { label =>
      label.deleteAll()
    }
  }

  def addIndex(labelStr: String, orderByKeys: Seq[(String, JsValue)]): HLabelIndex = {
    val result = for {
      label <- HLabel.findByName(labelStr)
    } yield {
      val labelOrderTypes =
      for ((k, v) <- orderByKeys; (innerVal, dataType) = toInnerVal(v)) yield {

        val lblMeta = HLabelMeta.findOrInsert(label.id.get, k, innerVal.toString, dataType)
        lblMeta.seq
      }
      HLabelIndex.findOrInsert(label.id.get, labelOrderTypes.toList, "none")
    }
    result.getOrElse(throw new RuntimeException(s"add index failed"))
  }
  def dropIndex(labelStr: String, orderByKeys: Seq[(String, JsValue)]): HLabelIndex = {
    val result = for {
      label <- HLabel.findByName(labelStr)
    } yield {
      val labelOrderTypes =
        for ((k, v) <- orderByKeys; (innerVal, dataType) = toInnerVal(v)) yield {

          val lblMeta = HLabelMeta.findOrInsert(label.id.get, k, innerVal.toString, dataType)
          lblMeta.seq
        }
      HLabelIndex.findOrInsert(label.id.get, labelOrderTypes.toList, "")
    }
    result.getOrElse(throw new RuntimeException(s"drop index failed"))
  }
  def addProp(labelStr: String, propName: String, defaultValue: JsValue, dataType: String): HLabelMeta = {
    val result = for {
      label <- HLabel.findByName(labelStr)
    } yield {
      HLabelMeta.findOrInsert(label.id.get, propName, defaultValue.toString, dataType)
    }
    result.getOrElse(throw new RuntimeException(s"add property on label failed"))
  }
  def addVertexProp(serviceName: String, columnName: String, columnType: String): HServiceColumn  = {
    val result = for {
      service <- HService.findByName(serviceName, useCache = false)
    } yield {
      HServiceColumn.findOrInsert(service.id.get, columnName, Some(columnType))
    }
    result.getOrElse(throw new RuntimeException(s"add property on vertex failed"))
  }

  def getServiceLable(label: String): Option[HLabel] = {
    HLabel.findByName(label)
  }

  /**
   *
   */

  def toLabelWithDirectionAndOp(label: HLabel, direction: String): Option[LabelWithDirection] = {
    for {
      labelId <- label.id
      dir = GraphUtil.toDirection(direction)
    } yield LabelWithDirection(labelId, dir)
  }

  def tryOption[A, R](key: A, f: A => Option[R]) = {
    f(key) match {
      case None => throw new KGraphExceptions.InternalException(s"$key is not found in DB. create $key first.")
      case Some(r) => r
    }
  }

  def toEdge(ts: Long, operation: String, srcId: String, tgtId: String,
    labelStr: String, direction: String = "", props: String): Edge = {

    val label = tryOption(labelStr, getServiceLable)

    val src = toInnerVal(srcId, label.srcColumnType)
    val tgt = toInnerVal(tgtId, label.tgtColumnType)

    val srcVertex = Vertex(CompositeId(label.srcColumn.id.get, src, true, true), ts)
    val tgtVertex = Vertex(CompositeId(label.tgtColumn.id.get, tgt, true, true), ts)
    val dir = if (direction == "") GraphUtil.toDirection(label.direction) else GraphUtil.toDirection(direction)
    val labelWithDir = LabelWithDirection(label.id.get, dir)
    val op = tryOption(operation, GraphUtil.toOp)

    val jsObject = Json.parse(props).asOpt[JsObject].getOrElse(Json.obj())
    val parsedProps = toProps(label, jsObject).toMap
    val propsWithTs = parsedProps.map(kv => (kv._1 -> InnerValWithTs(kv._2, ts))) ++ Map(HLabelMeta.timeStampSeq -> InnerValWithTs(InnerVal.withLong(ts), ts))
    Edge(srcVertex, tgtVertex, labelWithDir, op, ts, version = ts, propsWithTs)

  }

  def toVertex(ts: Long, operation: String, id: String, serviceName: String, columnName: String, props: String): Vertex = {
    HService.findByName(serviceName) match {
      case None => throw new RuntimeException(s"$serviceName does not exist. create service first.")
      case Some(service) =>
        HServiceColumn.find(service.id.get, columnName) match {
          case None => throw new RuntimeException(s"$columnName is not exist. create service column first.")
          case Some(col) =>
            val idVal = toInnerVal(id, col.columnType)
            val op = tryOption(operation, GraphUtil.toOp)
            val jsObject = Json.parse(props).asOpt[JsObject].getOrElse(Json.obj())
            val parsedProps = toProps(col, jsObject).toMap
            Vertex(CompositeId(col.id.get, idVal, isEdge = false, useHash = true), ts, parsedProps, op = op)
        }
    }
  }

  def toProps(column: HServiceColumn, js: JsObject): Seq[(Byte, InnerVal)] = {

    val props = for {
      (k, v) <- js.fields
    } yield {
      val colMeta = HColumnMeta.findOrInsert(column.id.get, k)
      val (innerVal, dataType) = toInnerVal(v)
      (colMeta.seq, innerVal)
    }
    //    Logger.debug(s"vertex.ToProps: $column, $js => $props")
    props

  }

  def toProps(label: HLabel, js: JsObject): Seq[(Byte, InnerVal)] = {

    val props = for {
      (k, v) <- js.fields
      meta <- label.metaPropsInvMap.get(k)
      //        meta <- LabelMeta.findByName(label.id.get, k)
      //      meta = tryOption((label.id.get, k), LabelMeta.findByName)
      innerVal <- jsValueToInnerVal(v, meta.dataType)
    } yield {
      (meta.seq, innerVal)
    }
    //    Logger.error(s"toProps: $js => $props")
    props

  }

  val idTableName = "id"
  val cf = "a"
  val idColName = "id"
  val regionCnt = 10

  def getAdmin(zkAddr: String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkAddr)
    val conn = ConnectionFactory.createConnection(conf)
    conn.getAdmin
  }
  def enableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).enableTable(TableName.valueOf(tableName))
  }
  def disableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).disableTable(TableName.valueOf(tableName))
  }
  def dropTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).disableTable(TableName.valueOf(tableName))
    getAdmin(zkAddr).deleteTable(TableName.valueOf(tableName))
  }
//  def deleteEdgesByLabelIds(zkAddr: String,
//    tableName: String,
//    labelIds: String = "",
//    minTs: Long = 0L,
//    maxTs: Long = Long.MaxValue,
//    include: Boolean = true) = {
//    val conf = HBaseConfiguration.create()
//    val longTimeout = "1200000"
//    conf.set("hbase.rpc.timeout", longTimeout)
//    conf.set("hbase.client.operation.timeout", longTimeout)
//    conf.set("hbase.client.scanner.timeout.period", longTimeout)
//    conf.set("hbase.zookeeper.quorum", zkAddr)
//    val conn = HConnectionManager.createConnection(conf)
//    val table = conn.getTable(tableName.getBytes)
//    var builder = DeleteLabelsArgument.newBuilder()
//    val scanner = Scan.newBuilder()
//
//    scanner.setTimeRange(TimeRange.newBuilder().setFrom(minTs).setTo(maxTs))
//    /**
//     *  when we clean up all data does not match current database ids
//     *  we will delete row completely
//     */
//    if (!include) scanner.setFilter(ProtobufUtil.toFilter(new FirstKeyOnlyFilter))
//
//    builder.setScan(scanner)
//    for (id <- labelIds.split(",")) {
//      builder.addId(id.toInt)
//    }
//
//    val argument = builder.build()
//
//    val regionStats = table.coprocessorService(classOf[GraphStatService], null, null,
//      new Batch.Call[GraphStatService, Long]() {
//        override def call(counter: GraphStatService): Long = {
//          val controller: ServerRpcController = new ServerRpcController()
//          val rpcCallback: BlockingRpcCallback[CountResponse] = new BlockingRpcCallback[CountResponse]()
//
//          if (include) {
//            counter.cleanUpDeleteLabelsRows(controller, argument, rpcCallback)
//          } else {
//            counter.cleanUpDeleteLabelsRowsExclude(controller, argument, rpcCallback)
//          }
//
//          val response: CountResponse = rpcCallback.get()
//          if (controller.failedOnException()) throw controller.getFailedOn()
//          if (response != null && response.hasCount()) {
//            response.getCount()
//          } else {
//            0L
//          }
//        }
//      })
//
//    //    regionStats.map(kv => Bytes.toString(kv._1) -> kv._2) ++ Map("total" -> regionStats.values().sum)
//  }
  def createTable(zkAddr: String, tableName: String, cfs: List[String], regionCnt: Int, ttl: Option[Int]) = {
    try {
      val admin = getAdmin(zkAddr)
      println(admin)
      if (!admin.tableExists(TableName.valueOf(tableName))) {
        println("createTable")
        val desc = new HTableDescriptor(TableName.valueOf(tableName))
        desc.setDurability(Durability.ASYNC_WAL)
        for (cf <- cfs) {
          val columnDesc = new HColumnDescriptor(cf)
            .setCompressionType(Compression.Algorithm.GZ)
            .setBloomFilterType(BloomType.ROW)
            .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
            .setMaxVersions(1)
            .setTimeToLive(2147483647)
            .setMinVersions(0)
            .setBlocksize(32768)
            .setBlockCacheEnabled(true)
          if (ttl.isDefined) columnDesc.setTimeToLive(ttl.get)
          desc.addFamily(columnDesc)
        }

        if (regionCnt <= 1) admin.createTable(desc)
        else admin.createTable(desc, getStartKey(regionCnt), getEndKey(regionCnt), regionCnt)
      } else {
        // already exist
      }
    } catch {
      case e: Throwable => println(e)
    }
  }
  // we only use murmur hash to distribute row key.
  private def getStartKey(regionCount: Int) = {
    Bytes.toBytes((Int.MaxValue / regionCount))
  }

  private def getEndKey(regionCount: Int) = {
    Bytes.toBytes((Int.MaxValue / regionCount * (regionCount - 1)))
  }


}