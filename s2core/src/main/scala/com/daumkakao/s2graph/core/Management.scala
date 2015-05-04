package com.daumkakao.s2graph.core

import java.util.concurrent.ConcurrentHashMap
import HBaseElement._
import play.api.libs.json._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.HBaseConfiguration
//import coprocessor.generated.GraphStatsProtos.DeleteLabelsArgument
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
//import coprocessor.generated.GraphStatsProtos.GraphStatService
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TimeRange
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.Durability
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.ipc.ServerRpcController
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
//import coprocessor.generated.GraphStatsProtos.CountResponse
import scala.util.parsing.combinator.JavaTokenParsers

/**
 * orchestrate kafka queue, db meta.
 */
object Management extends JSONParser {

  private val adminLogger = Logger.adminLogger
  private val queryLogger = Logger.queryLogger
  private val logger = Logger.logger

  val hardLimit = 10000
  val defaultLimit = 100

  //  val labels = new ConcurrentHashMap[String, Label]
  /**
   * source, target, label, orderBy, extra properties
   * e	src	tgt	label	key:value,key:value...	key:value,key:value...
   * v src, key:value,key:value
   *
   * talk_friend 10 -> story:friend 10 ->
   */

  /**
   * label
   */
  /**
   * create {labelName, labelName_delete, labelName_update} labels
   */
  def createLabel(label: String,
    srcServiceName: String,
    srcColumnName: String,
    srcColumnType: String,
    tgtServiceName: String,
    tgtColumnName: String,
    tgtColumType: String,
    isDirected: Boolean = true,
    serviceName: String,
    indexProps: Seq[(String, JsValue)],
    props: Seq[(String, JsValue)],
    consistencyLevel: String,
    hTableName: Option[String],
    hTableTTL: Option[Int],
    isAsync: Boolean): Label = {

    //        val ls = List(label, srcServiceName, srcColumnName, srcColumnType, tgtServiceName, tgtColumnName, tgtColumType, isDirected,
    //          serviceName, indexProps.toString, props.toString, consistencyLevel, hTableName)
    //        Logger.debug(s"$ls")
    val idxProps = for ((k, v) <- indexProps; (innerVal, dataType) = toInnerVal(v)) yield (k, innerVal, dataType, true)
    val metaProps = for ((k, v) <- props; (innerVal, dataType) = toInnerVal(v)) yield (k, innerVal, dataType, false)

    val indexPropsWithType =
      for ((k, v) <- indexProps) yield {
        val (innerVal, dataType) = toInnerVal(v)
        (k, innerVal, dataType)
      }
    var cacheKey = s"serviceName=$srcServiceName"
    Service.expireCache(cacheKey)
    val srcService = tryOption(srcServiceName, Service.findByName)
    cacheKey = s"serviceName=$tgtServiceName"
    Service.expireCache(cacheKey)
    val tgtService = tryOption(tgtServiceName, Service.findByName)
    cacheKey = s"serviceName=$serviceName"
    Service.expireCache(cacheKey)
    val service = tryOption(serviceName, Service.findByName)

    //    if (service.id.get != srcService.id.get && service.id.get != tgtService.id.get) {
    //      throw new RuntimeException(s"label`s serviceName should be either: ${srcService.serviceName} or ${tgtService.serviceName}")
    //    }
    cacheKey = s"label=$label"
    Label.expireCache(cacheKey)
    val labelOpt = Label.findByName(label, useCache = false)

    labelOpt match {
      case Some(l) =>
        val e = new KGraphExceptions.LabelAlreadyExistException(s"Label name ${l.label} already exist.")
        adminLogger.error(s"$e", e)
        throw e
      case None =>
        Label.insertAll(label,
          srcService.id.get, srcColumnName, srcColumnType,
          tgtService.id.get, tgtColumnName, tgtColumType,
          isDirected, serviceName, service.id.get, idxProps ++ metaProps, consistencyLevel, hTableName, hTableTTL, isAsync)
        Label.expireCache(cacheKey)
        Label.findByName(label).get
    }
  }

  def addIndex(labelStr: String, orderByKeys: Seq[(String, JsValue)]) = {
    val label = try {
      Label.findByName(labelStr).get
    } catch {
      case e: Throwable =>
        throw new KGraphExceptions.LabelNotExistException(labelStr)
    }
    val labelOrderTypes =
      for ((k, v) <- orderByKeys; (innerVal, dataType) = toInnerVal(v)) yield {

        val lblMeta = LabelMeta.findOrInsert(label.id.get, k, innerVal.toString, dataType, true)
        if (lblMeta.usedInIndex) lblMeta.seq
        else throw new KGraphExceptions.LabelMetaExistException(s"")
      }
    LabelIndex.findOrInsert(label.id.get, labelOrderTypes.toList, "")
  }

  def getServiceLable(label: String): Option[Label] = {
    Label.findByName(label)
  }

  /**
   *
   */

  def toLabelWithDirectionAndOp(label: Label, direction: String): Option[LabelWithDirection] = {
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
    val propsWithTs = parsedProps.map(kv => (kv._1 -> InnerValWithTs(kv._2, ts))) ++ Map(LabelMeta.timeStampSeq -> InnerValWithTs(InnerVal.withLong(ts), ts))
    Edge(srcVertex, tgtVertex, labelWithDir, op, ts, version = ts, propsWithTs)

  }

  def toVertex(ts: Long, operation: String, id: String, serviceName: String, columnName: String, props: String): Vertex = {
    Service.findByName(serviceName) match {
      case None => throw new RuntimeException(s"$serviceName does not exist. create service first.")
      case Some(service) =>
        ServiceColumn.find(service.id.get, columnName) match {
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

  def toProps(column: ServiceColumn, js: JsObject): Seq[(Byte, InnerVal)] = {

    val props = for {
      (k, v) <- js.fields
    } yield {
      val colMeta = ColumnMeta.findOrInsert(column.id.get, k)
      val (innerVal, dataType) = toInnerVal(v)
      (colMeta.seq, innerVal)
    }
    //    Logger.debug(s"vertex.ToProps: $column, $js => $props")
    props

  }

  def toProps(label: Label, js: JsObject): Seq[(Byte, InnerVal)] = {

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

    val adm = new HBaseAdmin(conf)
    adm
  }
  def enableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).enableTable(tableName)
  }
  def disableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).disableTable(tableName)
  }
  def dropTable(zkAddr: String, tableName: String) = {
    adminLogger.error(s"dropping table: $tableName on $zkAddr")
    getAdmin(zkAddr).disableTable(tableName)
    getAdmin(zkAddr).deleteTable(tableName)
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
    adminLogger.info(s"create table: $tableName on $zkAddr, $cfs, $regionCnt")
    val admin = getAdmin(zkAddr)
    if (!admin.tableExists(tableName)) {
      try {
        val desc = new HTableDescriptor(TableName.valueOf(tableName))
        desc.setDurability(Durability.ASYNC_WAL)
        for (cf <- cfs) {
          val columnDesc = new HColumnDescriptor(cf)
            .setCompressionType(Compression.Algorithm.LZ4)
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
      } catch {
        case e: Throwable =>
          adminLogger.error(s"$zkAddr, $tableName failed with $e", e)
          throw e
      }
    } else {
      adminLogger.error(s"$zkAddr, $tableName, $cf already exist.")
    }
  }
  // we only use murmur hash to distribute row key.
  private def getStartKey(regionCount: Int) = {
    Bytes.toBytes((Int.MaxValue / regionCount).toInt)
  }

  private def getEndKey(regionCount: Int) = {
    Bytes.toBytes((Int.MaxValue / regionCount * (regionCount - 1)).toInt)
  }


}