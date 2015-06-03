package com.daumkakao.s2graph.core


import com.daumkakao.s2graph.core.models._
import com.daumkakao.s2graph.core.types.{InnerVal, InnerValWithTs, CompositeId, LabelWithDirection}
//import com.daumkakao.s2graph.core.HBaseElement.{InnerVal, InnerValWithTs, CompositeId, LabelWithDirection}
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
                    cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int]): Service = {
    val service = Service.findOrInsert(serviceName, cluster, hTableName, preSplitSize, hTableTTL)
    service
  }

  def findService(serviceName: String) = {
    Service.findByName(serviceName, useCache = false)
  }

  def deleteService(serviceName: String) = {
    Service.findByName(serviceName).foreach { service =>
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
                  indexProps: Seq[(String, JsValue, String)],
                  props: Seq[(String, JsValue, String)],
                  consistencyLevel: String,
                  hTableName: Option[String],
                  hTableTTL: Option[Int]): Label = {

    //    val idxProps = for ((k, v, t) <- indexProps; innerVal <- jsValueToInnerVal(v, t)) yield (k, innerVal, t, true)
    //    val metaProps = for ((k, v, t) <- props; innerVal <- jsValueToInnerVal(v, t)) yield (k, innerVal, t, false)

    //    val indexPropsWithType =
    //      for ((k, v) <- indexProps) yield {
    //        val (innerVal, dataType) = toInnerVal(v)
    //        (k, innerVal, dataType)
    //      }

    val labelOpt = Label.findByName(label, useCache = false)

    labelOpt match {
      case Some(l) =>
        throw new KGraphExceptions.LabelAlreadyExistException(s"Label name ${l.label} already exist.")
      case None =>
        Label.insertAll(label,
          srcServiceName, srcColumnName, srcColumnType,
          tgtServiceName, tgtColumnName, tgtColumnType,
          isDirected, serviceName, indexProps, props, consistencyLevel, hTableName, hTableTTL)
        Label.findByName(label, useCache = false).get
    }
  }
  def createVertex(serviceName: String,
                   columnName: String,
                   columnType: String,
                   props: Seq[(String, JsValue, String)]) = {
    val serviceOpt = Service.findByName(serviceName)
    serviceOpt match {
      case None => throw new RuntimeException(s"create service $serviceName has not been created.")
      case Some(service) =>
        val serviceColumn = ServiceColumn.findOrInsert(service.id.get, columnName, Some(columnType))
        for {
          (propName, defaultValue, dataType) <- props
        } yield {
          ColumnMeta.findOrInsert(serviceColumn.id.get, propName, dataType)
        }
    }
  }
  def findLabel(labelName: String): Option[Label] = {
    Label.findByName(labelName, useCache = false)
  }

  def deleteLabel(labelName: String) = {
    Label.findByName(labelName, useCache = false).foreach { label =>
      label.deleteAll()
    }
  }

  def addIndex(labelStr: String, idxProps: Seq[(String, JsValue, String)]): LabelIndex = {
    val result = for {
      label <- Label.findByName(labelStr)
    } yield {
        val labelOrderTypes =
          for ((k, v, dataType) <- idxProps; innerVal <- jsValueToInnerVal(v, dataType)) yield {
            val lblMeta = LabelMeta.findOrInsert(label.id.get, k, innerVal.toString, dataType)
            lblMeta.seq
          }
        LabelIndex.findOrInsert(label.id.get, labelOrderTypes.toList, "none")
      }
    result.getOrElse(throw new RuntimeException(s"add index failed"))
  }

  def dropIndex(labelStr: String, idxProps: Seq[(String, JsValue, String)]): LabelIndex = {
    val result = for {
      label <- Label.findByName(labelStr)
    } yield {
        val labelOrderTypes =
          for ((k, v, dataType) <- idxProps; innerVal <- jsValueToInnerVal(v, dataType)) yield {

            val lblMeta = LabelMeta.findOrInsert(label.id.get, k, innerVal.toString, dataType)
            lblMeta.seq
          }
        LabelIndex.findOrInsert(label.id.get, labelOrderTypes.toList, "")
      }
    result.getOrElse(throw new RuntimeException(s"drop index failed"))
  }

  def addProp(labelStr: String, propName: String, defaultValue: JsValue, dataType: String): LabelMeta = {
    val result = for {
      label <- Label.findByName(labelStr)
    } yield {
        LabelMeta.findOrInsert(label.id.get, propName, defaultValue.toString, dataType)
      }
    result.getOrElse(throw new RuntimeException(s"add property on label failed"))
  }

  def addVertexProp(serviceName: String, columnName: String, columnType: String): ServiceColumn = {
    val result = for {
      service <- Service.findByName(serviceName, useCache = false)
    } yield {
        ServiceColumn.findOrInsert(service.id.get, columnName, Some(columnType))
      }
    result.getOrElse(throw new RuntimeException(s"add property on vertex failed"))
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
      meta <- column.metasInvMap.get(k)
      innerVal <- jsValueToInnerVal(v, meta.dataType)
    } yield {
        (meta.seq, innerVal)
      }
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