package com.daumkakao.s2graph.core.mysqls

/**
 * Created by shon on 6/3/15.
 */

import com.daumkakao.s2graph.core.Management.Model.{Index, Prop}
import com.daumkakao.s2graph.core.{GraphUtil, JSONParser, Management}
import play.api.Logger
import play.api.libs.json.Json
import scalikejdbc._

object Label extends Model[Label] {

  val maxHBaseTableNames = 2

  def apply(rs: WrappedResultSet): Label = {
    Label(Option(rs.int("id")), rs.string("label"),
      rs.int("src_service_id"), rs.string("src_column_name"), rs.string("src_column_type"),
      rs.int("tgt_service_id"), rs.string("tgt_column_name"), rs.string("tgt_column_type"),
      rs.boolean("is_directed"), rs.string("service_name"), rs.int("service_id"), rs.string("consistency_level"),
      rs.string("hbase_table_name"), rs.intOpt("hbase_table_ttl"), rs.string("schema_version"), rs.boolean("is_async"), rs.string("compressionAlgorithm"))
  }

  def findByName(labelUseCache: (String, Boolean)): Option[Label] = {
    val (label, useCache) = labelUseCache
    findByName(label, useCache)
  }

  def findByName(label: String, useCache: Boolean = true): Option[Label] = {
//    val cacheKey = s"label=$label"
    val cacheKey = "label=" + label
    if (useCache) {
      withCache(cacheKey)(
        sql"""
        select *
        from labels
        where label = ${label}"""
          .map { rs => Label(rs) }.single.apply())
    } else {
      sql"""
        select *
        from labels
        where label = ${label}"""
        .map { rs => Label(rs) }.single.apply()
    }
  }

  def insert(label: String,
             srcServiceId: Int,
             srcColumnName: String,
             srcColumnType: String,
             tgtServiceId: Int,
             tgtColumnName: String,
             tgtColumnType: String,
             isDirected: Boolean,
             serviceName: String,
             serviceId: Int,
             consistencyLevel: String,
             hTableName: String,
             hTableTTL: Option[Int],
             schemaVersion: String,
             isAsync: Boolean,
             compressionAlgorithm: String) = {
    sql"""
    	insert into labels(label,
    src_service_id, src_column_name, src_column_type,
    tgt_service_id, tgt_column_name, tgt_column_type,
    is_directed, service_name, service_id, consistency_level, hbase_table_name, hbase_table_ttl, schema_version, is_async, compressionAlgorithm)
    	values (${label},
    ${srcServiceId}, ${srcColumnName}, ${srcColumnType},
    ${tgtServiceId}, ${tgtColumnName}, ${tgtColumnType},
    ${isDirected}, ${serviceName}, ${serviceId}, ${consistencyLevel}, ${hTableName}, ${hTableTTL},
    ${schemaVersion}, ${isAsync}, ${compressionAlgorithm})
    """
      .updateAndReturnGeneratedKey.apply()
  }

  def findById(id: Int): Label = {
//    val cacheKey = s"id=$id"
    val cacheKey = "id=" + id
    withCache(cacheKey)( sql"""
        select 	*
        from 	labels
        where 	id = ${id}"""
      .map { rs => Label(rs) }.single.apply()).get
  }

  def findByTgtColumnId(columnId: Int): List[Label] = {
    val col = ServiceColumn.findById(columnId)
    sql"""
          select	*
          from	labels
          where	tgt_column_name = ${col.columnName}
          and service_id = ${col.serviceId}
        """.map { rs => Label(rs) }.list().apply()
  }

  def findBySrcColumnId(columnId: Int): List[Label] = {
    val col = ServiceColumn.findById(columnId)
    sql"""
          select 	*
          from	labels
          where	src_column_name = ${col.columnName}
          and service_id = ${col.serviceId}
        """.map { rs => Label(rs) }.list().apply()
  }

  def findBySrcServiceId(serviceId: Int): List[Label] = {
    sql"""select * from labels where src_service_id = ${serviceId}""".map { rs => Label(rs) }.list().apply
  }

  def findByTgtServiceId(serviceId: Int): List[Label] = {
    sql"""select * from labels where tgt_service_id = ${serviceId}""".map { rs => Label(rs) }.list().apply
  }

  def insertAll(labelName: String, srcServiceName: String, srcColumnName: String, srcColumnType: String,
                tgtServiceName: String, tgtColumnName: String, tgtColumnType: String,
                isDirected: Boolean = true,
                serviceName: String,
                indices: Seq[Index],
                metaProps: Seq[Prop],
                consistencyLevel: String,
                hTableName: Option[String],
                hTableTTL: Option[Int],
                schemaVersion: String,
                isAsync: Boolean,
                compressionAlgorithm: String) = {

    val srcServiceOpt = Service.findByName(srcServiceName, useCache = false)
    val tgtServiceOpt = Service.findByName(tgtServiceName, useCache = false)
    val serviceOpt = Service.findByName(serviceName, useCache = false)
    if (srcServiceOpt.isEmpty) throw new RuntimeException(s"source service $srcServiceName is not created.")
    if (tgtServiceOpt.isEmpty) throw new RuntimeException(s"target service $tgtServiceName is not created.")
    if (serviceOpt.isEmpty) throw new RuntimeException(s"service $serviceName is not created.")

    val newLabel = for {
      srcService <- srcServiceOpt
      tgtService <- tgtServiceOpt
      service <- serviceOpt
    } yield {
        val srcServiceId = srcService.id.get
        val tgtServiceId = tgtService.id.get
        val serviceId = service.id.get

        /** insert serviceColumn */
        val srcCol = ServiceColumn.findOrInsert(srcServiceId, srcColumnName, Some(srcColumnType), schemaVersion)
        val tgtCol = ServiceColumn.findOrInsert(tgtServiceId, tgtColumnName, Some(tgtColumnType), schemaVersion)

        if (srcCol.columnType != srcColumnType) throw new RuntimeException(s"source service column type not matched ${srcCol.columnType} != ${srcColumnType}")
        if (tgtCol.columnType != tgtColumnType) throw new RuntimeException(s"target service column type not matched ${tgtCol.columnType} != ${tgtColumnType}")

        /** create label */
        Label.findByName(labelName, useCache = false).getOrElse {

          val createdId = insert(labelName, srcServiceId, srcColumnName, srcColumnType,
            tgtServiceId, tgtColumnName, tgtColumnType, isDirected, serviceName, serviceId, consistencyLevel,
            hTableName.getOrElse(service.hTableName), hTableTTL.orElse(service.hTableTTL), schemaVersion, isAsync, compressionAlgorithm).toInt

          val labelMetaMap = metaProps.map { case Prop(propName, defaultValue, dataType) =>
            val labelMeta = LabelMeta.findOrInsert(createdId, propName, defaultValue, dataType)
            (propName -> labelMeta.seq)
          }.toMap ++ Map(LabelMeta.timestamp.name -> LabelMeta.timestamp.seq)

          if (indices.isEmpty) { // make default index with _PK, _timestamp, 0
            LabelIndex.findOrInsert(createdId, LabelIndex.defaultName, LabelIndex.defaultMetaSeqs.toList, "none")
          } else {
            indices.foreach { index =>
              val metaSeq = index.propNames.map { name => labelMetaMap(name) }
              LabelIndex.findOrInsert(createdId, index.name, metaSeq.toList, "none")
            }
          }

          /** TODO: */
          (hTableName, hTableTTL) match {
            case (None, None) => // do nothing
            case (None, Some(hbaseTableTTL)) => throw new RuntimeException("if want to specify ttl, give hbaseTableName also")
            case (Some(hbaseTableName), None) =>
              // create own hbase table with default ttl on service level.
              Management.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, service.hTableTTL, compressionAlgorithm)
            case (Some(hbaseTableName), Some(hbaseTableTTL)) =>
              // create own hbase table with own ttl.
              Management.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, hTableTTL, compressionAlgorithm)
          }

          val cacheKeys = List(s"id=$createdId", s"label=$labelName")
          val ret = findByName(labelName, useCache = false).get
          putsToCache(cacheKeys.map(k => k -> ret))
          ret
        }
      }

    newLabel.getOrElse(throw new RuntimeException("failed to create label"))
  }

  //
  //  def findOrInsert(label: String,
  //                   srcServiceId: Int,
  //                   srcColumnName: String,
  //                   srcColumnType: String,
  //                   tgtServiceId: Int,
  //                   tgtColumnName: String,
  //                   tgtColumnType: String,
  //                   isDirected: Boolean = true,
  //                   serviceName: String,
  //                   serviceId: Int,
  //                   props: Seq[(String, Any, String, Boolean)] = Seq.empty[(String, Any, String, Boolean)],
  //                   consistencyLevel: String,
  //                   hTableName: Option[String],
  //                   hTableTTL: Option[Int],
  //                   isAsync: Boolean): Label = {
  //
  //    findByName(label, false) match {
  //      case Some(l) => l
  //      case None =>
  //        insertAll(label, srcServiceId, srcColumnName, srcColumnType, tgtServiceId, tgtColumnName,
  //          tgtColumnType, isDirected, serviceName, serviceId, props, consistencyLevel, hTableName, hTableTTL, isAsync)
  //        val cacheKey = s"label=$label"
  //        expireCache(cacheKey)
  //        findByName(label).get
  //    }
  //  }
  def findAll() = {
    val ls = sql"""select * from labels""".map { rs => Label(rs) }.list().apply()
    putsToCache(ls.map { x =>
      var cacheKey = s"id=${x.id.get}"
      (cacheKey -> x)
    })
    putsToCache(ls.map { x =>
      var cacheKey = s"label=${x.label}"
      (cacheKey -> x)
    })

    //    for {
    //      x <- labels
    //    } {
    //      Logger.info(s"Label: $x")
    //      findById(x.id.get)
    //      findByName(x.label)
    //      findBySrcColumnId(x.srcColumn.id.get)
    //      findBySrcServiceId(x.srcServiceId)
    //      findByTgtColumnId(x.tgtColumn.id.get)
    //      findByTgtServiceId(x.tgtServiceId)
    //    }
    //
    ////    labels.foreach(_.init)
    //
    //    putsToCache(labels.map { label =>
    //      val cacheKey = s"label=${label.label}"
    //      Logger.info(s"loading to local cache: $cacheKey")
    //      (cacheKey -> label)
    //    })
    //    putsToCache(labels.map { label =>
    //      val cacheKey = s"id=${label.id.get}"
    //      Logger.info(s"loading to local cache: $cacheKey")
    //      (cacheKey -> label)
    //    })
    //    labels
  }

  def findAllLabels(serviceName: Option[String] = None, offset: Int = 0, limit: Int = 10): List[Label] = {
    val sql = serviceName match {
      case None =>
        sql"""
        select 	*
        from	labels
        limit	${offset}, ${limit}
        """
      case Some(sName) =>
        sql"""
        select 	*
        from	labels
        where	service_name = ${sName}
        limit	${offset}, ${limit}
        """
    }
    sql.map { rs => Label(rs) }.list().apply()
  }

  def updateName(oldName: String, newName: String) = {
    Logger.info(s"rename label: $oldName -> $newName")
    sql"""update labels set label = ${newName} where label = ${oldName}""".execute.apply()
  }

  def delete(id: Int) = {
    val label = findById(id)
    Logger.info(s"delete label: $label")
    sql"""delete from labels where id = ${label.id.get}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"label=${label.label}")
    cacheKeys.foreach(expireCache(_))
  }

  //  def prependHBaseTableName(labelName: String, tableName: String) = {
  //    for (label <- findByName(labelName); service = Service.findById(label.serviceId)) yield {
  //      val tables = label.hbaseTableName.split(",").toList.groupBy(s => s).keys.toList
  //      val newHBaseTableNames = tableName :: tables.sortBy(s => s).reverse.take(maxHBaseTableNames - 1)
  //      for (table <- tables if !newHBaseTableNames.contains(table)) {
  //        // delete table
  //        //        try {
  //        //          Management.dropTable(Config.HBASE_ZOOKEEPER_QUORUM, table)
  //        //        } catch {
  //        //          case e: Throwable =>
  //        //            play.api.Logger.error(s"dropTable: $table failed $e")
  //        //        }
  //      }
  //      sql"""update labels set hbase_table_name = ${newHBaseTableNames.mkString(",")} where id = ${label.id.get}""".executeUpdate().apply
  //    }
  //    findByName(labelName)
  //  }
}

case class Label(id: Option[Int], label: String,
                 srcServiceId: Int, srcColumnName: String, srcColumnType: String,
                 tgtServiceId: Int, tgtColumnName: String, tgtColumnType: String,
                 isDirected: Boolean = true, serviceName: String, serviceId: Int, consistencyLevel: String = "strong",
                 hTableName: String, hTableTTL: Option[Int],
                 schemaVersion: String, isAsync: Boolean = false,
                 compressionAlgorithm: String) extends JSONParser {
  def metas = LabelMeta.findAllByLabelId(id.get)

  def metaSeqsToNames = metas.map(x => (x.seq, x.name)) toMap

  //  lazy val firstHBaseTableName = hbaseTableName.split(",").headOption.getOrElse(Config.HBASE_TABLE_NAME)
  lazy val srcService = Service.findById(srcServiceId)
  lazy val tgtService = Service.findById(tgtServiceId)
  lazy val service = Service.findById(serviceId)
  /**
   * TODO
   * change this to apply hbase table from target serviceName
   */
  //  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, service.tableName.split(",").headOption.getOrElse(Config.HBASE_TABLE_NAME))
  //  lazy val (hbaseZkAddr, hbaseTableName) = (Config.HBASE_ZOOKEEPER_QUORUM, hTableName.split(",").headOption.getOrElse(Config.HBASE_TABLE_NAME))
  //  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, hTableName.split(",").headOption.getOrElse(GraphConnection.getConfVal("hbase.table.name")))
  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, hTableName.split(",").head)

  lazy val (srcColumn, tgtColumn) = (ServiceColumn.find(srcServiceId, srcColumnName).get, ServiceColumn.find(tgtServiceId, tgtColumnName).get)

  lazy val direction = if (isDirected) "out" else "undirected"
  lazy val defaultIndex = LabelIndex.findByLabelIdAndSeq(id.get, LabelIndex.defaultSeq)

  //TODO: Make sure this is correct
  lazy val indices = LabelIndex.findByLabelIdAll(id.get, useCache = true)
  lazy val indicesMap = indices.map(idx => (idx.seq, idx)) toMap
  lazy val indexSeqsMap = indices.map(idx => (idx.metaSeqs, idx)) toMap
  lazy val extraIndices = indices.filter(idx => defaultIndex.isDefined && idx.id.get != defaultIndex.get.id.get)
  //      indices filterNot (_.id.get == defaultIndex.get.id.get)
  lazy val extraIndicesMap = extraIndices.map(idx => (idx.seq, idx)) toMap

  lazy val metaProps = LabelMeta.reservedMetas ::: LabelMeta.findAllByLabelId(id.get, useCache = true)
  lazy val metaPropsMap = metaProps.map(x => (x.seq, x)).toMap
  lazy val metaPropsInvMap = metaProps.map(x => (x.name, x)).toMap
  lazy val metaPropNames = metaProps.map(x => x.name)
  lazy val metaPropNamesMap = metaProps.map(x => (x.seq, x.name)) toMap

  def srcColumnWithDir(dir: Int) = {
    if (dir == GraphUtil.directions("out")) srcColumn else tgtColumn
  }

  def tgtColumnWithDir(dir: Int) = {
    if (dir == GraphUtil.directions("out")) tgtColumn else srcColumn
  }

  def init() = {
    metas
    metaSeqsToNames
    service
    srcColumn
    tgtColumn
    defaultIndex
    indices
    metaProps
  }

  //  def srcColumnInnerVal(jsValue: JsValue) = {
  //    jsValueToInnerVal(jsValue, srcColumnType, version)
  //  }
  //  def tgtColumnInnerVal(jsValue: JsValue) = {
  //    jsValueToInnerVal(jsValue, tgtColumnType, version)
  //  }

  override def toString(): String = {
    val orderByKeys = LabelMeta.findAllByLabelId(id.get)
    super.toString() + orderByKeys.toString()
  }

  def findLabelIndexSeq(scoring: List[(Byte, Double)]): Byte = {
    if (scoring.isEmpty) LabelIndex.defaultSeq
    else {
      LabelIndex.findByLabelIdAndSeqs(id.get, scoring.map(_._1).sorted).map(_.seq).getOrElse(LabelIndex.defaultSeq)
    }
  }

  lazy val toJson = Json.obj("labelName" -> label,
    "from" -> srcColumn.toJson, "to" -> tgtColumn.toJson,
    "defaultIndex" -> defaultIndex.map(x => x.toJson),
    "extraIndex" -> extraIndices.map(exIdx => exIdx.toJson),
    "metaProps" -> metaProps.map(_.toJson)
  )

  def deleteAll() = {
    LabelMeta.findAllByLabelId(id.get, false).foreach { x => LabelMeta.delete(x.id.get) }
    LabelIndex.findByLabelIdAll(id.get, false).foreach { x => LabelIndex.delete(x.id.get) }
    Label.delete(id.get)
  }
}

