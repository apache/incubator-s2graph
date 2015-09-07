package com.daumkakao.s2graph.core.mysqls

/**
 * Created by shon on 6/3/15.
 */

import com.daumkakao.s2graph.core.Management.JsonModel.{Index, Prop}
import com.daumkakao.s2graph.core.{GraphUtil, JSONParser, Management}
import com.daumkakao.s2graph.logger
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

  def deleteAll(label: Label)(implicit session: DBSession) = {
    val id = label.id
    LabelMeta.findAllByLabelId(id.get, false).foreach { x => LabelMeta.delete(x.id.get) }
    LabelIndex.findByLabelIdAll(id.get, false).foreach { x => LabelIndex.delete(x.id.get) }
    Label.delete(id.get)
  }

  def findByName(label: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[Label] = {
    val cacheKey = "label=" + label
    lazy val labelOpt = sql"""
        select *
        from labels
        where label = ${label}""".map { rs => Label(rs) }.single.apply()

    if (useCache) withCache(cacheKey)(labelOpt)
    else labelOpt
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
             compressionAlgorithm: String)(implicit session: DBSession = AutoSession) = {
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

  def findById(id: Int)(implicit session: DBSession = AutoSession): Label = {
    val cacheKey = "id=" + id
    withCache(cacheKey)( sql"""
        select 	*
        from 	labels
        where 	id = ${id}"""
      .map { rs => Label(rs) }.single.apply()).get
  }

  def findByTgtColumnId(columnId: Int)(implicit session: DBSession = AutoSession): List[Label] = {
    val cacheKey = "tgtColumnId=" + columnId
    val col = ServiceColumn.findById(columnId)
    withCaches(cacheKey)(
    sql"""
          select	*
          from	labels
          where	tgt_column_name = ${col.columnName}
          and service_id = ${col.serviceId}
        """.map { rs => Label(rs) }.list().apply())
  }

  def findBySrcColumnId(columnId: Int)(implicit session: DBSession = AutoSession): List[Label] = {
    val cacheKey = "srcColumnId=" + columnId
    val col = ServiceColumn.findById(columnId)
    withCaches(cacheKey)(
    sql"""
          select 	*
          from	labels
          where	src_column_name = ${col.columnName}
          and service_id = ${col.serviceId}
        """.map { rs => Label(rs) }.list().apply())
  }

  def findBySrcServiceId(serviceId: Int)(implicit session: DBSession = AutoSession): List[Label] = {
    val cacheKey = "srcServiceId=" + serviceId
    withCaches(cacheKey)(
      sql"""select * from labels where src_service_id = ${serviceId}""".map { rs => Label(rs) }.list().apply
    )
  }

  def findByTgtServiceId(serviceId: Int)(implicit session: DBSession = AutoSession): List[Label] = {
    val cacheKey = "tgtServiceId=" + serviceId
    withCaches(cacheKey)(
    sql"""select * from labels where tgt_service_id = ${serviceId}""".map { rs => Label(rs) }.list().apply
    )
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
                compressionAlgorithm: String)(implicit session: DBSession = AutoSession) = {

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
          }.toMap ++ Map(LabelMeta.timestamp.name -> LabelMeta.timestamp.seq,
            LabelMeta.to.name -> LabelMeta.to.seq,
            LabelMeta.from.name -> LabelMeta.from.seq)

          if (indices.isEmpty) {
            // make default index with _PK, _timestamp, 0
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

  def findAll()(implicit session: DBSession = AutoSession) = {
    val ls = sql"""select * from labels""".map { rs => Label(rs) }.list().apply()
    putsToCache(ls.map { x =>
      val cacheKey = s"id=${x.id.get}"
      (cacheKey -> x)
    })
    putsToCache(ls.map { x =>
      val cacheKey = s"label=${x.label}"
      (cacheKey -> x)
    })
  }

  def updateName(oldName: String, newName: String)(implicit session: DBSession = AutoSession) = {
    logger.info(s"rename label: $oldName -> $newName")
    sql"""update labels set label = ${newName} where label = ${oldName}""".execute.apply()
  }

  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val label = findById(id)
    logger.info(s"delete label: $label")
    sql"""delete from labels where id = ${label.id.get}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"label=${label.label}")
    cacheKeys.foreach(expireCache(_))
  }
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
  lazy val indexNameMap = indices.map(idx => (idx.name, idx)) toMap
  lazy val extraIndices = indices.filter(idx => defaultIndex.isDefined && idx.id.get != defaultIndex.get.id.get)
  //      indices filterNot (_.id.get == defaultIndex.get.id.get)
  lazy val extraIndicesMap = extraIndices.map(idx => (idx.seq, idx)) toMap

  lazy val metaProps = LabelMeta.reservedMetas.map { m =>
   if (m == LabelMeta.to) m.copy(dataType = tgtColumnType)
   else if (m == LabelMeta.from) m.copy(dataType = srcColumnType)
   else m
  } ::: LabelMeta.findAllByLabelId(id.get, useCache = true)
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

//  def findLabelIndexSeq(scoring: List[(Byte, Double)]): Byte = {
//    if (scoring.isEmpty) LabelIndex.defaultSeq
//    else {
//      LabelIndex.findByLabelIdAndSeqs(id.get, scoring.map(_._1).sorted).map(_.seq).getOrElse(LabelIndex.defaultSeq)
//
////      LabelIndex.findByLabelIdAndSeqs(id.get, scoring.map(_._1).sorted).map(_.seq).getOrElse(LabelIndex.defaultSeq)
//    }
//  }

  lazy val toJson = Json.obj("labelName" -> label,
    "from" -> srcColumn.toJson, "to" -> tgtColumn.toJson,
    "defaultIndex" -> defaultIndex.map(x => x.toJson),
    "extraIndex" -> extraIndices.map(exIdx => exIdx.toJson),
    "metaProps" -> metaProps.filter { labelMeta => LabelMeta.isValidSeq(labelMeta.seq) }.map(_.toJson)
  )


}

