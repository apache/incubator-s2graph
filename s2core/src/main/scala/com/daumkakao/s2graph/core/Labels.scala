package com.daumkakao.s2graph.core

import play.api.libs.json.{ JsValue, Json }
import scalikejdbc._

object Label extends LocalCache[Label] {

  val maxHBaseTableNames = 2

  def apply(rs: WrappedResultSet): Label = {
    Label(Some(rs.int("id")), rs.string("label"),
      rs.int("src_service_id"), rs.string("src_column_name"), rs.string("src_column_type"),
      rs.int("tgt_service_id"), rs.string("tgt_column_name"), rs.string("tgt_column_type"),
      rs.boolean("is_directed"), rs.string("service_name"), rs.int("service_id"), rs.string("consistency_level"),
      rs.string("hbase_table_name"), rs.intOpt("hbase_table_ttl"))
  }
  def findByName(labelUseCache: (String, Boolean)): Option[Label] = {
    val (label, useCache) = labelUseCache
    findByName(label, useCache)
  }
  def findByName(label: String, useCache: Boolean = true): Option[Label] = {
    val cacheKey = s"label=$label"
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
    hTableTTL: Option[Int]) = {
    sql"""
    	insert into labels(label, 
    src_service_id, src_column_name, src_column_type, 
    tgt_service_id, tgt_column_name, tgt_column_type, 
    is_directed, service_name, service_id, consistency_level, hbase_table_name, hbase_table_ttl)
    	values (${label},
    ${srcServiceId}, ${srcColumnName}, ${srcColumnType},
    ${tgtServiceId}, ${tgtColumnName}, ${tgtColumnType},
    ${isDirected}, ${serviceName}, ${serviceId}, ${consistencyLevel}, ${hTableName}, ${hTableTTL})
    """
      .updateAndReturnGeneratedKey.apply()
  }
  def findById(id: Int): Label = {
    val cacheKey = s"id=$id"
    withCache(cacheKey)(sql"""
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
        """.map { rs => Label(rs) }.list().apply()
  }
  def findBySrcColumnId(columnId: Int): List[Label] = {
    val col = ServiceColumn.findById(columnId)
    sql"""
          select 	*
          from	labels
          where	src_column_name = ${col.columnName}
        """.map { rs => Label(rs) }.list().apply()
  }
  def findBySrcServiceId(serviceId: Int): List[Label] = {
    sql"""select * from labels where src_service_id = ${serviceId}""".map { rs => Label(rs) }.list().apply
  }
  def findByTgtServiceId(serviceId: Int): List[Label] = {
    sql"""select * from labels where tgt_service_id = ${serviceId}""".map { rs => Label(rs) }.list().apply
  }
  def insertAll(label: String,
    srcServiceId: Int,
    srcColumnName: String,
    srcColumnType: String,
    tgtServiceId: Int,
    tgtColumnName: String,
    tgtColumnType: String,
    isDirected: Boolean = true,
    serviceName: String,
    serviceId: Int,
    props: Seq[(String, Any, String, Boolean)] = Seq.empty[(String, Any, String, Boolean)],
    consistencyLevel: String,
    hTableName: Option[String],
    hTableTTL: Option[Int]) = {

    //    val ls = List(label, srcServiceId, srcColumnName, srcColumnType, tgtServiceId, tgtColumnName, tgtColumnType, isDirected
    //        , serviceName, serviceId, props.toString, consistencyLevel, hTableName)
    //    Logger.error(s"insertAll: $ls")
    val srcCol = ServiceColumn.findOrInsert(srcServiceId, srcColumnName, Some(srcColumnType))
    val tgtCol = ServiceColumn.findOrInsert(tgtServiceId, tgtColumnName, Some(tgtColumnType))
    val service = Service.findById(serviceId)
    //    require(service.id.get == srcServiceId || service.id.get == tgtServiceId)

    val createdId = insert(label, srcServiceId, srcColumnName, srcColumnType,
      tgtServiceId, tgtColumnName, tgtColumnType, isDirected, serviceName, serviceId, consistencyLevel,
      hTableName.getOrElse(service.hTableName), hTableTTL.orElse(service.hTableTTL))

    val labelMetas =
      if (props.isEmpty) List(LabelMeta.timestamp)
      else props.toList.map {
        case (name, defaultVal, dataType, usedInIndex) =>
          LabelMeta.findOrInsert(createdId.toInt, name, defaultVal.toString, dataType, usedInIndex)
      }

    //    Logger.error(s"$labelMetas")
    val defaultIndexMetaSeqs = labelMetas.filter(_.usedInIndex).map(_.seq) match {
      case metaSeqs => if (metaSeqs.isEmpty) List(LabelMeta.timestamp.seq) else metaSeqs
    }
    //    Logger.error(s"$defaultIndexMetaSeqs")
    //    kgraph.Logger.debug(s"Label: $defaultIndexMetaSeqs")
    /** deprecated */
    // 0 is reserved labelOrderSeq for delete, update
    //    LabelIndex.findOrInsert(createdId.toInt, 0, List(LabelMeta.timeStampSeq), "")
    LabelIndex.findOrInsert(createdId.toInt, LabelIndex.defaultSeq, defaultIndexMetaSeqs, "")

    /** TODO: */
    (hTableName, hTableTTL) match {
      case (None, None) => // do nothing
      case (None, Some(hbaseTableTTL)) => throw new RuntimeException("if want to specify ttl, give hbaseTableName also")
      case (Some(hbaseTableName), None) =>
        // create own hbase table with default ttl on service level.
        Management.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, service.hTableTTL)
      case (Some(hbaseTableName), Some(hbaseTableTTL)) =>
        // create own hbase table with own ttl.
        Management.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, hTableTTL)
    }
  }

  def findOrInsert(label: String,
    srcServiceId: Int,
    srcColumnName: String,
    srcColumnType: String,
    tgtServiceId: Int,
    tgtColumnName: String,
    tgtColumnType: String,
    isDirected: Boolean = true,
    serviceName: String,
    serviceId: Int,
    props: Seq[(String, Any, String, Boolean)] = Seq.empty[(String, Any, String, Boolean)],
    consistencyLevel: String,
    hTableName: Option[String],
    hTableTTL: Option[Int]): Label = {

    findByName(label, false) match {
      case Some(l) => l
      case None =>
        insertAll(label, srcServiceId, srcColumnName, srcColumnType, tgtServiceId, tgtColumnName,
          tgtColumnType, isDirected, serviceName, serviceId, props, consistencyLevel, hTableName, hTableTTL)
        val cacheKey = s"label=$label"
        expireCache(cacheKey)
        findByName(label).get
    }
  }
  def findAllLabels(): List[Label] = {
    val labels = sql"""
    	select 	*
    	from	labels
    """.map { rs => Label(rs) }.list().apply()

    labels.foreach(_.init)

    putsToCache(labels.map { label =>
      val cacheKey = s"label=${label.label}"
      (cacheKey -> label)
    })
    putsToCache(labels.map { label =>
      val cacheKey = s"id=${label.id.get}"
      (cacheKey -> label)
    })
    labels
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

  def delete(id: Int) = {
    val label = findById(id)
    sql"""delete from labels where id = ${label.id.get}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"label=${label.label}")
    cacheKeys.foreach(expireCache(_))
  }

}
case class Label(id: Option[Int], label: String,
  srcServiceId: Int, srcColumnName: String, srcColumnType: String,
  tgtServiceId: Int, tgtColumnName: String, tgtColumnType: String,
  isDirected: Boolean = true, serviceName: String, serviceId: Int,
                 consistencyLevel: String = "strong", hTableName: String,
                 hTableTTL: Option[Int]) extends JSONParser {

  def metas = LabelMeta.findAllByLabelId(id.get)
  def metaSeqsToNames = metas.map(x => (x.seq, x.name)) toMap

  //  lazy val firstHBaseTableName = hbaseTableName.split(",").headOption.getOrElse(Config.HBASE_TABLE_NAME)
  lazy val srcService = Service.findById(srcServiceId)
  lazy val tgtService = Service.findById(tgtServiceId)
  lazy val service = Service.findById(serviceId)

  /**
   * TODO
   *  change this to apply hbase table from target serviceName
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
  def srcColumnInnerVal(jsValue: JsValue) = {
    jsValueToInnerVal(jsValue, srcColumnType)
  }
  def tgtColumnInnerVal(jsValue: JsValue) = {
    jsValueToInnerVal(jsValue, tgtColumnType)
  }

  override def toString(): String = {
    val orderByKeys = LabelMeta.findAllByLabelId(id.get)
    super.toString() + orderByKeys.toString()
  }
  def findLabelIndexSeq(scoring: List[(Byte, Double)]): Byte = {
    if (scoring.isEmpty) LabelIndex.defaultSeq
    else {
      LabelIndex.findByLabeIdAndSeqs(id.get, scoring.map(_._1).sorted).map(_.seq).getOrElse(LabelIndex.defaultSeq)
    }
  }

  lazy val toJson = Json.obj("labelName" -> label,
    "from" -> srcColumn.toJson, "to" -> tgtColumn.toJson,
    //    "indexProps" -> indexPropNames,
    "defaultIndex" -> defaultIndex.map(x => x.toJson),
    "extraIndex" -> extraIndices.map(exIdx => exIdx.toJson),
    "metaProps" -> metaProps.map(_.toJson) //    , "indices" -> indices.map(idx => idx.toJson)
    )

  def deleteAll() = {
    LabelMeta.findAllByLabelId(id.get, false).foreach { x => LabelMeta.delete(x.id.get) }
    //    LabelIndexProp.findAllByLabel(id.get, false).foreach { x => LabelIndexProp.delete(x.id.get) }
    LabelIndex.findByLabelIdAll(id.get, false).foreach { x => LabelIndex.delete(x.id.get) }
    Label.delete(id.get)
  }
}
