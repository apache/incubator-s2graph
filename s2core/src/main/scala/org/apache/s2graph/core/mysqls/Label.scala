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

package org.apache.s2graph.core.mysqls

import org.apache.s2graph.core.GraphExceptions.ModelNotFoundException
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{GraphExceptions, GraphUtil, JSONParser}
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

  def findByName(labelName: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[Label] = {
    val cacheKey = "label=" + labelName
    lazy val labelOpt =
      sql"""
        select *
        from labels
        where label = ${labelName}""".map { rs => Label(rs) }.single.apply()

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

  def findByIdOpt(id: Int)(implicit session: DBSession = AutoSession): Option[Label] = {
    val cacheKey = "id=" + id
    withCache(cacheKey)(
      sql"""
        select 	*
        from 	labels
        where 	id = ${id}"""
        .map { rs => Label(rs) }.single.apply())
  }

  def findById(id: Int)(implicit session: DBSession = AutoSession): Label = {
    val cacheKey = "id=" + id
    withCache(cacheKey)(
      sql"""
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
                compressionAlgorithm: String)(implicit session: DBSession = AutoSession): Label = {

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
          }.toMap ++ LabelMeta.reservedMetas.map (labelMeta => labelMeta.name -> labelMeta.seq).toMap

          if (indices.isEmpty) {
            // make default index with _PK, _timestamp, 0
            LabelIndex.findOrInsert(createdId, LabelIndex.DefaultName, LabelIndex.DefaultMetaSeqs.toList, "none")
          } else {
            indices.foreach { index =>
              val metaSeq = index.propNames.map { name => labelMetaMap(name) }
              LabelIndex.findOrInsert(createdId, index.name, metaSeq.toList, "none")
            }
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
    sql"""update labels set label = ${newName} where label = ${oldName}""".update.apply()
  }

  def updateHTableName(labelName: String, newHTableName: String)(implicit session: DBSession = AutoSession) = {
    logger.info(s"update HTable of label $labelName to $newHTableName")
    val cnt = sql"""update labels set hbase_table_name = $newHTableName where label = $labelName""".update().apply()
    val label = Label.findByName(labelName, useCache = false).get

    val cacheKeys = List(s"id=${label.id}", s"label=${label.label}")
    cacheKeys.foreach { key =>
      expireCache(key)
      expireCaches(key)
    }
    cnt
  }

  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val label = findById(id)
    logger.info(s"delete label: $label")
    val cnt = sql"""delete from labels where id = ${label.id.get}""".update().apply()
    val cacheKeys = List(s"id=$id", s"label=${label.label}")
    cacheKeys.foreach { key =>
      expireCache(key)
      expireCaches(key)
    }
    cnt
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

  lazy val srcColumn = ServiceColumn.find(srcServiceId, srcColumnName).getOrElse(throw ModelNotFoundException("Source column not found"))
  lazy val tgtColumn = ServiceColumn.find(tgtServiceId, tgtColumnName).getOrElse(throw ModelNotFoundException("Target column not found"))

  lazy val direction = if (isDirected) "out" else "undirected"
  lazy val defaultIndex = LabelIndex.findByLabelIdAndSeq(id.get, LabelIndex.DefaultSeq)

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

  lazy val metaPropsInner = LabelMeta.reservedMetasInner.map { m =>
    if (m == LabelMeta.to) m.copy(dataType = tgtColumnType)
    else if (m == LabelMeta.from) m.copy(dataType = srcColumnType)
    else m
  } ::: LabelMeta.findAllByLabelId(id.get, useCache = true)

  lazy val metaPropsMap = metaProps.map(x => (x.seq, x)).toMap
  lazy val metaPropsInvMap = metaProps.map(x => (x.name, x)).toMap
  lazy val metaPropNames = metaProps.map(x => x.name)
  lazy val metaPropNamesMap = metaProps.map(x => (x.seq, x.name)) toMap

  /** this is used only by edgeToProps */
  lazy val metaPropsDefaultMap = (for {
    prop <- metaProps if LabelMeta.isValidSeq(prop.seq)
    jsValue <- innerValToJsValue(toInnerVal(prop.defaultValue, prop.dataType, schemaVersion), prop.dataType)
  } yield prop.name -> jsValue).toMap

  lazy val metaPropsDefaultMapInner = (for {
    prop <- metaPropsInner if LabelMeta.isValidSeq(prop.seq)
    jsValue <- innerValToJsValue(toInnerVal(prop.defaultValue, prop.dataType, schemaVersion), prop.dataType)
  } yield prop.name -> jsValue).toMap

  def srcColumnWithDir(dir: Int) = {
    if (dir == GraphUtil.directions("out")) srcColumn else tgtColumn
  }

  def tgtColumnWithDir(dir: Int) = {
    if (dir == GraphUtil.directions("out")) tgtColumn else srcColumn
  }

  def srcTgtColumn(dir: Int) =
    if (isDirected) {
      (srcColumnWithDir(dir), tgtColumnWithDir(dir))
    } else {
      if (dir == GraphUtil.directions("in")) {
        (tgtColumn, srcColumn)
      } else {
        (srcColumn, tgtColumn)
      }
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
    "isDirected" -> isDirected,
    "serviceName" -> serviceName,
    "consistencyLevel" -> consistencyLevel,
    "schemaVersion" -> schemaVersion,
    "isAsync" -> isAsync,
    "compressionAlgorithm" -> compressionAlgorithm,
    "defaultIndex" -> defaultIndex.map(x => x.toJson),
    "extraIndex" -> extraIndices.map(exIdx => exIdx.toJson),
    "metaProps" -> metaProps.filter { labelMeta => LabelMeta.isValidSeqForAdmin(labelMeta.seq) }.map(_.toJson)
  )


}

