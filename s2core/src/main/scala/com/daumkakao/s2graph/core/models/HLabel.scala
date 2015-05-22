package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.models.HBaseModel.{KEY, VAL}
import com.daumkakao.s2graph.core.{JSONParser, Management}
import play.api.libs.json.{Json, JsValue}

/**
 * Created by shon on 5/15/15.
 */

object HLabel {
  val maxHBaseTableNames = 2
  type PROPS = (String, Any, String, Boolean)
  def findByName(labelUseCache: (String, Boolean)): Option[HLabel] = {
    findByName(labelUseCache._1, labelUseCache._2)
  }
  def findByName(label: String, useCache: Boolean = true): Option[HLabel] = {
    HBaseModel.find[HLabel](useCache)(Seq(("label" -> label)))
  }
  def findById(id: Int, useCache: Boolean = true): HLabel = {
    HBaseModel.find[HLabel](useCache)(Seq(("id" -> id))).get
  }
  def findAll(useCache: Boolean = true): List[HLabel] = {
    HBaseModel.findsRange[HLabel](useCache)(Seq(("id" -> 0)), Seq(("id" -> Int.MaxValue)))
  }
  def findByList(key: String, id: Int, useCache: Boolean = true): List[HLabel] = {
    HBaseModel.findsMatch[HLabel](useCache)(Seq((key -> id)))
  }
  def findByTgtColumnId(columnId: Int, useCache: Boolean = true): List[HLabel] = {
    findByList("tgtColumnId", columnId, useCache)
  }
  def findBySrcColumnId(columnId: Int, useCache: Boolean = true): List[HLabel] = {
    findByList("srcColumnId", columnId, useCache)
  }
  def findBySrcServiceId(serviceId: Int, useCache: Boolean = true): List[HLabel] = {
    findByList("srcServiceId", serviceId, useCache)
  }
  def findByTgtServiceId(serviceId: Int, useCache: Boolean = true): List[HLabel] = {
    findByList("tgtServiceId", serviceId, useCache)
  }

  def insertAll(labelName: String, srcServiceName: String, srcColumnName: String, srcColumnType: String,
                tgtServiceName: String, tgtColumnName: String, tgtColumnType: String,
                isDirected: Boolean = true, serviceName: String,
                props: Seq[PROPS] = Seq.empty[PROPS],
                consistencyLevel: String,
                hTableName: Option[String],
                hTableTTL: Option[Int]) = {
    val newLabel = for {
      srcService <- HService.findByName(srcServiceName, useCache = false)
      tgtService <- HService.findByName(tgtServiceName, useCache = false)
      service <- HService.findByName(serviceName, useCache = false)
    } yield {
        val srcServiceId = srcService.id.get
        val tgtServiceId = tgtService.id.get
        val serviceId = service.id.get
        val srcCol = HServiceColumn.findOrInsert(srcServiceId, srcColumnName, Some(srcColumnType))
        val tgtCol = HServiceColumn.findOrInsert(tgtServiceId, tgtColumnName, Some(tgtColumnType))
        HLabel.findByName(labelName, useCache = false).getOrElse {
          val createdId = HBaseModel.getAndIncrSeq[HLabel]
          val label = HLabel(Map("id" -> createdId,
            "label" -> labelName, "srcServiceId" -> srcServiceId,
            "srcColumnName" -> srcColumnName, "srcColumnType" -> srcColumnType,
            "tgtServiceId" -> tgtServiceId, "tgtColumnName" -> tgtColumnName, "tgtColumnType" -> tgtColumnType,
            "isDirected" -> isDirected, "serviceName" -> serviceName, "serviceId" -> serviceId,
            "consistencyLevel" -> consistencyLevel, "hTableName" -> hTableName.getOrElse("s2graph-dev"),
            "hTableTTL" -> hTableTTL.getOrElse(-1)))
          label.create

          val (indexPropsMetas, propsMetas)  = {
            val metasWithIsIndex = if (props.isEmpty) List((HLabelMeta.timestamp, true))
            else props.toList.map {
              case (name, defaultVal, dataType, usedInIndex) =>
                (HLabelMeta.findOrInsert(createdId.toInt, name, defaultVal.toString, dataType), usedInIndex)
            }
            val (indexPropsMetas, propsMetas) = metasWithIsIndex.partition{ case (m, usedInIndex) => usedInIndex }
            (indexPropsMetas.map (t => t._1), propsMetas.map(t => t._1))
          }

          val indexMetaSeqs = indexPropsMetas.map { m => m.seq }
          /** insert default index(PK) of this label */
          HLabelIndex.findOrInsert(createdId.toInt, HLabelIndex.defaultSeq, indexMetaSeqs, "none")

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
      }
    newLabel.getOrElse(throw new RuntimeException("failed to create label"))
  }
}
case class HLabel(kvsParam: Map[KEY, VAL]) extends HBaseModel[HLabel]("HLabel", kvsParam) with JSONParser {
  override val columns = Seq("id", "label", "srcServiceId", "srcColumnName", "srcColumnType",
    "tgtServiceId", "tgtColumnName", "tgtColumnType", "isDirected", "serviceName", "serviceId",
    "consistencyLevel", "hTableName", "hTableTTL")

  val pk = Seq(("id", kvs("id")))
  val idxLabel = Seq(("label", kvs("label")))
  val idxSrcColumnName = Seq(("srcColumnName", kvs("srcColumnName")))
  val idxTgtColumnName = Seq(("tgtColumnName", kvs("tgtColumnName")))
  val idxSrcServiceId = Seq(("srcServiceId", kvs("srcServiceId")))
  val idxtgtServiceId = Seq(("tgtServiceId", kvs("tgtServiceId")))
  val idxServiceName = Seq(("serviceName", kvs("serviceName")))
  val idxServiceId = Seq(("serviceId", kvs("serviceId")))

  override val idxs = List(pk, idxLabel, idxSrcColumnName, idxTgtColumnName,
    idxSrcServiceId, idxtgtServiceId, idxServiceName, idxServiceId)
  override def foreignKeys() = {
    List(
      HBaseModel.findsMatch[HLabelIndex](useCache = false)(Seq("labelId" -> kvs("id"))),
      HBaseModel.findsMatch[HLabelMeta](useCache = false)(Seq("labelId" -> kvs("id")))
    )
  }
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val label = kvs("label").toString
  val srcServiceId = kvs("srcServiceId").toString.toInt
  val srcColumnName = kvs("srcColumnName").toString
  val srcColumnType = kvs("srcColumnType").toString

  val tgtServiceId = kvs("tgtServiceId").toString.toInt
  val tgtColumnName = kvs("tgtColumnName").toString
  val tgtColumnType = kvs("tgtColumnType").toString

  val isDirected = kvs("isDirected").toString.toBoolean
  val serviceName = kvs("serviceName").toString
  val serviceId = kvs("serviceId").toString.toInt

  val consistencyLevel = kvs("consistencyLevel").toString
  val hTableName = kvs("hTableName").toString
  val hTableTTL = {
    val ttl = kvs("hTableTTL").toString.toInt
    if (ttl < 0) None
    else Some(ttl)
  }


  def metas = HLabelMeta.findAllByLabelId(id.get)
  def metaSeqsToNames = metas.map(x => (x.seq, x.name)) toMap

  //  lazy val firstHBaseTableName = hbaseTableName.split(",").headOption.getOrElse(Config.HBASE_TABLE_NAME)
  lazy val srcService = HService.findById(srcServiceId)
  lazy val tgtService = HService.findById(tgtServiceId)
  lazy val service = HService.findById(serviceId)
  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, hTableName.split(",").head)
  lazy val (srcColumn, tgtColumn) = (HServiceColumn.find(srcServiceId, srcColumnName).get, HServiceColumn.find(tgtServiceId, tgtColumnName).get)
  lazy val direction = if (isDirected) "out" else "undirected"
  lazy val defaultIndex = HLabelIndex.findByLabelIdAndSeq(id.get, HLabelIndex.defaultSeq)

  //TODO: Make sure this is correct
  lazy val indices = HLabelIndex.findByLabelIdAll(id.get)
  lazy val indicesMap = indices.map(idx => (idx.seq, idx)) toMap
  lazy val indexSeqsMap = indices.map(idx => (idx.metaSeqs, idx)) toMap
  lazy val extraIndices = indices.filter(idx => defaultIndex.isDefined && idx.id.get != defaultIndex.get.id.get)
  //      indices filterNot (_.id.get == defaultIndex.get.id.get)
  lazy val extraIndicesMap = extraIndices.map(idx => (idx.seq, idx)) toMap

  lazy val metaProps = HLabelMeta.reservedMetas ::: HLabelMeta.findAllByLabelId(id.get)
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
    val orderByKeys = HLabelMeta.findAllByLabelId(id.get)
    super.toString() + orderByKeys.toString()
  }
  def findLabelIndexSeq(scoring: List[(Byte, Double)]): Byte = {
    if (scoring.isEmpty) HLabelIndex.defaultSeq
    else {
      HLabelIndex.findByLabelIdAndSeqs(id.get, scoring.map(_._1).sorted).map(_.seq).getOrElse(HLabelIndex.defaultSeq)
    }
  }

  lazy val toJson = Json.obj("labelName" -> label,
    "from" -> srcColumn.toJson, "to" -> tgtColumn.toJson,
    //    "indexProps" -> indexPropNames,
    "defaultIndex" -> defaultIndex.map(x => x.toJson),
    "extraIndex" -> extraIndices.map(exIdx => exIdx.toJson),
    "metaProps" -> metaProps.map(_.toJson) //    , "indices" -> indices.map(idx => idx.toJson)
  )
}
