package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.models.HBaseModel.{KEY, VAL}
import com.daumkakao.s2graph.core.{JSONParser, Management}
import play.api.Logger
import play.api.libs.json.{Json, JsValue}

/**
 * Created by shon on 5/15/15.
 */

object Label {
  val maxHBaseTableNames = 2
  type PROPS = (String, Any, String, Boolean)
  def findByName(labelUseCache: (String, Boolean)): Option[Label] = {
    findByName(labelUseCache._1, labelUseCache._2)
  }
  def findByName(label: String, useCache: Boolean = true): Option[Label] = {
    HBaseModel.find[Label](useCache)(Seq(("label" -> label)))
  }
  def findById(id: Int, useCache: Boolean = true): Label = {
    HBaseModel.find[Label](useCache)(Seq(("id" -> id))).get
  }
  def findAll(useCache: Boolean = true): List[Label] = {
    HBaseModel.findsRange[Label](useCache)(Seq(("id" -> 0)), Seq(("id" -> Int.MaxValue)))
  }
  def findByList(key: String, id: Int, useCache: Boolean = true): List[Label] = {
    HBaseModel.findsMatch[Label](useCache)(Seq((key -> id)))
  }
  def findByTgtColumnId(columnId: Int, useCache: Boolean = true): List[Label] = {
    findByList("tgtColumnId", columnId, useCache)
  }
  def findBySrcColumnId(columnId: Int, useCache: Boolean = true): List[Label] = {
    findByList("srcColumnId", columnId, useCache)
  }
  def findBySrcServiceId(serviceId: Int, useCache: Boolean = true): List[Label] = {
    findByList("srcServiceId", serviceId, useCache)
  }
  def findByTgtServiceId(serviceId: Int, useCache: Boolean = true): List[Label] = {
    findByList("tgtServiceId", serviceId, useCache)
  }

  def insertAll(labelName: String, srcServiceName: String, srcColumnName: String, srcColumnType: String,
                tgtServiceName: String, tgtColumnName: String, tgtColumnType: String,
                isDirected: Boolean = true, serviceName: String,
                idxProps: Seq[(String, JsValue, String)],
                props: Seq[(String, JsValue, String)],
                consistencyLevel: String,
                hTableName: Option[String],
                hTableTTL: Option[Int]) = {
    val srcServiceOpt = Service.findByName(srcServiceName, useCache = false)
    val tgtServiceOpt = Service.findByName(tgtServiceName, useCache = false)
    val serviceOpt = Service.findByName(serviceName, useCache = false)
    if (srcServiceOpt.isEmpty) throw new RuntimeException(s"source service $srcServiceName is not created.")
    if (tgtServiceOpt.isEmpty) throw new RuntimeException(s"target service $tgtServiceName is not created.")
    if (serviceOpt.isEmpty) throw new RuntimeException(s"service $serviceName is not created.")

    val newLabel = for {
      srcService <-srcServiceOpt
      tgtService <- tgtServiceOpt
      service <- serviceOpt
    } yield {
        val srcServiceId = srcService.id.get
        val tgtServiceId = tgtService.id.get
        val serviceId = service.id.get
        val srcCol = ServiceColumn.findOrInsert(srcServiceId, srcColumnName, Some(srcColumnType))
        val tgtCol = ServiceColumn.findOrInsert(tgtServiceId, tgtColumnName, Some(tgtColumnType))
        Label.findByName(labelName, useCache = false).getOrElse {
          /** create label */
          val createdId = HBaseModel.getAndIncrSeq[Label]
          val label = Label(Map("id" -> createdId,
            "label" -> labelName, "srcServiceId" -> srcServiceId,
            "srcColumnName" -> srcColumnName, "srcColumnType" -> srcColumnType,
            "tgtServiceId" -> tgtServiceId, "tgtColumnName" -> tgtColumnName, "tgtColumnType" -> tgtColumnType,
            "isDirected" -> isDirected, "serviceName" -> serviceName, "serviceId" -> serviceId,
            "consistencyLevel" -> consistencyLevel, "hTableName" -> hTableName.getOrElse("s2graph-dev"),
            "hTableTTL" -> hTableTTL.getOrElse(-1)))
          label.create

          /** create label metas */
          val idxPropsMetas =
            if (idxProps.isEmpty) List(LabelMeta.timestamp)
            else idxProps.map { case (propName, defaultValue, dataType) =>
              LabelMeta.findOrInsert(createdId.toInt, propName, defaultValue.toString, dataType)
            }
          val propsMetas = props.map { case (propName, defaultValue, dataType) =>
            LabelMeta.findOrInsert(createdId.toInt, propName, defaultValue.toString, dataType)
          }
          /** insert default index(PK) of this label */
          LabelIndex.findOrInsert(createdId.toInt, LabelIndex.defaultSeq,
            idxPropsMetas.map(m => m.seq).toList, "none")

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
case class Label(kvsParam: Map[KEY, VAL]) extends HBaseModel[Label]("HLabel", kvsParam) with JSONParser {
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
      HBaseModel.findsMatch[LabelIndex](useCache = false)(Seq("labelId" -> kvs("id"))),
      HBaseModel.findsMatch[LabelMeta](useCache = false)(Seq("labelId" -> kvs("id")))
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


  def metas = LabelMeta.findAllByLabelId(id.get)
  def metaSeqsToNames = metas.map(x => (x.seq, x.name)) toMap

  //  lazy val firstHBaseTableName = hbaseTableName.split(",").headOption.getOrElse(Config.HBASE_TABLE_NAME)
  lazy val srcService = Service.findById(srcServiceId)
  lazy val tgtService = Service.findById(tgtServiceId)
  lazy val service = Service.findById(serviceId)
  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, hTableName.split(",").head)
  lazy val (srcColumn, tgtColumn) = (ServiceColumn.find(srcServiceId, srcColumnName).get, ServiceColumn.find(tgtServiceId, tgtColumnName).get)
  lazy val direction = if (isDirected) "out" else "undirected"
  lazy val defaultIndex = LabelIndex.findByLabelIdAndSeq(id.get, LabelIndex.defaultSeq)

  //TODO: Make sure this is correct
  lazy val indices = LabelIndex.findByLabelIdAll(id.get)
  lazy val indicesMap = indices.map(idx => (idx.seq, idx)) toMap
  lazy val indexSeqsMap = indices.map(idx => (idx.metaSeqs, idx)) toMap
  lazy val extraIndices = indices.filter(idx => defaultIndex.isDefined && idx.id.get != defaultIndex.get.id.get)
  //      indices filterNot (_.id.get == defaultIndex.get.id.get)
  lazy val extraIndicesMap = extraIndices.map(idx => (idx.seq, idx)) toMap

  lazy val metaProps = LabelMeta.reservedMetas ::: LabelMeta.findAllByLabelId(id.get)
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
      LabelIndex.findByLabelIdAndSeqs(id.get, scoring.map(_._1).sorted).map(_.seq).getOrElse(LabelIndex.defaultSeq)
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
