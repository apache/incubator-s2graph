package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.models.Model.{KEY, VAL}
import com.daumkakao.s2graph.core.types2.{HBaseType, InnerVal}
import com.daumkakao.s2graph.core.{GraphUtil, JSONParser, Management}
import org.apache.hadoop.hbase.client.Result
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
    Model.find[Label](useCache)(Seq(("label" -> label)))
  }
  def findById(id: Int, useCache: Boolean = true): Label = {
    Model.find[Label](useCache)(Seq(("id" -> id))).get
  }
  def findAll(useCache: Boolean = true): List[Label] = {
    Model.findsRange[Label](useCache)(Seq(("id" -> 0)), Seq(("id" -> Int.MaxValue)))
  }
  def findByList(key: String, id: Int, useCache: Boolean = true): List[Label] = {
    Model.findsMatch[Label](useCache)(Seq((key -> id)))
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
      srcService <-srcServiceOpt
      tgtService <- tgtServiceOpt
      service <- serviceOpt
    } yield {
        val srcServiceId = srcService.id.get
        val tgtServiceId = tgtService.id.get
        val serviceId = service.id.get
        /** insert serviceColumn */
        val srcCol = ServiceColumn.findOrInsert(srcServiceId, srcColumnName, Some(srcColumnType), schemaVersion)
        val tgtCol = ServiceColumn.findOrInsert(tgtServiceId, tgtColumnName, Some(tgtColumnType), schemaVersion)


        /** create label */
        Label.findByName(labelName, useCache = false).getOrElse {
          val createdId = Model.getAndIncrSeq[Label]
          val label = Label(Map("id" -> createdId,
            "label" -> labelName, "srcServiceId" -> srcServiceId,
            "srcColumnName" -> srcColumnName, "srcColumnType" -> srcColumnType,
            "tgtServiceId" -> tgtServiceId, "tgtColumnName" -> tgtColumnName, "tgtColumnType" -> tgtColumnType,
            "isDirected" -> isDirected, "serviceName" -> serviceName, "serviceId" -> serviceId,
            "consistencyLevel" -> consistencyLevel, "hTableName" -> hTableName.getOrElse("s2graph-dev"),
            "hTableTTL" -> hTableTTL.getOrElse(-1),
            "schemaVersion" -> schemaVersion, "isAsync" -> isAsync))
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
              Management.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, service.hTableTTL, compressionAlgorithm)
            case (Some(hbaseTableName), Some(hbaseTableTTL)) =>
              // create own hbase table with own ttl.
              Management.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, hTableTTL, compressionAlgorithm)
          }
        }
      }
    newLabel.getOrElse(throw new RuntimeException("failed to create label"))
  }
}
case class Label(kvsParam: Map[KEY, VAL]) extends Model[Label]("HLabel", kvsParam) with JSONParser {
  override val columns = Seq("id", "label", "srcServiceId", "srcColumnName", "srcColumnType",
    "tgtServiceId", "tgtColumnName", "tgtColumnType", "isDirected", "serviceName", "serviceId",
    "consistencyLevel", "hTableName", "hTableTTL", "schemaVersion", "isAsync")

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
      Model.findsMatch[LabelIndex](useCache = false)(Seq("labelId" -> kvs("id"))),
      Model.findsMatch[LabelMeta](useCache = false)(Seq("labelId" -> kvs("id")))
    )
  }
  validate(columns, Seq("schemaVersion"))

  val schemaVersion = kvs.get("schemaVersion").getOrElse(HBaseType.DEFAULT_VERSION).toString
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
  val isAsync = kvs.get("isAsync").map(_.toString.toBoolean).getOrElse(false)


  /** all properties belongs to this label */
  def metas = LabelMeta.findAllByLabelId(id.get)

  def metaSeqsToNames = metas.map(x => (x.seq, x.name)) toMap

  val useCache = true
  //  lazy val firstHBaseTableName = hbaseTableName.split(",").headOption.getOrElse(Config.HBASE_TABLE_NAME)
  lazy val srcService = Service.findById(srcServiceId, useCache)
  lazy val tgtService = Service.findById(tgtServiceId, useCache)
  lazy val service = Service.findById(serviceId, useCache)
  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, hTableName.split(",").head)
  lazy val (srcColumn, tgtColumn) = (ServiceColumn.find(srcServiceId, srcColumnName).get, ServiceColumn.find(tgtServiceId, tgtColumnName).get)
  lazy val direction = if (isDirected) "out" else "undirected"
  lazy val defaultIndex = LabelIndex.findByLabelIdAndSeq(id.get, LabelIndex.defaultSeq, useCache)

  //TODO: Make sure this is correct
  lazy val indices = LabelIndex.findByLabelIdAll(id.get)
//  lazy val defaultIndex = indices.filter(idx => idx.seq == LabelIndex.defaultSeq).headOption
  lazy val indicesMap = indices.map(idx => (idx.seq, idx)) toMap
  lazy val indexSeqsMap = indices.map(idx => (idx.metaSeqs, idx)) toMap
  lazy val extraIndices = indices.filter(idx => defaultIndex.isDefined && idx.id.get != defaultIndex.get.id.get)
  //      indices filterNot (_.id.get == defaultIndex.get.id.get)
  lazy val extraIndicesMap = extraIndices.map(idx => (idx.seq, idx)) toMap

  lazy val metaProps = LabelMeta.reservedMetas ::: LabelMeta.findAllByLabelId(id.get, useCache)
  lazy val metaPropsMap = metaProps.map(x => (x.seq, x)).toMap
  lazy val metaPropsInvMap = metaProps.map(x => (x.name, x)).toMap
  lazy val metaPropNames = metaProps.map(x => x.name)
  lazy val metaPropNamesMap = metaProps.map(x => (x.seq, x.name)) toMap

  def srcColumnWithDir(dir: Int) = {
    if (isDirected) {
      if (dir == GraphUtil.directions("out")) srcColumn else tgtColumn
    } else {
      srcColumn
    }
  }
  def tgtColumnWithDir(dir: Int) = {
    if (isDirected) {
      if (dir == GraphUtil.directions("out")) tgtColumn else srcColumn
    } else {
      tgtColumn
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
//    jsValueToInnerVal(jsValue, srcColumnType, schemaVersion)
//  }
//  def tgtColumnInnerVal(jsValue: JsValue) = {
//    jsValueToInnerVal(jsValue, tgtColumnType, schemaVersion)
//  }

//  override def toString(): String = {
//    val orderByKeys = LabelMeta.findAllByLabelId(id.get)
//    super.toString() + orderByKeys.toString()
//  }
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
