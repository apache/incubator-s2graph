package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.JSONParser
import com.daumkakao.s2graph.core.models.HBaseModel.{VAL, KEY}
//import com.daumkakao.s2graph.core.types2.InnerVal

//import com.daumkakao.s2graph.core.types.InnerVal
import play.api.libs.json.{Json, JsObject, JsValue}

/**
 * Created by shon on 5/15/15.
 */

object LabelMeta extends JSONParser {

  /** dummy sequences */
  val fromSeq = -4.toByte
  val toSeq = -5.toByte
  val lastOpSeq = -3.toByte
  val lastDeletedAt = -2.toByte
  val timeStampSeq = 0.toByte
  val countSeq = -1.toByte
  val degreeSeq = (Byte.MaxValue - 1).toByte
  val maxValue = Byte.MaxValue
  val emptyValue = Byte.MaxValue

  /** reserved sequences */
  val from = LabelMeta(Map("id" -> fromSeq, "labelId" -> fromSeq, "name" -> "_from", "seq" -> fromSeq,
    "defaultValue" -> fromSeq.toString,
    "dataType" -> "long"))
  val to = LabelMeta(Map("id" -> toSeq, "labelId" -> toSeq, "name" -> "_to", "seq" -> toSeq,
    "defaultValue" -> toSeq.toString, "dataType" -> "long"))
  val timestamp = LabelMeta(Map("id" -> -1, "labelId" -> -1, "name" -> "_timestamp", "seq" -> timeStampSeq,
    "defaultValue" -> "0", "dataType" -> "long"))
  val degree = LabelMeta(Map("id" -> -1, "labelId" -> -1, "name" -> "_degree", "seq" -> degreeSeq,
    "defaultValue" -> 0, "dataType" -> "long"))

  val reservedMetas = List(from, to, timestamp)
  val notExistSeqInDB = List(lastOpSeq, lastDeletedAt, countSeq, timeStampSeq, degreeSeq, from.seq, to.seq)

  def findById(id: Int, useCache: Boolean = true): LabelMeta = {
    HBaseModel.find[LabelMeta](useCache)(Seq(("id" -> id))).get
  }

  def findAllByLabelId(labelId: Int, useCache: Boolean = true): List[LabelMeta] = {
    HBaseModel.findsMatch[LabelMeta](useCache)(Seq(("labelId" -> labelId))) ++
      List(degree)
  }

  def findByName(labelId: Int, name: String, useCache: Boolean = true): Option[LabelMeta] = {
    name match {
      case timestamp.name => Some(timestamp)
      case to.name => Some(to)
      case _ =>
        HBaseModel.find[LabelMeta](useCache)(Seq(("labelId" -> labelId), ("name" -> name)))
    }
  }

  def findOrInsert(labelId: Int, name: String, defaultValue: String, dataType: String): LabelMeta = {
    findByName(labelId, name, useCache = false) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq[LabelMeta]
        val allMetas = findAllByLabelId(labelId, useCache = false)
        val seq = (allMetas.length + 1).toByte
        val model = LabelMeta(Map("id" -> id, "labelId" -> labelId, "name" -> name, "seq" -> seq,
          "defaultValue" -> defaultValue, "dataType" -> dataType))
        model.create
        model
    }
  }

//  def convert(labelId: Int, jsValue: JsValue): Map[Byte, InnerVal] = {
//    val ret = for {
//      (k, v) <- jsValue.as[JsObject].fields
//      meta <- LabelMeta.findByName(labelId, k)
//      innerVal <- jsValueToInnerVal(v, meta.dataType)
//    } yield (meta.seq, innerVal)
//    ret.toMap
//  }
}

case class LabelMeta(kvsParam: Map[KEY, VAL])
  extends HBaseModel[LabelMeta]("HLabelMeta", kvsParam) with JSONParser {

  override val columns = Seq("id", "labelId", "name", "seq", "defaultValue", "dataType")
  val pk = Seq(("id", kvs("id")))
  val idxLabelIdName = Seq(("labelId", kvs("labelId")), ("name", kvs("name")))
  val idxLabelIdSeq = Seq(("labelId", kvs("labelId")), ("seq", kvs("seq")))
  override val idxs = List(pk, idxLabelIdName, idxLabelIdSeq)
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val labelId = kvs("labelId").toString.toInt
  val name = kvs("name").toString
  val seq = kvs("seq").toString.toByte
  val defaultValue = kvs("defaultValue").toString
  val dataType = kvs("dataType").toString
  //  val usedInIndex = kvs("usedInIndex").toString.toBoolean
  lazy val toJson = Json.obj("name" -> name, "defaultValue" -> defaultValue, "dataType" -> dataType)
}
