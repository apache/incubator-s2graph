package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.models.HBaseModel.{VAL, KEY}
import play.api.libs.json.Json

/**
 * Created by shon on 5/15/15.
 */

object LabelIndex {
  val timestamp = LabelIndex(Map("id" -> "0", "labelId" -> 0, "seq" -> 0.toByte, "metaSeqs" -> "0", "formular" -> ""))
  //  val withTsSeq = 0.toByte
  val defaultSeq = 1.toByte
  val maxOrderSeq = 7
  import HBaseModel._

  def findById(id: Int, useCache: Boolean = true): LabelIndex = {
    HBaseModel.find[LabelIndex](useCache)(Seq(("id" -> id))).get
  }
  def findByLabelIdAll(labelId: Int, useCache: Boolean = true): List[LabelIndex] = {
    HBaseModel.findsMatch[LabelIndex](useCache)(Seq(("labelId" -> labelId)))
  }
  def findByLabelIdAndSeq(labelId: Int, seq: Byte, useCache: Boolean = true): Option[LabelIndex] = {
    HBaseModel.find[LabelIndex](useCache)(Seq(("labelId" -> labelId), ("seq" -> seq)))
  }
  def findByLabelIdAndSeqs(labelId: Int, seqs: List[Byte], useCache: Boolean = true): Option[LabelIndex] = {
    HBaseModel.find[LabelIndex](useCache)(Seq(("labelId" -> labelId), ("metaSeqs" -> seqs.mkString(HBaseModel.META_SEQ_DELIMITER))))
  }
  def findOrInsert(labelId: Int, seq: Byte, metaSeqs: List[Byte], formular: String): LabelIndex = {
    findByLabelIdAndSeq(labelId, seq, useCache = false) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq[LabelIndex]
        val model = LabelIndex(Map("id" -> id, "labelId" -> labelId,
          "seq" -> seq, "metaSeqs" -> metaSeqs.mkString(META_SEQ_DELIMITER), "formular" -> formular))
        model.create
        model
    }
  }
  def findOrInsert(labelId: Int, metaSeqs: List[Byte], formular: String): LabelIndex = {
    findByLabelIdAndSeqs(labelId, metaSeqs, useCache = false) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq[LabelIndex]
        val indices = LabelIndex.findByLabelIdAll(labelId, useCache = false)
        val seq = (indices.length + 1).toByte
        val model = LabelIndex(Map("id" -> id, "labelId" -> labelId,
          "seq" -> seq, "metaSeqs" -> metaSeqs.mkString(META_SEQ_DELIMITER), "formular" -> formular))
        model.create
        model
    }
  }
}
case class LabelIndex(kvsParam: Map[KEY, VAL]) extends HBaseModel[LabelIndex]("HLabelIndex", kvsParam) {
  override val columns = Seq("id", "labelId", "seq", "metaSeqs", "formular")
  val pk = Seq(("id", kvs("id")))
  val labelIdSeq = Seq(("labelId", kvs("labelId")), ("seq", kvs("seq")))
  val labelIdMetaSeqs = Seq(("labelId", kvs("labelId")), ("metaSeqs", kvs("metaSeqs")))
  override val idxs = List(pk, labelIdSeq, labelIdMetaSeqs)
  validate(columns)

  import HBaseModel._

  val id = Some(kvs("id").toString.toInt)
  val labelId = kvs("labelId").toString.toInt
  val seq = kvs("seq").toString.toByte
  val metaSeqs = kvs("metaSeqs").toString.split(META_SEQ_DELIMITER).map(x => x.toByte).toList
  val formular = kvs("formular").toString

  lazy val label = Label.findById(labelId)
  lazy val metas = label.metaPropsMap
  lazy val sortKeyTypes = metaSeqs.map(metaSeq => label.metaPropsMap.get(metaSeq)).flatten
//  lazy val sortKeyTypeDefaultVals = sortKeyTypes.map(x => x.defaultInnerVal)
  lazy val toJson = Json.obj("indexProps" -> sortKeyTypes.map(x => x.name))

}
