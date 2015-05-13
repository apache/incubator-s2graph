//package com.daumkakao.s2graph.core.models
//
///**
// * Created by shon on 5/12/15.
// */
////ALTER TABLE label_indices ADD FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE;
//case class HLabelIndex(id: Long, labelId: Int, seq: Byte, metaSeqs: String) extends HBaseModel {
//  import HBaseModel._
//  logicalTableName = "HLabelIndex"
//  val pk = KeyVals(Seq("id"), Seq(id))
//  val pkVal = KeyVals(Seq("labelId", "seq", "metaSeqs"), Seq(labelId, seq, metaSeqs))
//  val idxByLabelIdMetaSeqs = KeyVals(Seq("labelId", "metaSeqs"), Seq(labelId, metaSeqs))
//  val idxValByLabelIdMetaSeqs = KeyVals(Seq("seq"), Seq(seq))
//  def insert(zkQuorum: String): Boolean =
//    super.insert(zkQuorum)(id, pk, pkVal) &&
//    super.insert(zkQuorum)(id, idxByLabelIdMetaSeqs, idxValByLabelIdMetaSeqs)
//  override def parse(rowKey: String, qualifier: String, value: String): HLabelIndex = {
//
//    val components = rowKey.split(DELIMITER)
//    try {
//
//      val idxKVs = KeyVals(components.dropRight(1).mkString(DELIMITER)).toKVMap()
//      val metaKVs = KeyVals(value).toKVMap()
//      val kvs = idxKVs ++ metaKVs
//      HLabelIndex(kvs("id").toLong, kvs("labelId").toInt, kvs("seq").toByte, kvs("metaSeqs"))
//    }
//  }
//}
