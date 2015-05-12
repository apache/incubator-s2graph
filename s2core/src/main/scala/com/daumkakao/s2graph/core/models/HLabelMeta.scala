package com.daumkakao.s2graph.core.models

/**
 * Created by shon on 5/12/15.
 */

//ALTER TABLE label_metas ADD FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE;
case class HLabelMeta(id: Long, labelId: Int, name: String, seq: Byte, defaultVal: String,
                 dataType: String, usedInIndex: Boolean) extends HBaseModel {
  import HBaseModel._
  logicalTableName = "HLabelMeta"
  val pk = KeyVals(Seq("id"), Seq(id))
  val pkVal = KeyVals(Seq("labelId", "name", "seq", "defaultVal", "dataType", "usedInIndex"),
  Seq(labelId, name, seq, defaultVal, dataType, usedInIndex))

  val idxByLabelIdName = KeyVals(Seq("labelId", "name"), Seq(labelId, name))
  val idxValByLabelIdName = KeyVals(Seq("seq", "defaultVal", "dataType", "usedInIndex"),
  Seq(seq, defaultVal, dataType, usedInIndex))

  val idxByLabelIdSeq = KeyVals(Seq("labelId", "seq"), Seq(labelId, seq))
  val idxValByLabelIdSeq = KeyVals(Seq("name", "defaultVal", "dataType", "usedInIndex"),
  Seq(name, defaultVal, dataType, usedInIndex))

  def insert(zkQuorum: String): Boolean =
    super.insert(zkQuorum)(id, pk, pkVal) &&
    super.insert(zkQuorum)(id, idxByLabelIdName, idxValByLabelIdName) &&
    super.insert(zkQuorum)(id, idxByLabelIdSeq, idxValByLabelIdSeq)

  override def parse(rowKey: String, qualifier: String, value: String): HLabelMeta = {

    val components = rowKey.split(DELIMITER)
    try {

      val idxKVs = KeyVals(components.dropRight(1).mkString(DELIMITER)).toKVMap()
      val metaKVs = KeyVals(value).toKVMap()
      val kvs = idxKVs ++ metaKVs
      HLabelMeta(kvs("id").toLong, kvs("labelId").toInt, kvs("name"), kvs("seq").toByte, kvs("defaultVal"),
      kvs("dataType"), kvs("usedInIndex").toBoolean)
    }
  }

}
