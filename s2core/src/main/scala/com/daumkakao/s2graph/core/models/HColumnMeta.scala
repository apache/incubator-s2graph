//package com.daumkakao.s2graph.core.models
//
///**
// * Created by shon on 5/12/15.
// */
//case class HColumnMeta(tableName: String = "HColumnMeta", kvs: Map[String, String]) extends HBaseModel(tableName, kvs) {
//  import HBaseModel._
//  logicalTableName = "HColumnMeta"
//  val pk = KeyVals(Seq("columnId", "name"), Seq(columnId, name))
//  val pkVal = KeyVals(Seq("seq"), Seq(seq))
//  val idxByColumnIdSeq = KeyVals(Seq("columnId", "seq"), Seq(columnId, seq))
//  val idxValByColumnIdSeq = KeyVals(Seq("name"), Seq(name))
//
//  def insert(zkQuorum: String): Boolean = {
//    super.insert(zkQuorum)(id, pk, pkVal) &&
//    super.insert(zkQuorum)(id, idxByColumnIdSeq, idxValByColumnIdSeq)
//  }
//  override def parse(rowKey: String, qualifier: String, value: String): HColumnMeta = {
//
//    val components = rowKey.split(DELIMITER)
//    try {
//
//      val idxKVs = KeyVals(components.dropRight(1).mkString(DELIMITER)).toKVMap()
//      val metaKVs = KeyVals(value).toKVMap()
//      val kvs = idxKVs ++ metaKVs
//      HColumnMeta(kvs("id").toLong, kvs("columnId").toInt, kvs("name"), kvs("seq").toByte)
//    }
//  }
//}
