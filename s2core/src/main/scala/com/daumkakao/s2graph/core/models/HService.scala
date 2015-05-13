//package com.daumkakao.s2graph.core.models
//
///**
// * Created by shon on 5/12/15.
// */
//
//
//case class HService(id: Long,
//                    serviceName: String,
//                    cluster: String,
//                    hbaseTableName: String,
//                    preSplitSize: Int = 0,
//                    hbaseTableTTL: Int = Int.MaxValue) extends HBaseModel {
//  import HBaseModel._
//  logicalTableName = "HService"
//  val pk = KeyVals(Seq("id"), Seq(s"$id"))
//  val pkVal = KeyVals(Seq("serviceName", "cluster", "hbaseTableName", "preSplitSize", "hbaseTableTTL"),
//  Seq(serviceName, cluster, hbaseTableName, s"$preSplitSize", s"$hbaseTableTTL"))
//
//  val idxByServiceName = KeyVals(Seq("serviceName"), Seq(s"$serviceName"))
//  val idxValByServiceName = KeyVals(
//      Seq("cluster", "hbaseTableName", "preSplitSize", "hbaseTableTTL"),
//      Seq(s"$cluster", s"$hbaseTableName", s"$preSplitSize", s"$hbaseTableTTL"))
//  val idxByCluster = KeyVals(Seq("cluster"), Seq(s"$cluster"))
//  val idxValByCluster = KeyVals(
//      Seq("serviceName","hbaseTableName", "preSplitSize", "hbaseTableTTL"),
//      Seq(s"$serviceName", s"$hbaseTableName", s"$preSplitSize", s"$hbaseTableTTL"))
//
//  def insert(zkQuorum: String) =
//    super.insert(zkQuorum)(id, pk, pkVal) &&
//    super.insert(zkQuorum)(id, idxByServiceName, idxValByServiceName) &&
//    super.insert(zkQuorum)(id, idxByCluster, idxValByCluster)
//
//  override def parse(rowKey: String, qualifier: String, value: String): HService = {
//
//    val components = rowKey.split(DELIMITER)
//    try {
//      val idxKVs = KeyVals(components.dropRight(1).mkString(DELIMITER)).toKVMap()
//      val metaKVs = KeyVals(value).toKVMap()
//      val kvs = idxKVs ++ metaKVs
//      HService(kvs("id").toLong, kvs("serviceName"), kvs("cluster"), kvs("hbaseTableName"), kvs("preSplitSize").toInt, kvs("hbaseTableTTL").toInt)
//    }
//  }
//}
