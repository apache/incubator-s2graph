//package com.daumkakao.s2graph.core.models
//
//import com.daumkakao.s2graph.core.models.HBaseModel._
//
///**
// * Created by shon on 5/12/15.
// */
////ALTER TABLE service_columns add FOREIGN KEY(service_id) REFERENCES services(id) ON DELETE CASCADE;
//
//case class HServiceColumn(id: Long, serviceId: Int, columnName: String, columnType: String) extends HBaseModel {
//  logicalTableName = "HServiceColumn"
//  val pk = KeyVals(Seq("id"), Seq(id))
//  val pkVal = KeyVals(Seq("serviceId", "columnName", "columnType"), Seq(serviceId, columnName, columnType))
//
//  val idxByServiceIdColumnName = KeyVals(Seq("serviceId", "columnName"), Seq(serviceId, columnName))
//  val idxValByServiceIdColumnName = KeyVals(Seq("columnType"), Seq(columnType))
//
//  def insert(zkQuorum: String) =
//    super.insert(zkQuorum)(id, pk, pkVal) &&
//    super.insert(zkQuorum)(id, idxByServiceIdColumnName, idxValByServiceIdColumnName)
//
//  override def parse(rowKey: String, qualifier: String, value: String): HServiceColumn = {
//    val components = rowKey.split(DELIMITER)
//    println(rowKey, qualifier, value)
//    try {
//      val idxKVs = KeyVals(components.dropRight(1).mkString(DELIMITER)).toKVMap()
//      val metaKVs = KeyVals(value).toKVMap()
//      val kvs = idxKVs ++ metaKVs
//      HServiceColumn(kvs("id").toLong, kvs("serviceId").toInt, kvs("columnName"), kvs("columnType"))
//    }
//  }
//}
