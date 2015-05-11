package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.{Graph, GraphConnection}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, KeyValue, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import collection.JavaConversions._

abstract class HBaseModel[R <: HBaseModel[R]] {
  val modelTableName = "models"
  val modelCf = "m"
  val idTableName = "ids"
  val idRowKey = "id"
  val idCf = "i"
  val idQualifier = "seq"
  val DELIMITER = ":"


  def parse(id: Long, rowKey: String, qualifier: String, value: String): HBaseModel = ???
  def fromResult[R <: HBaseModel](r: Result): Option[HBaseModel] = {
    if (r == null | r.isEmpty) None
    r.listCells().headOption.map { cell =>
      val (rowKey, id) = toRowKeyWithId(cell.getRow)
      val qualifier = Bytes.toString(cell.getQualifier)
      val value = Bytes.toString(cell.getValue)
      parse(id, rowKey, qualifier, value)
    }
  }
  def fromResultLs(r: Result): List[HBaseModel] = {
    if (r == null | r.isEmpty) List.empty[HBaseModel]
    r.listCells().map { cell =>
      val (rowKey, id) = toRowKeyWithId(cell.getRow)
      val qualifier = Bytes.toString(cell.getQualifier)
      val value = Bytes.toString(cell.getValue)
      parse(id, rowKey, qualifier, value)
    } toList
  }
  def rowKeyWithId(id: Long, rk: String) = {
    s"$rk$DELIMITER$id"
  }
  def toRowKeyWithId(bytes: Array[Byte]) = {
    val tuple = Bytes.toString(bytes).split(DELIMITER)
    if (tuple.length < 2) throw new RuntimeException("decoding byte[] to valWithId failed.")
    (tuple.dropRight(1).mkString(DELIMITER), tuple.last.toLong)
  }
  def find(zkQuorum: String)(tableName: String = modelTableName, rowKey: String, cf: String = modelCf, qualifier: String): Option[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(tableName))
    try {
      val get = new Get(rowKey.getBytes)
      get.addColumn(cf.getBytes, qualifier.getBytes)
      get.setMaxVersions(1)
      val res = table.get(get)
      fromResult(res)
    } finally {
      table.close()
    }
  }
  def finds(zkQuorum: String)(tableName: String = modelTableName, rowKey: String, cf: String = modelCf, qualifier: String): List[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(tableName))
    try {
      val get = new Get(rowKey.getBytes)
      get.addColumn(cf.getBytes, qualifier.getBytes)
      get.setMaxVersions(1)
      val res = table.get(get)
      fromResultLs(res)
    } finally {
      table.close()
    }
  }
  def getAndIncrSeq(zkQuorum: String)(tableName: String = idTableName, rowKey: String, cf: String = idCf): Long = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(tableName))
    try {
      val incr = new Increment(rowKey.getBytes)
      incr.addColumn(cf.getBytes, idQualifier.getBytes, 1L)
      table.incrementColumnValue(rowKey.getBytes, cf.getBytes, idQualifier.getBytes, 1L)
    } finally {
      table.close()
    }
  }

  def insert(zkQuorum: String)(tableName: String = modelTableName, rowKey: String, cf: String = modelCf, qualifier: String, value: String) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(tableName))
    try {
      /** assumes using same hbase cluster **/
      val newSeq = getAndIncrSeq(zkQuorum)(tableName = idTableName, rowKey, cf = idCf)
      val put = new Put(rowKeyWithId(newSeq, rowKey).getBytes)
      put.addColumn(cf.getBytes, qualifier.getBytes, value.getBytes)
      /** expecte null **/
      table.checkAndPut(rowKeyWithId(newSeq, rowKey).getBytes,
        cf.getBytes, qualifier.getBytes, value.getBytes, null)
    } finally {
      table.close()
    }
  }
}
object ColumnKeyValues {
  def apply(s: String): ColumnKeyValues = {
    val t = s.split(":")
    if (t.length != 2) throw new RuntimeException("ColumnKeyValue parsing failed.")
    ColumnKeyValues(t.head.split("|").toSeq, t.last.split("|").toSeq)
  }
}
case class ColumnKeyValues(keys: Seq[String], values: Seq[String]) {
  override def toString(): String = s"${keys.mkString("|")}:${values.mkString("|")}"
  def toKVMap() = keys.zip(values).map { kv => kv._1 -> kv._2 } toMap
}
case class HColumnMeta(id: Long, columnId: Int, name: String, seq: Byte) extends HBaseModel[HColumnMeta] {
  val idxs = Map("columnId|name" -> s"$columnId|$name", "columnId|seq" -> s"$columnId|$seq")
  override def parse(id: Long, rowKey: String, qualifier: String, value: String): HColumnMeta = {
    val components = rowKey.split(DELIMITER)
    try {
      // rowKey: idxValues, idxKeys, logicalTableName
      // qualifier: dummy
      val logicalTableName = components.last
      val idxKVs = ColumnKeyValues(components.dropRight(1).mkString(DELIMITER)).toKVMap()
      val metaKVs = ColumnKeyValues(value).toKVMap()
      val kvs = idxKVs ++ metaKVs
      HColumnMeta(id, kvs("columnId").toInt, kvs("name"), kvs("seq").toByte)
    }
  }
}