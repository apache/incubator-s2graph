package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.{Graph, GraphConnection}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, KeyValue, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import collection.JavaConversions._

object HBaseModel {
  val DELIMITER = ":"
  val INNER_DELIMITER_WITH_ESCAPE = "\\|"
  val INNER_DELIMITER = "|"
}

/**
 */
abstract class HBaseModel {
  import HBaseModel._

  val modelTableName = "models"
  val modelCf = "m"
  val idQualifier = "i"
  val qualifier = "q"
  protected var logicalTableName = "HBaseModel"
  protected def parse(rowKey: String, qualifier: String, value: String): HBaseModel = ???
  def toRowKey(idxKeyVals: KeyVals) = s"${idxKeyVals}$DELIMITER$logicalTableName"
  def valueOfRowKey(bytes: Array[Byte]) = {
    Bytes.toString(bytes)
  }

  def fromResult[R <: HBaseModel](r: Result): Option[HBaseModel] = {
    if (r == null | r.isEmpty) None
    r.listCells().headOption.map { cell =>
      val rowKey = valueOfRowKey(cell.getRow)
      val qualifier = Bytes.toString(cell.getQualifier)
      val value = Bytes.toString(cell.getValue)
      parse(rowKey, qualifier, value)
    }
  }
  def fromResultLs(r: Result): List[HBaseModel] = {
    if (r == null | r.isEmpty) List.empty[HBaseModel]
    r.listCells().map { cell =>
      val rowKey = valueOfRowKey(cell.getRow)
      val qualifier = Bytes.toString(cell.getQualifier)
      val value = Bytes.toString(cell.getValue)
      println(rowKey, qualifier, value)
      parse(rowKey, qualifier, value)
    } toList
  }

  def find(zkQuorum: String)(idxKeyVals: KeyVals): Option[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val get = new Get(toRowKey(idxKeyVals).getBytes)
      get.addColumn(modelCf.getBytes, qualifier.getBytes)
      get.setMaxVersions(1)
      val res = table.get(get)
      fromResult(res)
    } finally {
      table.close()
    }
  }
  def finds(zkQuorum: String)(idxKeyVals: KeyVals): List[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val get = new Get(toRowKey(idxKeyVals).getBytes)
      get.addColumn(modelCf.getBytes, qualifier.getBytes)
      get.setMaxVersions(1)
      val res = table.get(get)
      fromResultLs(res)
    } finally {
      table.close()
    }
  }
  def getAndIncrSeq(zkQuorum: String, tableName: String = logicalTableName): Long = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      table.incrementColumnValue(tableName.getBytes, modelCf.getBytes, idQualifier.getBytes, 1L)
    } finally {
      table.close()
    }
  }

  def insert(zkQuorum: String)(id: Long, idxKeyVals: KeyVals, value: KeyVals) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      /** assumes using same hbase cluster **/
      val newSeq = getAndIncrSeq(zkQuorum)
      val put = new Put(toRowKey(idxKeyVals).getBytes)
      put.addColumn(modelCf.getBytes, qualifier.getBytes, value.append("id", s"$id").toString.getBytes)
      /** expecte null **/
      table.checkAndPut(toRowKey(idxKeyVals).getBytes, modelCf.getBytes, qualifier.getBytes, null, put)
    } finally {
      table.close()
    }
  }

}
object KeyVals {
  import HBaseModel._
  def apply(s: String): KeyVals = {
    val t = s.split(DELIMITER)
    if (t.length != 2) throw new RuntimeException("ColumnKeyValue parsing failed.")
    val (vals, keys) = (t.head.split(INNER_DELIMITER_WITH_ESCAPE).toList, t.last.split(INNER_DELIMITER_WITH_ESCAPE).toList)
    KeyVals(keys, vals)
  }
}
case class KeyVals(keys: Seq[Any], values: Seq[Any]) {
  import HBaseModel._
  override def toString(): String = s"${values.mkString(INNER_DELIMITER)}$DELIMITER${keys.mkString(INNER_DELIMITER)}"
  def toKVMap() = keys.zip(values).map { kv => kv._1.toString -> kv._2.toString } toMap
  def append(key: String, value: String) = {
    KeyVals(key +: keys, value +: values)
  }
}
