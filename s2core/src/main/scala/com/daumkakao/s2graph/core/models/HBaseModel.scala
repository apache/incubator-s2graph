package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.{Graph, GraphConnection}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, KeyValue, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object HBaseModel {
  val DELIMITER = ":"
  val KEY_VAL_DELIMITER = "^"
  val KEY_VAL_DELIMITER_WITH_ESCAPE = "\\^"
  val INNER_DELIMITER_WITH_ESCAPE = "\\|"
  val INNER_DELIMITER = "|"

  val modelTableName = "models"
  val modelCf = "m"
  val idQualifier = "i"
  val qualifier = "q"

  def toKVTuplesMap(s: String) = {
    val tupleLs = for {
      kv <- s.split(KEY_VAL_DELIMITER_WITH_ESCAPE)
      t = kv.split(INNER_DELIMITER_WITH_ESCAPE) if t.length == 2
    } yield (t.head, t.last)
    tupleLs.toMap
  }
  def fromResult(r: Result): Option[HBaseModel] = {
    if (r == null | r.isEmpty) None
    else {
       r.listCells().headOption.map { cell =>
        val rowKey = Bytes.toString(cell.getRow)
        val qualifier = Bytes.toString(cell.getQualifier)
        val value = Bytes.toString(cell.getValue)
        val elements = rowKey.split(DELIMITER)
        val (tName, idxKeyVals) = (elements(0), elements(1))
        val merged = toKVTuplesMap(idxKeyVals) ++ toKVTuplesMap(value)
        new HBaseModel(tName, merged)
      }
    }
  }
  def fromResultLs(r: Result): List[HBaseModel] = {
    if (r == null | r.isEmpty) List.empty[HBaseModel]
    else {
      r.listCells().map { cell =>
        val rowKey = Bytes.toString(cell.getRow)
        val qualifier = Bytes.toString(cell.getQualifier)
        val value = Bytes.toString(cell.getValue)
        val elements = rowKey.split(DELIMITER)
        val (tName, idxKeyVals) = (elements(0), elements(1))
        val merged = toKVTuplesMap(idxKeyVals) ++ toKVTuplesMap(value)
        new HBaseModel(tName, merged)
      } toList
    }
  }
  def toKVs(kvs: Seq[(String, String)]) =  {
    val idxKVs = for {
      (k, v) <- kvs
    } yield s"$k$INNER_DELIMITER$v"
    idxKVs.mkString(KEY_VAL_DELIMITER)
  }
  def toKVsWithFilter(kvs: Map[String, String],filterKeys: Seq[(String, String)]) = {
    val tgt = filterKeys.map(_._1).toSet
    val filtered = for {
      (k, v) <- kvs if !tgt.contains(k)
    } yield (k, v)
    filtered.toSeq
  }
  def toRowKey(tableName: String, idxKeyVals: Seq[(String, String)]) = {
    List(tableName, toKVs(idxKeyVals)).mkString(DELIMITER)
  }
  def find(zkQuorum: String)(tableName: String)(idxKeyVals: Seq[(String, String)]): Option[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val rowKey = toRowKey(tableName, idxKeyVals)
      val get = new Get(rowKey.getBytes)
      get.addColumn(modelCf.getBytes, qualifier.getBytes)
      get.setMaxVersions(1)
      val res = table.get(get)
      fromResult(res)
    } finally {
      table.close()
    }
  }
  def finds(zkQuorum: String)(tableName: String)(idxKeyVals: Seq[(String, String)], endIdxKeyVals: Seq[(String, String)]): List[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val scan = new Scan()
      scan.setStartRow(toRowKey(tableName, idxKeyVals).getBytes)
      scan.setStopRow(toRowKey(tableName, endIdxKeyVals).getBytes)
      scan.addColumn(modelCf.getBytes, qualifier.getBytes)
      val resScanner = table.getScanner(scan)
      val models = for {r <- resScanner; m <- fromResult(r)} yield m
      models.toList
    } finally {
      table.close()
    }
  }

  def getAndIncrSeq(zkQuorum: String)(tableName: String): Long = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      table.incrementColumnValue(tableName.getBytes, modelCf.getBytes, idQualifier.getBytes, 1L)
    } finally {
      table.close()
    }
  }

  def insert(zkQuorum: String)(tableName: String)(idxKVs: Seq[(String, String)], valKVs: Seq[(String, String)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      /** assumes using same hbase cluster **/
      val newSeq = getAndIncrSeq(zkQuorum)(tableName)
      val rowKey = toRowKey(tableName, idxKVs).getBytes
      val put = new Put(rowKey)
      put.addColumn(modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes)
      /** expecte null **/
      table.checkAndPut(rowKey, modelCf.getBytes, qualifier.getBytes, null, put)
    } finally {
      table.close()
    }
  }
  def delete(zkQuorum: String)(tableName: String)(idxKVs: Seq[(String, String)], valKVs: Seq[(String, String)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val rowKey = toRowKey(tableName, idxKVs).getBytes
      val delete = new Delete(rowKey)
      table.checkAndDelete(rowKey, modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes, delete)
    } finally {
      table.close()
    }
  }
}

/**
 */
class HBaseModel(protected val tableName: String, protected val kvs: Map[String, String]) {
  import HBaseModel._
  protected val columns = Seq.empty[String]
  protected val idxKVsList = List.empty[Seq[(String, String)]]
  override def toString(): String = (kvs ++ Map("tableName" -> tableName)).toString

  def validate(columns: Seq[String]): Unit = {
    for (c <- columns) {
      if (!kvs.contains(c)) throw new RuntimeException(s"tableName expect $columns, found $kvs")
    }
  }
  def create(zkQuorum: String) = {
    val f = HBaseModel.insert(zkQuorum)(tableName)_
    val rets = for {
      idxKVs <- idxKVsList
    } yield {
      f(idxKVs, toKVsWithFilter(kvs, idxKVs))
    }
    rets.forall(r => r)
  }
  def destroy(zkQuorum: String) = {
    val f = HBaseModel.delete(zkQuorum)(tableName)_
    val rets = for (idxKVs <- idxKVsList) yield {
      f(idxKVs, toKVsWithFilter(kvs, idxKVs))
    }
    rets.forall(r => r)
  }
}
case class HColumnMeta(kvsParam: Map[String, String]) extends HBaseModel("HColumnMeta", kvsParam) {
  override val columns = Seq("id", "columnId", "name", "seq")

  val pk = Seq(("id", kvs("id")))
  val columnIdName = Seq(("columnId", kvs("columnId")), ("name", kvs("name")))
  val columnIdSeq = Seq(("columnId", kvs("columnId")), ("seq", kvs("seq")))

  override val idxKVsList = List(pk, columnIdName, columnIdSeq)
  validate(columns)
}


case class HService(kvsParam: Map[String, String]) extends HBaseModel("HService", kvsParam) {
  override val columns = Seq("id", "serviceName", "cluster", "hbaseTableName", "preSplitSize", "hbaseTableTTL")

  val pk = Seq(("id", kvs("id")))
  val serviceName = Seq(("serviceName", kvs("serviceName")))
  val cluster = Seq(("cluster", kvs("cluster")))

  override val idxKVsList = List(pk, serviceName, cluster)
  validate(columns)
}


case class HServiceColumn(kvsParam: Map[String, String]) extends HBaseModel("HServiceColumn", kvsParam) {
  override val columns = Seq("id", "serviceId", "columnName", "columnType")
  val pk = Seq(("id", kvs("id")))
  val serviceIdColumnName = Seq(("serviceId", kvs("serviceId")), ("columnName", kvs("columnName")))
  override val idxKVsList = List(pk, serviceIdColumnName)
  validate(columns)
}

case class HLabelIndex(kvsParam: Map[String, String]) extends HBaseModel("HLabelIndex", kvsParam) {
  override val columns = Seq("id", "labelId", "seq", "metaSeqs", "formular")
  val pk = Seq(("id", kvs("id")))
  val labelIdSeq = Seq(("labelId", kvs("labelId")), ("metaSeqs", kvs("metaSeqs")))
  override val idxKVsList = List(pk, labelIdSeq)
  validate(columns)
  assert(!kvs("metaSeqs").isEmpty)
}
