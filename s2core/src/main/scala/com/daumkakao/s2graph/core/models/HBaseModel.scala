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
  var zkQuorum: String = "localhost"
  def apply(zk: String) = {
    this.zkQuorum = zk
  }
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
  def find(tableName: String)(idxKeyVals: Seq[(String, String)]): Option[HBaseModel] = {
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
  def findsRange(tableName: String)(idxKeyVals: Seq[(String, String)], endIdxKeyVals: Seq[(String, String)]): List[HBaseModel] = {
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
  def findsMatch(tableName: String)(idxKeyVals: Seq[(String, String)]): List[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val scan = new Scan()
      scan.setStartRow(toRowKey(tableName, idxKeyVals).getBytes)
      val endBytes = Bytes.add(toRowKey(tableName, idxKeyVals).getBytes, Array.fill(1)(Byte.MinValue.toByte))
      scan.setStopRow(endBytes)
      scan.addColumn(modelCf.getBytes, qualifier.getBytes)
      val resScanner = table.getScanner(scan)
      val models = for {r <- resScanner; m <- fromResult(r)} yield m
      models.toList
    } finally {
      table.close()
    }
  }
  def getAndIncrSeq(tableName: String): Long = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      table.incrementColumnValue(tableName.getBytes, modelCf.getBytes, idQualifier.getBytes, 1L)
    } finally {
      table.close()
    }
  }

  def insert(tableName: String)(idxKVs: Seq[(String, String)], valKVs: Seq[(String, String)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      /** assumes using same hbase cluster **/
      val newSeq = getAndIncrSeq(tableName)
      val rowKey = toRowKey(tableName, idxKVs).getBytes
      val put = new Put(rowKey)
      put.addColumn(modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes)
      /** expecte null **/
      table.checkAndPut(rowKey, modelCf.getBytes, qualifier.getBytes, null, put)
    } finally {
      table.close()
    }
  }
  def delete(tableName: String)(idxKVs: Seq[(String, String)], valKVs: Seq[(String, String)]) = {
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
  def create() = {
    val f = HBaseModel.insert(tableName)_
    val rets = for {
      idxKVs <- idxKVsList
    } yield {
      f(idxKVs, toKVsWithFilter(kvs, idxKVs))
    }
    rets.forall(r => r)
  }
  def destroy() = {
    val f = HBaseModel.delete(tableName)_
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

  val id = Some(kvs("id").toInt)
  val columnId = kvs("columnId").toInt
  val name = kvs("name")
  val seq = kvs("seq").toByte
  def findById(id: Int) = HBaseModel.find(tableName)(Seq(("id", s"$id")))
  def findsByColumn(columnId: Int) = HBaseModel.findsMatch(tableName)(Seq(("columnId", s"$columnId")))
  def findByColumnIdName(columnId: Int, name: String) = {
    HBaseModel.find(tableName)(Seq(("columnId", s"$columnId"), ("name", s"$name")))
  }
  def findOrInsert(columnId: Int, name: String) = {
    findByColumnIdName(columnId, name)}.getOrElse {
      val filtered = kvs.filter(kv => kv._1 != "columnId" && kv._1 != "name")
      val given = Map("columnId" -> s"$columnId", )
    }
  }
  // 1. findById
  // 2. findAllByColumn(columnId)
  // 3. findByName(columnId, name)
  // 4. insert
  // 5. findOrInsert(columnId, name)
  // 6. findByIdAndSeq(columnId, seq)
  // 7. delete

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
