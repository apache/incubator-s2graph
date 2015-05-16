package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.models.HBaseModel.{KEY, VAL}
import com.daumkakao.s2graph.core._
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName}
import org.apache.hadoop.hbase.client._
import collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}


object HBaseModel extends LocalCache[HBaseModel] {
  val DELIMITER = ":"
  val KEY_VAL_DELIMITER = "^"
  val KEY_VAL_DELIMITER_WITH_ESCAPE = "\\^"
  val INNER_DELIMITER_WITH_ESCAPE = "\\|"
  val INNER_DELIMITER = "|"
  val META_SEQ_DELIMITER = "~"
  val modelTableName = s"models-${GraphConnection.defaultConfigs("phase")}"
  val modelCf = "m"
  val idQualifier = "i"
  val qualifier = "q"
  var zkQuorum: String = "localhost"
  var cacheTTL: Int = 10
  var maxCacheSize: Int = 1000
  type KEY = String
  type VAL = Any

  def apply(config: Config) = {
    zkQuorum = GraphConnection.getOrElse(config)("hbase.zookeeper.quorum", zkQuorum)
    cacheTTL = GraphConnection.getOrElse(config)("cache.ttl.seconds", cacheTTL)
    maxCacheSize = GraphConnection.getOrElse(config)("cache.max.size", maxCacheSize)
  }
//  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]
//  def newInstance[T](clazz: Class)(implicit tag: ru.TypeTag[T]) = {
//    val m = ru.runtimeMirror(getClass.getClassLoader)
//    ru.typeTag[]
//    val symbol = ru.typeOf[].typeSymbol.asClass
//  }
  def newInstance(tableName: String)(kvs: Map[KEY, VAL]) = {
    tableName match {
      case "HService" => HService(kvs)
      case "HServiceColumn" => HServiceColumn(kvs)
      case "HColumnMeta" => HColumnMeta(kvs)
      case "HLabelMeta" => HLabelMeta(kvs)
      case "HLabelIndex" => HLabelIndex(kvs)
      case "HLabel" => HLabel(kvs)
      case _ => new HBaseModel(tableName, kvs)
    }
  }
  def apply(zkQuorum: String) = {
    this.zkQuorum = zkQuorum
  }
  def padZeros(v: VAL): String = {
    v match {
      case b: Byte => "%03d".format(b)
      case s: Short => "%05d".format(s)
      case i: Int => "%08d".format(i)
      case l: Long => "%08d".format(l)
      case ls: List[Any] => ls.mkString(",")
      case _ => v.toString
    }
  }

  def toKVTuplesMap(s: String) = {
    val tupleLs = for {
      kv <- s.split(KEY_VAL_DELIMITER_WITH_ESCAPE)
      t = kv.split(INNER_DELIMITER_WITH_ESCAPE) if t.length == 2
    } yield (t.head, t.last)
    tupleLs.toMap
  }
  def toKVs(kvs: Seq[(KEY, VAL)]) =  {
    val idxKVs = for {
      (k, v) <- kvs
    } yield s"$k$INNER_DELIMITER${padZeros(v)}"
    idxKVs.mkString(KEY_VAL_DELIMITER)
  }
  def toKVsWithFilter(kvs: Map[KEY, VAL],filterKeys: Seq[(KEY, VAL)]) = {
    val tgt = filterKeys.map(_._1).toSet
    val filtered = for {
      (k, v) <- kvs if !tgt.contains(k)
    } yield (k, padZeros(v))
    filtered.toSeq
  }
  def toRowKey(tableName: String, idxKeyVals: Seq[(KEY, VAL)]) = {
    List(tableName, toKVs(idxKeyVals)).mkString(DELIMITER)
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
        newInstance(tName)(merged)
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
        newInstance(tName)(merged)
      } toList
    }
  }
  def toCacheKey(kvs: Seq[(KEY, VAL)]) = kvs.map { kv => s"${kv._1}$INNER_DELIMITER${kv._2}" }.mkString(KEY_VAL_DELIMITER)
  def find(tableName: String, useCache: Boolean = true)(idxKeyVals: Seq[(KEY, VAL)]): Option[HBaseModel] = {
    withCache(toCacheKey(idxKeyVals), useCache) {
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
  }
  def findsRange(tableName: String, useCache: Boolean = true)(idxKeyVals: Seq[(KEY, VAL)],
                                                              endIdxKeyVals: Seq[(KEY, VAL)]): List[HBaseModel] = {
    withCaches(toCacheKey(idxKeyVals ++ endIdxKeyVals), useCache) {
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
  }
  def findsMatch(tableName: String, useCache: Boolean = true)(idxKeyVals: Seq[(KEY, VAL)]): List[HBaseModel] = {
    withCaches(toCacheKey(idxKeyVals), useCache) {
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
  }
  def getSequence(tableName: String): Long = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val get = new Get(tableName.getBytes)
      get.addColumn(modelCf.getBytes, idQualifier.getBytes)
      val result = table.get(get)
      if (result == null || result.isEmpty) 0L
      else {
        val cell = result.getColumnLatestCell(modelCf.getBytes, idQualifier.getBytes)
        Bytes.toLong(cell.getValue())
      }
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

  def insert(tableName: String)(idxKVs: Seq[(KEY, VAL)], valKVs: Seq[(KEY, VAL)]) = {
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
  def delete(tableName: String)(idxKVs: Seq[(KEY, VAL)], valKVs: Seq[(KEY, VAL)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val rowKey = toRowKey(tableName, idxKVs).getBytes
      val delete = new Delete(rowKey)
      delete.addColumn(modelCf.getBytes, qualifier.getBytes)
      table.checkAndDelete(rowKey, modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes, delete)
    } finally {
      table.close()
    }
  }
}

class HBaseModel(protected val tableName: String, protected val kvs: Map[KEY, VAL]) {
  import HBaseModel._
  /** act like columns in table */
  protected val columns = Seq.empty[String]
  /** act like index */
  protected val idxKVsList = List.empty[Seq[(KEY, VAL)]]
  /** act like foreign key */
  protected val referencedBysList = List.empty[Seq[(String, KEY, KEY)]]

  override def toString(): String = (kvs ++ Map("tableName" -> tableName)).toString

  def validate(columns: Seq[String]): Unit = {
    for (c <- columns) {
      if (!kvs.contains(c))
        throw new RuntimeException(s"$tableName expect ${columns.toList.sorted}, found ${kvs.toList.sortBy{kv => kv._1}}")
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
  def deleteAll(): Boolean = {
    val rets = for {
      tableNameForiegnKeyColumns <- referencedBysList
      (referenceTableName, referenceColumn, column) <- tableNameForiegnKeyColumns
      referenced <- HBaseModel.findsMatch(referenceTableName, useCache = false)(Seq(referenceColumn -> kvs(column)))
      reference = HBaseModel.newInstance(referenceTableName)(referenced.kvs)
    } yield {
      reference.deleteAll()
    }
    destroy()
  }
}




