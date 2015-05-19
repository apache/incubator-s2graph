package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core._
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName}
import org.apache.hadoop.hbase.client._
import collection.JavaConversions._
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import HBaseModel._

object HBaseModel {
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
  def getClassName[T: ClassTag] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass
    clazz.getName()
  }

  def newInstance[T: ClassTag](kvs: Map[KEY, VAL]) = {
    val clazz = implicitly[ClassTag[T]].runtimeClass
    val ctr = clazz.getConstructors()(0)
    ctr.newInstance(kvs).asInstanceOf[T]
 }

//  def newInstance(tableName: String)(kvs: Map[KEY, VAL]) = {
//    tableName match {
//      case "HService" => HService(kvs)
//      case "HServiceColumn" => HServiceColumn(kvs)
//      case "HColumnMeta" => HColumnMeta(kvs)
//      case "HLabelMeta" => HLabelMeta(kvs)
//      case "HLabelIndex" => HLabelIndex(kvs)
//      case "HLabel" => HLabel(kvs)
//      case _ => new HBaseModel(tableName, kvs)
//    }
//  }
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
  def fromResultOnlyVal(r: Result): Array[Byte] = {
    if (r == null || r.isEmpty) None
    val cell = r.getColumnLatestCell(modelCf.getBytes, qualifier.getBytes)
    cell.getValue
  }
  def fromResult[T : ClassTag](r: Result): Option[T] = {
    if (r == null || r.isEmpty) None
    else {
       r.listCells().headOption.map { cell =>
        val rowKey = Bytes.toString(cell.getRow)
        val qualifier = Bytes.toString(cell.getQualifier)
        val value = Bytes.toString(cell.getValue)
        val elements = rowKey.split(DELIMITER)
        val (tName, idxKeyVals) = (elements(0), elements(1))
        val merged = toKVTuplesMap(idxKeyVals) ++ toKVTuplesMap(value)
        newInstance[T](merged)
      }
    }
  }
  def fromResultLs[T : ClassTag](r: Result): List[T] = {
    if (r == null || r.isEmpty) List.empty[T]
    else {
      r.listCells().map { cell =>
        val rowKey = Bytes.toString(cell.getRow)
        val qualifier = Bytes.toString(cell.getQualifier)
        val value = Bytes.toString(cell.getValue)
        val elements = rowKey.split(DELIMITER)
        val (tName, idxKeyVals) = (elements(0), elements(1))
        val merged = toKVTuplesMap(idxKeyVals) ++ toKVTuplesMap(value)
        newInstance[T](merged)
      } toList
    }
  }
  def toCacheKey(kvs: Seq[(KEY, VAL)]) = kvs.map { kv => s"${kv._1}$INNER_DELIMITER${kv._2}" }.mkString(KEY_VAL_DELIMITER)
  def find[T : ClassTag](useCache: Boolean = true)(idxKeyVals: Seq[(KEY, VAL)]): Option[T] = {
//    withCache(toCacheKey(idxKeyVals), useCache) {
      val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
      try {

        val rowKey = toRowKey(getClassName[T], idxKeyVals)
        val get = new Get(rowKey.getBytes)
        get.addColumn(modelCf.getBytes, qualifier.getBytes)
        get.setMaxVersions(1)
        val res = table.get(get)
        fromResult[T](res)
      } finally {
        table.close()
      }
//    }
  }
  def findsRange[T : ClassTag](useCache: Boolean = true)(idxKeyVals: Seq[(KEY, VAL)],
                                                              endIdxKeyVals: Seq[(KEY, VAL)]): List[T] = {
//    withCaches(toCacheKey(idxKeyVals ++ endIdxKeyVals), useCache) {
      val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
      try {
        val scan = new Scan()
        scan.setStartRow(toRowKey(getClassName[T], idxKeyVals).getBytes)
        scan.setStopRow(toRowKey(getClassName[T], endIdxKeyVals).getBytes)
        scan.addColumn(modelCf.getBytes, qualifier.getBytes)
        val resScanner = table.getScanner(scan)
        val models = for {r <- resScanner; m <- fromResult[T](r)} yield m
        models.toList
      } finally {
        table.close()
      }
//    }
  }
  def findsMatch[T : ClassTag](useCache: Boolean = true)(idxKeyVals: Seq[(KEY, VAL)]): List[T] = {
//    withCaches(toCacheKey(idxKeyVals), useCache) {
      val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
      try {
        val scan = new Scan()
        scan.setStartRow(toRowKey(getClassName[T], idxKeyVals).getBytes)
        val endBytes = Bytes.add(toRowKey(getClassName[T], idxKeyVals).getBytes, Array.fill(1)(Byte.MinValue.toByte))
        scan.setStopRow(endBytes)
        scan.addColumn(modelCf.getBytes, qualifier.getBytes)
        val resScanner = table.getScanner(scan)
        val models = for {r <- resScanner; m <- fromResult[T](r)} yield m
        models.toList
      } finally {
        table.close()
      }
//    }
  }

  def getSequence[T : ClassTag]: Long = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val get = new Get(getClassName[T].getBytes)
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
  def getAndIncrSeq[T : ClassTag]: Long = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      table.incrementColumnValue(getClassName[T].getBytes, modelCf.getBytes, idQualifier.getBytes, 1L)
    } finally {
      table.close()
    }
  }

  def insert[T : ClassTag](idxKVs: Seq[(KEY, VAL)], valKVs: Seq[(KEY, VAL)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val rowKey = toRowKey(getClassName[T], idxKVs).getBytes
      val put = new Put(rowKey)
      put.addColumn(modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes)
      /** expecte null **/
      table.checkAndPut(rowKey, modelCf.getBytes, qualifier.getBytes, null, put)
    } finally {
      table.close()
    }
  }
  def delete[T : ClassTag](idxKVs: Seq[(KEY, VAL)], valKVs: Seq[(KEY, VAL)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val rowKey = toRowKey(getClassName[T], idxKVs).getBytes
      val delete = new Delete(rowKey)
      delete.addColumn(modelCf.getBytes, qualifier.getBytes)
      table.checkAndDelete(rowKey, modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes, delete)
    } finally {
      table.close()
    }
  }
//  def update[T: ClassTag](idxKVs: Seq[(KEY, VAL)], valsToUpdate: Seq[(KEY, VAL)]) = {
//    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
//    try {
//      /** read previous value */
//      val rowKey = toRowKey(getClassName[T], idxKVs).getBytes
//      val get = new Get(rowKey)
//      get.addColumn(modelCf.getBytes, qualifier.getBytes)
//      val result = table.get(get)
//      val prev = fromResult[T](result)
//      val prevValue = fromResultOnlyVal(result)
//      /** build updates based on previous value */
//      val prevKVs = prev.map(m => m.kvs).getOrElse(Map.empty[KEY, VAL])
//      val newKVs = valsToUpdate.toMap
//      val merged = prevKVs.filter(kv => !newKVs.containsKey(kv._1)) ++ newKVs
//      /** execute check and put */
//      val put = new Put(rowKey)
//      put.addColumn(modelCf.getBytes, qualifier.getBytes, toKVs(merged.toSeq).getBytes)
//      table.checkAndPut(rowKey, modelCf.getBytes, qualifier.getBytes, prevValue, put)
//    } finally {
//      table.close()
//    }
//  }
}

class HBaseModel[T : ClassTag](protected val tableName: String, protected val kvs: Map[KEY, VAL]) extends LocalCache[T] {
  import HBaseModel._
  import scala.reflect.runtime._
  import scala.reflect.runtime.universe._

  /** act like columns in table */
  protected val columns = Seq.empty[String]
  /** act like index */
  protected val idxKVsList = List.empty[Seq[(KEY, VAL)]]
  /** act like foreign key */
//  protected val referencedBysList = List.empty[List[HBaseModel[_]]]
  protected def getReferencedModels() = {
    List.empty[List[HBaseModel[_]]]
  }

  override def toString(): String = (kvs ++ Map("tableName" -> tableName)).toString

  def validate(columns: Seq[String]): Unit = {
    for (c <- columns) {
      if (!kvs.contains(c))
        throw new RuntimeException(s"$tableName expect ${columns.toList.sorted}, found ${kvs.toList.sortBy{kv => kv._1}}")
    }
  }

  def create() = {
    val rets = for {
      idxKVs <- idxKVsList
    } yield {
      HBaseModel.insert[T](idxKVs, toKVsWithFilter(kvs, idxKVs))
    }
    rets.forall(r => r)
  }
  def destroy() = {
    val rets = for (idxKVs <- idxKVsList) yield {
      HBaseModel.delete[T](idxKVs, toKVsWithFilter(kvs, idxKVs))
    }
    rets.forall(r => r)
  }
  def update(key: KEY, value: VAL) = {
    for {
      idxKVs <- idxKVsList
    } {
      for {
        oldRaw <- HBaseModel.find[T] (useCache = false) (idxKVs)
        old = oldRaw.asInstanceOf[HBaseModel]
      } {
        val oldMetaKVs = old.kvs.filter(kv => !idxKVs.contains(kv._1))
        val (newIdxKVs, newMetaKVs) = if (idxKVs.contains(key)) {
          (idxKVs.filter(kv => kv._1 != key) ++ Seq(key -> value), oldMetaKVs)
        } else {
          (idxKVs, oldMetaKVs.filter(kv => kv._1 != key) ++ Seq(key -> value))
        }
        HBaseModel.delete[T](idxKVs, oldMetaKVs.toSeq)
        HBaseModel.insert[T](newIdxKVs, newMetaKVs.toSeq)
      }
    }
  }
//  def deleteAll(): Boolean = destroy()
  def deleteAll(): Boolean = {
    val rets = for {
      models <- getReferencedModels()
      model <- models
    } yield {
      model.deleteAll()
    }
    destroy()
  }
}




