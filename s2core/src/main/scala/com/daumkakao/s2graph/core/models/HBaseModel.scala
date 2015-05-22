package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core._
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client._
import play.api.Logger
import collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

object HBaseModel extends LocalCache[Result] {
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
    clazz.getName().split("\\.").last
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
    if (r == null || r.isEmpty) Array.empty[Byte]
    val cell = r.getColumnLatestCell(modelCf.getBytes, qualifier.getBytes)
    CellUtil.cloneValue(cell)
  }
  def fromResult[T : ClassTag](r: Result): Option[T] = {
    if (r == null || r.isEmpty) None
    else {
       r.listCells().headOption.map { cell =>
        val rowKey = Bytes.toString(CellUtil.cloneRow(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
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
        val rowKey = Bytes.toString(CellUtil.cloneRow(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        val elements = rowKey.split(DELIMITER)
        val (tName, idxKeyVals) = (elements(0), elements(1))
        val merged = toKVTuplesMap(idxKeyVals) ++ toKVTuplesMap(value)
        newInstance[T](merged)
      } toList
    }
  }
  def toCacheKey(kvs: Seq[(KEY, VAL)]) = kvs.map { kv => s"${kv._1}$INNER_DELIMITER${kv._2}" }.mkString(KEY_VAL_DELIMITER)
  def distincts[T: ClassTag](ls: List[T]): List[T] = {
    val uniq = new mutable.HashSet[String]
    for {
      r <- ls
      m = r.asInstanceOf[HBaseModel[_]]
      if m.kvs.containsKey("id") && !uniq.contains(m.kvs("id").toString)
    } yield {
      uniq += m.kvs("id").toString
      r
    }
  }
  def find[T : ClassTag](useCache: Boolean = true)(idxKeyVals: Seq[(KEY, VAL)]): Option[T] = {
    val result = withCache(toCacheKey(idxKeyVals), useCache) {
      val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
      try {

        val rowKey = toRowKey(getClassName[T], idxKeyVals)
        val get = new Get(rowKey.getBytes)
        get.addColumn(modelCf.getBytes, qualifier.getBytes)
        get.setMaxVersions(1)
//        val res = table.get(get)
        table.get(get)
//        fromResult[T](res)
      } finally {
        table.close()
      }
    }
    fromResult[T](result)
  }
  def findsRange[T : ClassTag](useCache: Boolean = true)(idxKeyVals: Seq[(KEY, VAL)],
                                                              endIdxKeyVals: Seq[(KEY, VAL)]): List[T] = {
    val results = withCaches(toCacheKey(idxKeyVals ++ endIdxKeyVals), useCache) {
      val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
      try {
        val scan = new Scan()
        scan.setStartRow(toRowKey(getClassName[T], idxKeyVals).getBytes)
        scan.setStopRow(toRowKey(getClassName[T], endIdxKeyVals).getBytes)
        scan.addColumn(modelCf.getBytes, qualifier.getBytes)
        val resScanner = table.getScanner(scan)
        resScanner.toList
        //        val models = for {r <- resScanner; m <- fromResult[T](r)} yield m
        //        models.toList
      } finally {
        table.close()
      }
    }
    val rs = results.flatMap { r => fromResult[T](r) }
    distincts[T](rs)
  }
  def findsMatch[T : ClassTag](useCache: Boolean = true)(idxKeyVals: Seq[(KEY, VAL)]): List[T] = {
    val results = withCaches(toCacheKey(idxKeyVals), useCache) {
      val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
      try {
        val scan = new Scan()
        scan.setStartRow(toRowKey(getClassName[T], idxKeyVals).getBytes)
        val endBytes = Bytes.add(toRowKey(getClassName[T], idxKeyVals).getBytes, Array.fill(1)(Byte.MinValue.toByte))
        scan.setStopRow(endBytes)
        scan.addColumn(modelCf.getBytes, qualifier.getBytes)
        val resScanner = table.getScanner(scan)
        resScanner.toList
        //        val models = for {r <- resScanner; m <- fromResult[T](r)} yield m
        //        models.toList
      } finally {
        table.close()
      }
    }
    val rs = results.flatMap { r => fromResult[T](r) }
    distincts[T](rs)
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
        Bytes.toLong(CellUtil.cloneValue(cell))
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
  def insertForce[T: ClassTag](idxKVs: Seq[(KEY, VAL)], valKVs: Seq[(KEY, VAL)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val rowKey = toRowKey(getClassName[T], idxKVs).getBytes
      val put = new Put(rowKey)
      put.addColumn(modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes)
      table.put(put)
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
  def deleteForce[T: ClassTag](idxKVs: Seq[(KEY, VAL)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val rowKey = toRowKey(getClassName[T], idxKVs).getBytes
      val delete = new Delete(rowKey)
      delete.addColumn(modelCf.getBytes, qualifier.getBytes)
      table.delete(delete)
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

}

class HBaseModel[T : ClassTag](protected val tableName: String, protected val kvs: Map[HBaseModel.KEY, HBaseModel.VAL])  {
  import HBaseModel._
  import scala.reflect.runtime._
  import scala.reflect.runtime.universe._

  /** act like columns in table */
  protected val columns = Seq.empty[String]
  /** act like index */
  protected val idxs = List.empty[Seq[(KEY, VAL)]]
  /** act like foreign key */
  protected def foreignKeys() = {
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
      idxKVs <- idxs
    } yield {
      HBaseModel.insert[T](idxKVs, toKVsWithFilter(kvs, idxKVs))
    }
    rets.forall(r => r)
  }
  def destroy() = {
    val rets = for (idxKVs <- idxs) yield {
      HBaseModel.delete[T](idxKVs, toKVsWithFilter(kvs, idxKVs))
    }
    rets.forall(r => r)
  }
  def update(key: KEY, value: VAL) = {
    for {
      idxKVs <- idxs
    } {
      val idxKVsMap = idxKVs.toMap
      for {
        oldRaw <- HBaseModel.find[T] (useCache = false) (idxKVs)
        old = oldRaw.asInstanceOf[HBaseModel[T]]
      } {
        val oldMetaKVs = old.kvs.filter(kv => !idxKVsMap.containsKey(kv._1))
        val (newIdxKVs, newMetaKVs) = if (idxKVsMap.containsKey(key)) {
          (idxKVs.filter(kv => kv._1 != key) ++ Seq(key -> value), oldMetaKVs)
        } else {
          (idxKVs, oldMetaKVs.filter(kv => kv._1 != key) ++ Seq(key -> value))
        }
        HBaseModel.deleteForce[T](idxKVs)
        HBaseModel.insertForce[T](newIdxKVs, newMetaKVs.toSeq)
      }
    }
  }
//  def deleteAll(): Boolean = destroy()
  def deleteAll(): Boolean = {
    val rets = for {
      models <- foreignKeys()
      model <- models
    } yield {
      model.deleteAll()
    }
    destroy()
  }
}




