package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.models.HBaseModel.{KEY, VAL}
import com.daumkakao.s2graph.core.{Graph, GraphConnection}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, KeyValue, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import play.api.libs.json.Json
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

  type KEY = String
  type VAL = Any
  def apply(zkQuorum: String) = {
    this.zkQuorum = zkQuorum
  }
  def padZeros(v: VAL): String = {
    v match {
      case b: Byte => "%03d".format(b)
      case s: Short => "%05d".format(s)
      case i: Int => "%08d".format(i)
      case l: Long => "%08d".format(l)
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
  def newInstance(tableName: String)(kvs: Map[KEY, VAL]) = {
    tableName match {
      case "HService" => HService(kvs)
      case "HServiceColumn" => HServiceColumn(kvs)
      case _ => new HBaseModel(tableName, kvs)
    }
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

  def find(tableName: String)(idxKeyVals: Seq[(KEY, VAL)]): Option[HBaseModel] = {
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
  def findsRange(tableName: String)(idxKeyVals: Seq[(KEY, VAL)], endIdxKeyVals: Seq[(KEY, VAL)]): List[HBaseModel] = {
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
  def findsMatch(tableName: String)(idxKeyVals: Seq[(KEY, VAL)]): List[HBaseModel] = {
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
      table.checkAndDelete(rowKey, modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes, delete)
    } finally {
      table.close()
    }
  }
}

/**
 */
class HBaseModel(protected val tableName: String, protected val kvs: Map[KEY, VAL]) {
  import HBaseModel._
  protected val columns = Seq.empty[String]
  protected val idxKVsList = List.empty[Seq[(KEY, VAL)]]
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

object HColumnMeta {
  val timeStampSeq = 0.toByte
  val countSeq = -1.toByte
  val lastModifiedAtColumnSeq = 0.toByte
  val lastModifiedAtColumn = HColumnMeta(Map("id" -> 0, "columnId" -> 0,
    "name" -> "lastModifiedAt", "seq" -> lastModifiedAtColumnSeq))
  val maxValue = Byte.MaxValue


  def findById(id: Int): HColumnMeta = {
    HBaseModel.find("HColumnMeta")(Seq(("id" -> id))).get.asInstanceOf[HColumnMeta]
  }
  def findAllByColumn(columnId: Int) = {
    HBaseModel.findsMatch("HColumnMeta")(Seq(("columnId" -> columnId))).map(x => x.asInstanceOf[HColumnMeta])
  }
  def findByName(columnId: Int, name: String) = {
    HBaseModel.find("HColumnMeta")(Seq(("columnId" -> columnId), ("name" -> name))).map(x => x.asInstanceOf[HColumnMeta])
  }
  def findByIdAndSeq(columnId: Int, seq: Byte) = {
    HBaseModel.find("HColumnMeta")(Seq(("columnId" -> columnId), ("seq" -> seq))).map(x => x.asInstanceOf[HColumnMeta])
  }
  def findOrInsert(columnId: Int, name: String): HColumnMeta = {
    findByName(columnId, name) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq("HColumnMeta")
        val seq = HBaseModel.findsMatch("HColumnMeta")(Seq(("columnId" -> columnId)))
        val model = HColumnMeta(Map("id" -> id, "columnId" -> columnId, "name" -> name, "seq" -> seq))
        model.create
        model
    }
  }
}

case class HColumnMeta(kvsParam: Map[KEY, VAL]) extends HBaseModel("HColumnMeta", kvsParam) {
  override val columns = Seq("id", "columnId", "name", "seq")

  val pk = Seq(("id", kvs("id")))
  val idxColumnIdName = Seq(("columnId", kvs("columnId")), ("name", kvs("name")))
  val idxColumnIdSeq = Seq(("columnId", kvs("columnId")), ("seq", kvs("seq")))

  override val idxKVsList = List(pk, idxColumnIdName, idxColumnIdSeq)
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val columnId = kvs("columnId").toString.toInt
  val name = kvs("name").toString
  val seq = kvs("seq").toString.toByte

}

object HService {
  def findById(id: Int): HService = {
    HBaseModel.find("HService")(Seq(("id" -> id))).get.asInstanceOf[HService]
  }
  def findByName(serviceName: String): Option[HService] = {
    HBaseModel.find("HService")(Seq(("serviceName" -> serviceName))).map { x => x.asInstanceOf[HService] }
  }
  def findOrInsert(serviceName: String, cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int]): HService = {
    findByName(serviceName) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq("HService")
        val kvs = Map("id" -> id, "serviceName" -> serviceName, "cluster" -> cluster, "hbaseTableName" -> hTableName,
          "preSplitSize" -> preSplitSize, "hbaseTableTTL" -> hTableTTL.getOrElse(-1))
        val service = HService(kvs)
        service.create()
        service
    }
  }
  def findAllServices(): List[HService] = {
    HBaseModel.findsRange("HService")(Seq(("id"-> 0)), Seq(("id" -> Int.MaxValue))).map{x => x.asInstanceOf[HService]}
  }
}
case class HService(kvsParam: Map[KEY, VAL]) extends HBaseModel("HService", kvsParam) {
  override val columns = Seq("id", "serviceName", "cluster", "hbaseTableName", "preSplitSize", "hbaseTableTTL")

  val pk = Seq(("id", kvs("id")))
  val idxServiceName = Seq(("serviceName", kvs("serviceName")))
  val idxCluster = Seq(("cluster", kvs("cluster")))

  override val idxKVsList = List(pk, idxServiceName, idxCluster)
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val serviceName = kvs("serviceName").toString
  val cluster = kvs("cluster").toString
  val hTableName = kvs("hbaseTableName").toString
  val preSplitSize = kvs("preSplitSize").toString.toInt
  val hTableTTL = {
    val ttl = kvs("hbaseTableTTL").toString.toInt
    if (ttl < 0) None
    else Some(ttl)
  }
  lazy val toJson = kvs.toString
}

object HServiceColumn {
  def findById(id: Int): HServiceColumn = {
    HBaseModel.find("HServiceColumn")(Seq(("id" -> id))).get.asInstanceOf[HServiceColumn]
  }
  def find(serviceId: Int, columnName: String): Option[HServiceColumn] = {
    HBaseModel.find("HServiceColumn")(Seq("serviceId" -> serviceId, "columnName" -> columnName))
      .map { x => x.asInstanceOf[HServiceColumn]}
  }
  def findOrInsert(serviceId: Int, columnName: String, columnType: Option[String]): HServiceColumn = {
    find(serviceId, columnName) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq("HServiceColumn")
        val model = new HServiceColumn(Map("id" -> id, "serviceId" -> serviceId, "columnName" -> columnName,
        "columnType" -> columnType.getOrElse("string")))
        model.create
        model
    }
  }
}
case class HServiceColumn(kvsParam: Map[KEY, VAL]) extends HBaseModel("HServiceColumn", kvsParam) {
  override val columns = Seq("id", "serviceId", "columnName", "columnType")
  val pk = Seq(("id", kvs("id")))
  val idxServiceIdColumnName = Seq(("serviceId", kvs("serviceId")), ("columnName", kvs("columnName")))
  override val idxKVsList = List(pk, idxServiceIdColumnName)
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val serviceId = kvs("serviceId").toString.toInt
  val columnName = kvs("columnName").toString
  val columnType = kvs("columnType").toString


  val service = HService.findById(serviceId)
  val metas = HColumnMeta.findAllByColumn(id.get)
  val metaNamesMap = (HColumnMeta.lastModifiedAtColumn :: metas).map(x => (x.seq, x.name)) toMap
  lazy val toJson = Json.obj("serviceName" -> service.serviceName, "columnName" -> columnName, "columnType" -> columnType)
}


case class HLabelIndex(kvsParam: Map[KEY, VAL]) extends HBaseModel("HLabelIndex", kvsParam) {
  override val columns = Seq("id", "labelId", "seq", "metaSeqs", "formular")
  val pk = Seq(("id", kvs("id")))
  val labelIdSeq = Seq(("labelId", kvs("labelId")), ("metaSeqs", kvs("metaSeqs")))
  override val idxKVsList = List(pk, labelIdSeq)
  validate(columns)
  assert(!kvs("metaSeqs").toString().isEmpty)
}
