//package com.kakao.ml.io
//
//import com.kakao.ml.{BaseDataProcessor, Params, PredecessorData}
//import com.kakao.s2graph.core.Management
//import com.kakao.s2graph.core.mysqls.{Label, Service}
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.ConnectionFactory
//import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
//import org.apache.spark.sql.types.DoubleType
//import org.apache.spark.sql.{DataFrame, Row, SQLContext}
//import subscriber.GraphSubscriberHelper
//
//case class GraphLoaderParams(
//    keyToSave: String,
//    phase: String,
//    dBUrl: String,
//    zkQuorum: String,
//    kafkaBrokerList: String,
//    label: String,
//    loadFactor: Option[Int],
//    batchSize: Option[Int],
//    scale: Option[Int],
//    newTable: Option[Boolean]) extends Params
//
//class GraphLoader(params: GraphLoaderParams) extends BaseDataProcessor[PredecessorData, PredecessorData](params) {
//
//  import com.kakao.ml.recommendation._
//
//  override protected def processBlock(sqlContext: SQLContext, input: PredecessorData): PredecessorData = {
//
//    /** default params */
//    val loadFactor = params.loadFactor.getOrElse(10)
//    val batchSize = params.batchSize.getOrElse(10000)
//    val scale = params.scale.getOrElse(100)
//    val newTable = params.newTable.getOrElse(true)
//
//    if(!input.asMap.contains(params.keyToSave)) {
//      logError(s"predecessor does not contain ${params.keyToSave}")
//      return input
//    }
//
//    val dataToSaveAll = predecessorData.asMap(params.keyToSave) match {
//      case df: DataFrame => df
//      case Some(df: DataFrame) => df
//      case _ =>
//        logError(s"predecessorData(${params.keyToSave}) is not instance of DataFrame")
//        return input
//    }
//
//    val dataToSave = try {
//      dataToSaveAll.select(fromCol, toCol, scoreCol.cast(DoubleType))
//    } catch {
//      case _: Throwable =>
//        logError(s"predecessorData(${params.keyToSave}) dose not contain columns $fromColString, $toColString, $scoreColString")
//        return input
//    }
//
//    GraphLoader.apply(params.phase, params.dBUrl, params.zkQuorum, params.kafkaBrokerList)
//
//    val hTableOpt = GraphLoader.getHTableName(params.label, newTable)
//
//    if (hTableOpt.isEmpty) {
//      logError(s"Cannot find hbaseTableName for the label : (${params.label})")
//      return input
//    }
//
//    val hTable = hTableOpt.get
//
//    logInfo(s">>> hTableName : $hTable")
//
//    GraphLoader.load(dataToSave, hTable, loadFactor, batchSize, scale, newTable,
//      params.label, params.phase, params.dBUrl, params.zkQuorum, params.kafkaBrokerList)
//
//    input
//  }
//}
//
//
//object GraphLoader {
//
//  DriverRegistry.register("com.mysql.jdbc.Driver")
//
//  def apply(phase: String, dbUrl: String, zkQuorum: String, kafkaBrokerList: String) {
//    println(s"phase: $phase, dbUrl: $dbUrl, zkQuorum: $zkQuorum, kafkaBrokerList: $kafkaBrokerList")
//    GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, kafkaBrokerList)
//  }
//
//  def load(dataToSave: DataFrame, hTable: String, loadFactor: Int, batchSize: Int, scale: Int, newTable: Boolean,
//      label: String, phase: String, dBUrl: String, zkQuorum: String, kafkaBrokerList: String) = {
//
//    dataToSave.repartition(loadFactor).foreachPartition { pairs =>
//
//      GraphLoader.apply(phase, dBUrl, zkQuorum, kafkaBrokerList)
//
//      val ts = System.currentTimeMillis()
//
//      val hbaseConf = HBaseConfiguration.create()
//      hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
//      val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
//
//      val elements = pairs.map { case Row(from: String, to: String, score: Double) =>
//        val prop = s"""{"weight" : ${(score * scale).toInt}}"""
//        s"$ts\tinsertBulk\te\t$from\t$to\t$label\t$prop"
//      }
//
//      elements.grouped(batchSize).foreach { grouped =>
//        GraphSubscriberHelper.storeBulk(zkQuorum, hTable)(grouped, Map.empty, true)(None)
//      }
//    }
//
//    // change hTableName
//    if (newTable) {
//      println("create a newTable")
//      GraphLoader.changeHTable(label, hTable)
//    }
//
//  }
//
//  def getHTableName(labelName: String, isNew: Boolean = false): Option[String] = {
//    if(isNew) createTable(labelName)
//    else {
//      Label.findByName(labelName, false) match {
//        case Some(l:Label) => Some(l.hbaseTableName)
//        case None =>
//          None
//      }
//    }
//  }
//
//  private def createTable(labelName:String):Option[String] = {
//    import java.util.Date
//
//    val format = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
//    val hTableName = s"s2ml-$labelName-${format.format(new Date(System.currentTimeMillis()))}"
//
//    val label = Label.findByName(labelName)
//    label match {
//      case Some(l:Label) =>
//        val service = Service.findById(l.serviceId)
//        // create hTable
//        println(s">>> create hTable...(label:$labelName, hTableName:$hTableName)")
//        Management.createTable(service.cluster, hTableName, List("e", "v"), service.preSplitSize, l.hTableTTL)
//        Some(hTableName)
//      case None => None
//    }
//  }
//
//  def changeHTable(labelName:String, hTableName:String):Boolean = {
//    val label = Label.findByName(labelName)
//    label match {
//      case Some(l:Label) =>
//        println(s">>> change hTable for label '$labelName' (${l.hbaseTableName} => ${hTableName}})")
//        val rst = Label.updateHTableName(labelName, hTableName)
//        println(s">>>> update result : $rst")
//
//        // TODO: metadb cache expire for s2graph??
//        true
//      case None =>
//        println(s">>> failed to change hTable for label '$labelName' ")
//        false
//    }
//  }
//
//}
