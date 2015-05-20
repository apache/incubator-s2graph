package com.daumkakao.s2graph.core.models

import java.util.concurrent.ExecutorService

import com.daumkakao.s2graph.core.Graph
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContext

/**
 * Created by shon on 5/12/15.
 */
class HBaseModelTest extends FunSuite with Matchers {

  val zkQuorum = "localhost"
  val config = ConfigFactory.parseString(s"hbase.zookeeper.quorum=$zkQuorum")
  Graph(config)(ExecutionContext.Implicits.global)
  HBaseModel(zkQuorum)
  val serviceName = "testService"
  val newServiceName = "newTestService"
  val cluster = "localhost"
  val hbaseTableName = "s2graph-dev"
  val columnName = "user_id"
  val columnType = "long"
  val labelName = "model_test_label"
  val newLabelName = "new_model_test_label"
  val columnMetaName = "is_valid_user"
  val labelMetaName = "is_hidden"
  val hbaseTableTTL = -1
  val id = 1

  val service = HService(Map("id" -> id, "serviceName" -> serviceName, "cluster" -> cluster,
    "hbaseTableName" -> hbaseTableName, "preSplitSize" -> 0, "hbaseTableTTL" -> -1))
  val serviceColumn = HServiceColumn(Map("id" -> id, "serviceId" -> service.id.get,
    "columnName" -> columnName, "columnType" -> columnType))
  val columnMeta = HColumnMeta(Map("id" -> id, "columnId" -> serviceColumn.id.get, "name" -> columnMetaName,  "seq" -> 1.toByte))
  val label = HLabel(Map("id" -> id, "label" -> labelName,
    "srcServiceId" -> service.id.get, "srcColumnName" -> columnName, "srcColumnType" -> columnType,
    "tgtServiceId" -> service.id.get, "tgtColumnName" -> columnName, "tgtColumnType" -> columnType,
    "isDirected" -> true, "serviceName" -> service.serviceName, "serviceId" -> service.id.get,
    "consistencyLevel" -> "weak", "hTableName" -> hbaseTableName, "hTableTTL" -> -1
  ))
  val labelMeta = HLabelMeta(Map("id" -> id, "labelId" -> label.id.get, "name" -> labelMetaName, "seq" -> 1.toByte,
    "defaultValue" -> false, "dataType" -> "boolean", "usedInIndex" -> false))
  val labelIndex = HLabelIndex(Map("id" -> id, "labelId" -> label.id.get, "seq" -> 1.toByte,
    "metaSeqs" -> "0", "formular" -> "none"))

  test("test create") {
    service.create()
    HService.findByName(serviceName, useCache = false) == Some(service)

    serviceColumn.create()
    HServiceColumn.findsByServiceId(service.id.get, useCache = false).headOption == Some(serviceColumn)

    columnMeta.create()
    HColumnMeta.findByName(serviceColumn.id.get, columnMetaName, useCache = false) == Some(columnMeta)

    label.create()
    HLabel.findByName(labelName, useCache = false) == Some(label)

    labelMeta.create()
    HLabelMeta.findByName(label.id.get, labelMetaName, useCache = false) == Some(labelMeta)

    labelIndex.create()
    HLabelIndex.findByLabelIdAll(label.id.get, useCache = false).headOption == Some(labelIndex)
  }

  test("test update") {
    service.update("cluster", "...")
    HService.findById(service.id.get, useCache = false).cluster == "..."

    service.update("serviceName", newServiceName)
    assert(HService.findByName(serviceName, useCache = false) == None)
    HService.findByName(newServiceName, useCache = false).map { service => service.id.get == service.id.get}

    label.update("label", newLabelName)
    HLabel.findById(label.id.get, useCache = false).label == "newLabelName"

    label.update("consistencyLevel", "strong")
    HLabel.findById(label.id.get, useCache = false).consistencyLevel == "strong" &&
    HLabel.findByName(newLabelName).isDefined &&
    HLabel.findByName(labelName) == None

  }
  test("test read by index") {
    val labels = HLabel.findBySrcServiceId(service.id.get, useCache = false)
    val idxs = HLabelIndex.findByLabelIdAll(label.id.get, useCache = false)
    labels.length == 1 &&
    labels.head == label
    idxs.length == 1 &&
    idxs.head == labelIndex
  }
  test("test delete") {
//    HLabel.findByName(labelName).foreach { label =>
//      label.deleteAll()
//    }
    HLabel.findByName(newLabelName).foreach { label =>
      label.deleteAll()
    }
    HLabelMeta.findAllByLabelId(label.id.get, useCache = false).isEmpty &&
    HLabelIndex.findByLabelIdAll(label.id.get, useCache = false).isEmpty

    service.deleteAll()
  }
//  test("test HColumnMeta") {
//    val id = 1
//    val kvs = Map("id" -> id, "columnId" -> 10, "name" -> "testMeta", "seq" -> 4.toByte)
//    val model = HColumnMeta(kvs)
//    println(model.create())
//    val original = HColumnMeta.findById(1, useCache = false)
//
//  }
//  test("test HService") {
//    val id = 1
//    val model = HService(Map("id" -> id, "serviceName" -> serviceName, "cluster" -> "localhost", "hbaseTableName" -> "s2graph-dev",
//    "preSplitSize" -> 0, "hbaseTableTTL" -> -1))
//    /** create new model */
//    println(model.create())
//    /** update index property serviceName */
//    model.update("serviceName", s"new_$serviceName")
//
//    var updated = HService.findById(id, useCache = false)
//    HService.findByName(serviceName) == None &&
//    HService.findByName(s"new_$serviceName") == Some(updated)
//
//    /** update meta property cluster */
//    updated.update("cluster", "new")
//    updated = HService.findById(id, useCache = false)
//    println(updated)
//    updated.kvsParam("cluster").toString == "new"
//  }
//  test("test HServiceColumn") {
//    val kvs = Map("id" -> "1", "serviceId" -> "10", "columnName" -> "testColumnName", "columnType" -> "long")
//    val model = HServiceColumn(kvs)
//    println(model.create(zkQuorum))
//    val find = HBaseModel.find(zkQuorum)("HServiceColumn")_
//    find(Seq(("id", "1"))) ==
//      find(Seq(("serviceId", "10"), ("columnName", "testColumnName")))
//  }
//  test("test HLabelIndex") {
//    val find = HBaseModel.find(zkQuorum)("HLabelIndex")_
//    val finds = HBaseModel.finds(zkQuorum)("HLabelIndex")_
//
//    val models = for (seq <- (1 until 4)) yield {
//      val kvs = Map("id" -> s"${10 + seq}", "labelId" -> "1", "seq" -> s"$seq",
//        "metaSeqs" -> (0 until seq).toList.map (x => "a").mkString(","),
//        "formular" -> "")
//
//      val model = HLabelIndex(kvs)
//      println(model.create(zkQuorum))
//      println(find(Seq(("id", s"${10 + seq}"))))
//    }
//    println(finds(Seq(("labelId", "1")), Seq(("labelId", "2"))))
//  }
//  test("test HLabelMeta") {
//    val model = HLabelMeta(1, 23, "testName", 1.toByte, "null", "string", true)
//    println(model.insert(zkQuorum))
//    println(model.find(zkQuorum)(model.pk))
//    println(model.find(zkQuorum)(model.idxByLabelIdName))
//    println(model.find(zkQuorum)(model.idxByLabelIdSeq))
//  }

}
