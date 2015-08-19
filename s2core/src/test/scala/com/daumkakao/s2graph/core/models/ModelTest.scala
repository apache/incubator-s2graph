package com.daumkakao.s2graph.core.models

import java.util.concurrent.ExecutorService

import com.daumkakao.s2graph.core.mysqls.{Label, Model}
import com.daumkakao.s2graph.core.{TestCommonWithModels, TestCommon, Graph}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContext

/**
 * Created by shon on 5/12/15.
 */
class ModelTest extends FunSuite with Matchers with TestCommonWithModels {

//  val serviceName = "testService"
//  val newServiceName = "newTestService"
//  val cluster = "localhost"
//  val hbaseTableName = "s2graph-dev"
//  val columnName = "user_id"
//  val columnType = "long"
//  val labelName = "model_test_label"
//  val newLabelName = "new_model_test_label"
//  val columnMetaName = "is_valid_user"
//  val labelMetaName = "is_hidden"
//  val hbaseTableTTL = -1
//  val id = 1
//
//  val service = HService(Map("id" -> id, "serviceName" -> serviceName, "cluster" -> cluster,
//    "hbaseTableName" -> hbaseTableName, "preSplitSize" -> 0, "hbaseTableTTL" -> -1))
//  val serviceColumn = HServiceColumn(Map("id" -> id, "serviceId" -> service.id.get,
//    "columnName" -> columnName, "columnType" -> columnType))
//  val columnMeta = HColumnMeta(Map("id" -> id, "columnId" -> serviceColumn.id.get, "name" -> columnMetaName,  "seq" -> 1.toByte))
//  val label = HLabel(Map("id" -> id, "label" -> labelName,
//    "srcServiceId" -> service.id.get, "srcColumnName" -> columnName, "srcColumnType" -> columnType,
//    "tgtServiceId" -> service.id.get, "tgtColumnName" -> columnName, "tgtColumnType" -> columnType,
//    "isDirected" -> true, "serviceName" -> service.serviceName, "serviceId" -> service.id.get,
//    "consistencyLevel" -> "weak", "hTableName" -> hbaseTableName, "hTableTTL" -> -1
//  ))
//  val labelMeta = HLabelMeta(Map("id" -> id, "labelId" -> label.id.get, "name" -> labelMetaName, "seq" -> 1.toByte,
//    "defaultValue" -> false, "dataType" -> "boolean", "usedInIndex" -> false))
//  val labelIndex = HLabelIndex(Map("id" -> id, "labelId" -> label.id.get, "seq" -> 1.toByte,
//    "metaSeqs" -> "0", "formular" -> "none"))
  test("test Label.findByName") {
    val labelOpt = Label.findByName(labelName, useCache = false)
    println(labelOpt)
    labelOpt.isDefined shouldBe true
    val indices = labelOpt.get.indices
    indices.size > 0 shouldBe true
    println(indices)
    val defaultIndexOpt = labelOpt.get.defaultIndex
    println(defaultIndexOpt)
    defaultIndexOpt.isDefined shouldBe true
    val metas = labelOpt.get.metaProps
    println(metas)
    metas.size > 0 shouldBe true
    val srcService = labelOpt.get.srcService
    println(srcService)
    val tgtService = labelOpt.get.tgtService
    println(tgtService)
    val service = labelOpt.get.service
    println(service)
    val srcColumn = labelOpt.get.srcService
    println(srcColumn)
    val tgtColumn = labelOpt.get.tgtService
    println(tgtColumn)
  }
//  test("test create") {
//    service.create()
//    HService.findByName(serviceName, useCache = false) == Some(service)
//
//    serviceColumn.create()
//    HServiceColumn.findsByServiceId(service.id.get, useCache = false).headOption == Some(serviceColumn)
//
//    columnMeta.create()
//    HColumnMeta.findByName(serviceColumn.id.get, columnMetaName, useCache = false) == Some(columnMeta)
//
//    label.create()
//    HLabel.findByName(labelName, useCache = false) == Some(label)
//
//    labelMeta.create()
//    HLabelMeta.findByName(label.id.get, labelMetaName, useCache = false) == Some(labelMeta)
//
//    labelIndex.create()
//    HLabelIndex.findByLabelIdAll(label.id.get, useCache = false).headOption == Some(labelIndex)
//  }
//
//  test("test update") {
//    service.update("cluster", "...")
//    HService.findById(service.id.get, useCache = false).cluster == "..."
//
//    service.update("serviceName", newServiceName)
//    assert(HService.findByName(serviceName, useCache = false) == None)
//    HService.findByName(newServiceName, useCache = false).map { service => service.id.get == service.id.get}
//
//    label.update("label", newLabelName)
//    HLabel.findById(label.id.get, useCache = false).label == "newLabelName"
//
//    label.update("consistencyLevel", "strong")
//    HLabel.findById(label.id.get, useCache = false).consistencyLevel == "strong" &&
//    HLabel.findByName(newLabelName).isDefined &&
//    HLabel.findByName(labelName) == None
//
//  }
//  test("test read by index") {
//    val labels = HLabel.findBySrcServiceId(service.id.get, useCache = false)
//    val idxs = HLabelIndex.findByLabelIdAll(label.id.get, useCache = false)
//    labels.length == 1 &&
//    labels.head == label
//    idxs.length == 1 &&
//    idxs.head == labelIndex
//  }
//  test("test delete") {
////    HLabel.findByName(labelName).foreach { label =>
////      label.deleteAll()
////    }
//    HLabel.findByName(newLabelName).foreach { label =>
//      label.deleteAll()
//    }
//    HLabelMeta.findAllByLabelId(label.id.get, useCache = false).isEmpty &&
//    HLabelIndex.findByLabelIdAll(label.id.get, useCache = false).isEmpty
//
//    service.deleteAll()
//  }

//  test("test labelIndex") {
//    println(HLabelIndex.findByLabelIdAll(1))
//  }

}
