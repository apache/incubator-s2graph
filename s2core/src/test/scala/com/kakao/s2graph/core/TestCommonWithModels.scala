package com.kakao.s2graph.core

import com.kakao.s2graph.core.Management.JsonModel.{Index, Prop}
import com.kakao.s2graph.core.mysqls._

//import com.kakao.s2graph.core.models._


import com.kakao.s2graph.core.types.{InnerVal, LabelWithDirection}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext

trait TestCommonWithModels {

  import InnerVal._
  import types.HBaseType._

  val config = ConfigFactory.load()

  val zkQuorum = config.getString("hbase.zookeeper.quorum")
  val serviceName = "_test_service"
  val serviceNameV2 = "_test_service_v2"
  val columnName = "user_id"
  val columnNameV2 = "user_id_v2"
  val columnType = "long"
  val columnTypeV2 = "long"

  val tgtColumnName = "itme_id"
  val tgtColumnNameV2 = "item_id_v2"
  val tgtColumnType = "string"
  val tgtColumnTypeV2 = "string"

  val cluster = config.getString("hbase.zookeeper.quorum")
  val hTableName = "_test_cases"
  val preSplitSize = 0
  val labelName = "_test_label"
  val labelNameV2 = "_test_label_v2"

  val undirectedLabelName = "_test_label_undirected"
  val undirectedLabelNameV2 = "_test_label_undirected_v2"

  val testProps = Seq(
    Prop("affinity_score", "0.0", DOUBLE),
    Prop("is_blocked", "false", BOOLEAN),
    Prop("time", "0", INT),
    Prop("weight", "0", INT),
    Prop("is_hidden", "true", BOOLEAN),
    Prop("phone_number", "xxx-xxx-xxxx", STRING),
    Prop("score", "0.1", FLOAT),
    Prop("age", "10", INT)
  )
  val testIdxProps = Seq(Index("_PK", Seq("_timestamp", "affinity_score")))
  val consistencyLevel = "strong"
  val hTableTTL = None

  val graph = new Graph(config)(ExecutionContext.Implicits.global)

  def initTests() = {
    deleteTestLabel()
    deleteTestService()

    Thread.sleep(1000)

    createTestService()
    createTestLabel()
  }

  def createTestService() = {
    Management.createService(serviceName, cluster, hTableName, preSplitSize, hTableTTL = None, "gz")
    Management.createService(serviceNameV2, cluster, hTableName, preSplitSize, hTableTTL = None, "gz")
  }

  def deleteTestService() = {
    Management.deleteService(serviceName)
    Management.deleteService(serviceNameV2)
  }

  def deleteTestLabel() = {
    Management.deleteLabel(labelName)
    Management.deleteLabel(labelNameV2)
    Management.deleteLabel(undirectedLabelName)
    Management.deleteLabel(undirectedLabelNameV2)
  }


  def createTestLabel() = {
    Management.createLabel(labelName, serviceName, columnName, columnType, serviceName, columnName, columnType,
      isDirected = true, serviceName, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION1, false, "lg4")

    Management.createLabel(labelNameV2, serviceNameV2, columnNameV2, columnTypeV2, serviceNameV2, tgtColumnNameV2, tgtColumnTypeV2,
      isDirected = true, serviceNameV2, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION2, false, "lg4")

    Management.createLabel(undirectedLabelName, serviceName, columnName, columnType, serviceName, tgtColumnName, tgtColumnType,
      isDirected = false, serviceName, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION1, false, "lg4")

    Management.createLabel(undirectedLabelNameV2, serviceNameV2, columnNameV2, columnTypeV2, serviceNameV2, tgtColumnNameV2, tgtColumnTypeV2,
      isDirected = false, serviceName, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION2, false, "lg4")
  }

  /** */
  initTests()

  lazy val service = Service.findByName(serviceName, useCache = false).get
  lazy val serviceV2 = Service.findByName(serviceNameV2, useCache = false).get

  lazy val column = ServiceColumn.find(service.id.get, columnName, useCache = false).get
  lazy val columnV2 = ServiceColumn.find(serviceV2.id.get, columnNameV2, useCache = false).get

  lazy val tgtColumn = ServiceColumn.find(service.id.get, tgtColumnName, useCache = false).get
  lazy val tgtColumnV2 = ServiceColumn.find(serviceV2.id.get, tgtColumnNameV2, useCache = false).get

  lazy val label = Label.findByName(labelName, useCache = false).get
  lazy val labelV2 = Label.findByName(labelNameV2, useCache = false).get

  lazy val undirectedLabel = Label.findByName(undirectedLabelName, useCache = false).get
  lazy val undirectedLabelV2 = Label.findByName(undirectedLabelNameV2, useCache = false).get

  lazy val dir = GraphUtil.directions("out")
  lazy val op = GraphUtil.operations("insert")
  lazy val labelOrderSeq = LabelIndex.DefaultSeq

  lazy val labelWithDir = LabelWithDirection(label.id.get, dir)
  lazy val labelWithDirV2 = LabelWithDirection(labelV2.id.get, dir)

  lazy val queryParam = QueryParam(labelWithDir)
  lazy val queryParamV2 = QueryParam(labelWithDirV2)
}
