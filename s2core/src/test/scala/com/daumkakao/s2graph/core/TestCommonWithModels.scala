package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.models._
import com.daumkakao.s2graph.core.types.{LabelWithDirection, HBaseType, InnerVal}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{KeyValue, PutRequest}
import play.api.libs.json.{JsNumber, JsString, JsBoolean}

import scala.concurrent.ExecutionContext

/**
 * Created by shon on 6/1/15.
 */
trait TestCommonWithModels {
  val zkQuorum = "localhost"
  val serviceName = "_test_service"
  val columnName = "user_id"
  val columnType = "long"
  val cluster = "localhost"
  val hTableName = "_test_cases"
  val preSplitSize = 0
  val labelName = "_test_label"
  val testProps = Seq(
    ("is_blocked", JsBoolean(false), InnerVal.BOOLEAN),
    ("time", JsNumber(0), InnerVal.INT),
    ("weight", JsNumber(0), InnerVal.INT),
    ("is_hidden", JsBoolean(true), InnerVal.BOOLEAN),
    ("phone_number", JsString("xxx-xxx-xxxx"), InnerVal.STRING),
    ("score", JsNumber(0.1), InnerVal.FLOAT),
    ("age", JsNumber(10), InnerVal.INT)
  )
  val testIdxProps = Seq(
    ("_timestamp", JsNumber(0L), "long"),
    ("affinity_score", JsNumber(0.0), "double")
  )
  val consistencyLevel = "strong"
  val hTableTTL = None


  val config = ConfigFactory.parseString(
    s"""hbase.zookeeper.quorum=$zkQuorum
     """.stripMargin)
  Graph(config)(ExecutionContext.Implicits.global)
  HBaseModel(zkQuorum)




  def initTests() = {
    deleteTestLabel()
    deleteTestService()

    Thread.sleep(10000)

    createTestService()
    createTestLabel()
  }

  def createTestService() = {
    Management.createService(serviceName, cluster, hTableName, preSplitSize, hTableTTL = None)
  }

  def deleteTestService() =
    Management.deleteService(serviceName)

  def deleteTestLabel() =
    Management.deleteLabel(labelName)

  def createTestLabel() = {
    Management.createLabel(labelName, serviceName, columnName, columnType, serviceName, columnName, columnType,
      isDirected = true, serviceName, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL)
  }

  /** */
  initTests()

  lazy val service = HService.findByName(serviceName, useCache = false).get
  lazy val column = HServiceColumn.find(service.id.get, columnName, useCache = false).get
  lazy val label = HLabel.findByName(labelName, useCache = false).get
  lazy val dir = GraphUtil.directions("out")
  lazy val op = GraphUtil.operations("insert")
  lazy val labelOrderSeq = HLabelIndex.defaultSeq
  lazy val labelWithDir = LabelWithDirection(label.id.get, dir)
  lazy val queryParam = QueryParam(labelWithDir)
}
