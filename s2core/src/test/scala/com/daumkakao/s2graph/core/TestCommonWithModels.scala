package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.models.{HLabel, HServiceColumn, HService, HBaseModel}
import com.daumkakao.s2graph.core.types.{HBaseType, InnerVal}
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
  val config = ConfigFactory.parseString(s"hbase.zookeeper.quorum=$zkQuorum")
  Graph(config)(ExecutionContext.Implicits.global)
  HBaseModel(zkQuorum)


  val serviceName = "_test_service"
  val columnName = "user_id"
  val columnType = "long"
  val cluster = "localhost"
  val hTableName = "_test_cases"
  val preSplitSize = 0
  val labelName = "_test_label"
  val testProps = Seq(
    ("is_hidden", JsBoolean(true), "boolean"),
    ("phone_number", JsString("xxx-xxx-xxxx"), "string"),
    ("score", JsNumber(0.1), "float"),
    ("age", JsNumber(10), "int")
  )
  val testIdxProps = Seq(
    ("_timestamp", JsNumber(0L), "long"),
    ("affinity_score", JsNumber(0.0), "double")
  )
  val consistencyLevel = "strong"
  val hTableTTL = None

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

  val service = HService.findByName(serviceName, useCache = false).get
  val column = HServiceColumn.find(service.id.get, columnName, useCache = false).get
  val label = HLabel.findByName(labelName, useCache = false).get
}
