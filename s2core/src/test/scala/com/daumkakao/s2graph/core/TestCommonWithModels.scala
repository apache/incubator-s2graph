package com.daumkakao.s2graph.core

//import com.daumkakao.s2graph.core.models._
 import com.daumkakao.s2graph.core.mysqls._

import com.daumkakao.s2graph.core.types2.{LabelWithDirection, InnerVal}
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
  val serviceNameV2 = "_test_service_v2"
  val columnName = "user_id"
  val columnNameV2 = "user_id_v2"
  val columnType = "long"
  val columnTypeV2 = "long"
  val cluster = "localhost"
  val hTableName = "_test_cases"
  val preSplitSize = 0
  val labelName = "_test_label"
  val labelNameV2 = "_test_label_v2"
  //FIXME:
  val LABEL = Label
  val LABEMETA = LabelMeta

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
       |db.default.url="jdbc:mysql://nuk151.kr2.iwilab.com:13306/graph_alpha"
     """.stripMargin)
  Graph(config)(ExecutionContext.Implicits.global)
  Model(config)
//  HBaseModel(zkQuorum)




  def initTests() = {
    deleteTestLabel()
    deleteTestService()

    Thread.sleep(1000)

    createTestService()
    createTestLabel()
  }

  def createTestService() = {
    Management.createService(serviceName, cluster, hTableName, preSplitSize, hTableTTL = None)
    Management.createService(serviceNameV2,  cluster, hTableName, preSplitSize, hTableTTL = None)
  }

  def deleteTestService() = {
    Management.deleteService(serviceName)
    Management.deleteService(serviceNameV2)
  }

  def deleteTestLabel() = {
    Management.deleteLabel(labelName)
    Management.deleteLabel(labelNameV2)
  }


  def createTestLabel() = {
    Management.createLabel(labelName, serviceName, columnName, columnType, serviceName, columnName, columnType,
      isDirected = true, serviceName, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, InnerVal.VERSION1)

    Management.createLabel(labelNameV2, serviceNameV2, columnNameV2, columnTypeV2, serviceNameV2, columnNameV2, columnTypeV2,
      isDirected = true, serviceNameV2, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, InnerVal.VERSION2)
  }

  /** */
  initTests()

  lazy val service = Service.findByName(serviceName, useCache = false).get
  lazy val serviceV2 = Service.findByName(serviceNameV2, useCache = false).get

  lazy val column = ServiceColumn.find(service.id.get, columnName, useCache = false).get
  lazy val columnV2 = ServiceColumn.find(serviceV2.id.get, columnNameV2, useCache = false).get

  lazy val label = Label.findByName(labelName, useCache = false).get
  lazy val labelV2 = Label.findByName(labelNameV2, useCache = false).get

  lazy val dir = GraphUtil.directions("out")
  lazy val op = GraphUtil.operations("insert")
  lazy val labelOrderSeq = LabelIndex.defaultSeq

  lazy val labelWithDir = LabelWithDirection(label.id.get, dir)
  lazy val labelWithDirV2 = LabelWithDirection(labelV2.id.get, dir)

  lazy val queryParam = QueryParam(labelWithDir)
  lazy val queryParamV2 = QueryParam(labelWithDirV2)
}
