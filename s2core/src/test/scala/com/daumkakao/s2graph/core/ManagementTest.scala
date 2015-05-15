package com.daumkakao.s2graph.core
import com.daumkakao.s2graph.core.Graph
import com.daumkakao.s2graph.core.models.HBaseModel
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{JsString, JsBoolean, JsNumber, Json}

import scala.concurrent.ExecutionContext

/**
 * Created by shon on 5/15/15.
 */
class ManagementTest extends FunSuite with Matchers {
  val labelName = "test_label"
  val serviceName = "test_service"
  val columnName = "test_column"
  val columnType = "long"
  val indexProps = Seq("weight" -> JsNumber(5), "is_hidden" -> JsBoolean(true))
  val props = Seq("is_blocked" -> JsBoolean(true), "category" -> JsString("sports"))
  val consistencyLevel = "weak"
  val hTableName = Some("testHTable")
  val hTableTTL = Some(86000)
  val preSplitSize = 10
  val zkQuorum = "localhost"

  val config = ConfigFactory.parseString(s"hbase.zookeeper.quorum=$zkQuorum")
  Graph(config)(ExecutionContext.Implicits.global)
  HBaseModel(zkQuorum)

  test("test create service") {
    Management.deleteService(serviceName)
    Management.findService(serviceName) == None
    val service = Management.createService(serviceName, zkQuorum, hTableName.get, preSplitSize, hTableTTL)
    val other = Management.findService(service.serviceName)
    other.isDefined && service == other.get
  }
  test("test create label") {
    Management.deleteLabel(labelName)
    Management.findLabel(labelName) == None
    val label = Management.createLabel(labelName, serviceName, columnName, columnType,
      serviceName, columnName, columnType,
      true, serviceName, indexProps, props,
      consistencyLevel, hTableName, hTableTTL
    )
    val other = Management.findLabel(labelName)
    other.isDefined && label == other.get
  }
}
