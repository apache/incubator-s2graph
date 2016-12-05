/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, Service, ServiceColumn}
import org.apache.s2graph.core.types.{InnerVal, LabelWithDirection}
import scalikejdbc.AutoSession

import scala.concurrent.ExecutionContext
import scala.util.Try

trait TestCommonWithModels {

  import InnerVal._
  import types.HBaseType._

  var graph: S2Graph = _
  var config: Config = _
  var management: Management = _

  def initTests(): Try[Label] = {
    config = ConfigFactory.load()
    graph = new S2Graph(config)(ExecutionContext.Implicits.global)
    management = new Management(graph)

    implicit val session = AutoSession

    deleteTestLabel()
    deleteTestService()

    createTestService()
    createTestLabel()
  }

  def zkQuorum: String = config.getString("hbase.zookeeper.quorum")

  def cluster: String = config.getString("hbase.zookeeper.quorum")

  implicit val session = AutoSession

  val serviceName = "_test_service"
  val serviceNameV2 = "_test_service_v2"
  val serviceNameV3 = "_test_service_v3"
  val serviceNameV4 = "_test_service_v4"

  val columnName = "user_id"
  val columnNameV2 = "user_id_v2"
  val columnNameV3 = "user_id_v3"
  val columnNameV4 = "user_id_v4"

  val columnType = "long"
  val columnTypeV2 = "long"
  val columnTypeV3 = "long"
  val columnTypeV4 = "long"

  val tgtColumnName = "itme_id"
  val tgtColumnNameV2 = "item_id_v2"
  val tgtColumnNameV3 = "item_id_v3"
  val tgtColumnNameV4 = "item_id_v4"

  val tgtColumnType = "string"
  val tgtColumnTypeV2 = "string"
  val tgtColumnTypeV3 = "string"
  val tgtColumnTypeV4 = "string"

  val hTableName = "_test_cases"
  val preSplitSize = 0

  val labelName = "_test_label"
  val labelNameSecure = "_test_label_secure"
  val labelNameV2 = "_test_label_v2"
  val labelNameV3 = "_test_label_v3"
  val labelNameV4 = "_test_label_v4"

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


  def createTestService(): Try[Service] = {
    implicit val session = AutoSession
    management.createService(serviceName, cluster, hTableName, preSplitSize, hTableTTL = None, "gz")
    management.createService(serviceNameV2, cluster, hTableName, preSplitSize, hTableTTL = None, "gz")
    management.createService(serviceNameV3, cluster, hTableName, preSplitSize, hTableTTL = None, "gz")
    management.createService(serviceNameV4, cluster, hTableName, preSplitSize, hTableTTL = None, "gz")
  }

  def deleteTestService(): Unit = {
    implicit val session = AutoSession
    Management.deleteService(serviceName)
    Management.deleteService(serviceNameV2)
    Management.deleteService(serviceNameV3)
    Management.deleteService(serviceNameV4)
  }

  def deleteTestLabel(): Try[String] = {
    implicit val session = AutoSession
    Management.deleteLabel(labelName)
    Management.deleteLabel(labelNameV2)
    Management.deleteLabel(undirectedLabelName)
    Management.deleteLabel(undirectedLabelNameV2)
    Management.deleteLabel(labelNameSecure)
  }

  def createTestLabel(): Try[Label] = {
    implicit val session = AutoSession

    management.createLabel(labelName, serviceName, columnName, columnType, serviceName, columnName, columnType,
      isDirected = true, serviceName, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION1, false, "lg4", None)

    management.createLabel(labelNameV2, serviceNameV2, columnNameV2, columnTypeV2, serviceNameV2, tgtColumnNameV2, tgtColumnTypeV2,
      isDirected = true, serviceNameV2, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION2, false, "lg4", None)

    management.createLabel(labelNameV3, serviceNameV3, columnNameV3, columnTypeV3, serviceNameV3, tgtColumnNameV3, tgtColumnTypeV3,
      isDirected = true, serviceNameV3, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION3, false, "lg4", None)

    management.createLabel(labelNameV4, serviceNameV4, columnNameV4, columnTypeV4, serviceNameV4, tgtColumnNameV4, tgtColumnTypeV4,
      isDirected = true, serviceNameV4, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION4, false, "lg4", None)

    management.createLabel(undirectedLabelName, serviceName, columnName, columnType, serviceName, tgtColumnName, tgtColumnType,
      isDirected = false, serviceName, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION3, false, "lg4", None)

    management.createLabel(undirectedLabelNameV2, serviceNameV2, columnNameV2, columnTypeV2, serviceNameV2, tgtColumnNameV2, tgtColumnTypeV2,
      isDirected = false, serviceName, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION2, false, "lg4", None)

    management.createLabel(labelNameSecure, serviceName, columnName, columnType, serviceName, tgtColumnName, tgtColumnType,
      isDirected = false, serviceName, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, VERSION3, false, "lg4",
      Option("""{ "tokens": ["xxx-yyy", "aaa-bbb"] }"""))
  }

  def service: Service = Service.findByName(serviceName, useCache = false).get

  def serviceV2: Service = Service.findByName(serviceNameV2, useCache = false).get

  def serviceV3: Service = Service.findByName(serviceNameV3, useCache = false).get

  def serviceV4: Service = Service.findByName(serviceNameV4, useCache = false).get

  def column: ServiceColumn = ServiceColumn.find(service.id.get, columnName, useCache = false).get

  def columnV2: ServiceColumn = ServiceColumn.find(serviceV2.id.get, columnNameV2, useCache = false).get

  def columnV3: ServiceColumn = ServiceColumn.find(serviceV3.id.get, columnNameV3, useCache = false).get

  def columnV4: ServiceColumn = ServiceColumn.find(serviceV4.id.get, columnNameV4, useCache = false).get

  def tgtColumn: ServiceColumn = ServiceColumn.find(service.id.get, tgtColumnName, useCache = false).get

  def tgtColumnV2: ServiceColumn = ServiceColumn.find(serviceV2.id.get, tgtColumnNameV2, useCache = false).get

  def tgtColumnV3: ServiceColumn = ServiceColumn.find(serviceV3.id.get, tgtColumnNameV3, useCache = false).get

  def tgtColumnV4: ServiceColumn = ServiceColumn.find(serviceV4.id.get, tgtColumnNameV4, useCache = false).get

  def label: Label = Label.findByName(labelName, useCache = false).get

  def labelV2: Label = Label.findByName(labelNameV2, useCache = false).get

  def labelV3: Label = Label.findByName(labelNameV3, useCache = false).get

  def labelV4: Label = Label.findByName(labelNameV4, useCache = false).get

  def undirectedLabel: Label = Label.findByName(undirectedLabelName, useCache = false).get

  def undirectedLabelV2: Label = Label.findByName(undirectedLabelNameV2, useCache = false).get

  def dir: Int = GraphUtil.directions("out")

  def op: Byte = GraphUtil.operations("insert")

  def labelOrderSeq: Byte = LabelIndex.DefaultSeq

  def labelWithDir: LabelWithDirection = LabelWithDirection(label.id.get, dir)

  def labelWithDirV2: LabelWithDirection = LabelWithDirection(labelV2.id.get, dir)

  def labelWithDirV3: LabelWithDirection = LabelWithDirection(labelV3.id.get, dir)

  def labelWithDirV4: LabelWithDirection = LabelWithDirection(labelV4.id.get, dir)

  def queryParam: QueryParam = QueryParam(labelWithDir)

  def queryParamV2: QueryParam = QueryParam(labelWithDirV2)

  def queryParamV3: QueryParam = QueryParam(labelWithDirV3)

  def queryParamV4: QueryParam = QueryParam(labelWithDirV4)

}
