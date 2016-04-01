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

import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, Service, ServiceColumn}
import org.apache.s2graph.core.types.{InnerVal, LabelWithDirection}
import scalikejdbc.AutoSession

import scala.concurrent.ExecutionContext

trait TestCommonWithModels extends TestCommon {

  import InnerVal._

  def initTests(ver: String) = {
//    config = ConfigFactory.load()
    graph = new Graph(config)(ExecutionContext.Implicits.global)
    management = new Management(graph)

    implicit val session = AutoSession

    deleteTestLabel(ver)
    deleteTestService(ver)

    createTestService(ver)
    createTestLabel(ver)
  }

  def zkQuorum = config.getString("hbase.zookeeper.quorum")

  def cluster = config.getString("hbase.zookeeper.quorum")

  implicit val session = AutoSession

  def serviceName(ver: String) = s"_test_service_$ver"
  def labelName(ver: String) = s"_test_label_$ver"
  def undirectedLabelName(ver: String) = s"_test_label_undirected_$ver"

  def columnName(ver: String) = s"user_id_$ver"
  def tgtColumnName(ver: String) = s"itme_id_$ver"

  val columnType = "long"
  val tgtColumnType = "string"

  val hTableName = "_test_cases"
  val preSplitSize = 0

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


  def createTestService(ver: String) = {
    implicit val session = AutoSession
    val service = serviceName(ver)
    management.createService(service, cluster, hTableName, preSplitSize, hTableTTL = None, "gz")
  }

  def deleteTestService(ver: String) = {
    implicit val session = AutoSession
    val service = serviceName(ver)
    Management.deleteService(service)
  }

  def deleteTestLabel(ver: String) = {
    implicit val session = AutoSession
    val label = labelName(ver)
    val undirectedLabel = undirectedLabelName(ver)
    Management.deleteLabel(label)
    Management.deleteLabel(undirectedLabel)
  }

  def createTestLabel(ver: String) = {
    implicit val session = AutoSession
    val label = labelName(ver)
    val service = serviceName(ver)
    val column = columnName(ver)
    management.createLabel(label, service, column, columnType, service, column, columnType,
      isDirected = true, service, testIdxProps, testProps, consistencyLevel, Some(hTableName), hTableTTL, ver, false, "lg4")
  }

  def service(ver: String) = Service.findByName(serviceName(ver), useCache = false).get
  def column(ver: String) = ServiceColumn.find(service(ver).id.get, columnName(ver), useCache = false).get
  def tgtColumn(ver: String) = ServiceColumn.find(service(ver).id.get, tgtColumnName(ver), useCache = false).get
  def label(ver: String) = Label.findByName(labelName(ver), useCache = false).get
  def undirectedLabel(ver: String) = Label.findByName(undirectedLabelName(ver), useCache = false).get
  def dir = GraphUtil.directions("out")
  def op = GraphUtil.operations("insert")
  def labelOrderSeq = LabelIndex.DefaultSeq
  def labelWithDir(ver: String) = LabelWithDirection(label(ver).id.get, dir)
  def queryParam(ver: String) = QueryParam(labelWithDir(ver))
}
