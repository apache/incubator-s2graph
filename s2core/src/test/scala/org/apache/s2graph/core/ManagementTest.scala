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

import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.schema.Label
import play.api.libs.json.Json

class ManagementTest extends IntegrateCommon {


  def checkCopyLabel(originalLabelName: String, newLabelName: String) = {
    val originalLabelOpt = Label.findByName(originalLabelName, useCache = true)
    originalLabelOpt.isDefined should be(true)
    val originalLabel = originalLabelOpt.get

    val labelTry = management.copyLabel(originalLabelName, newLabelName, hTableName = Option(newLabelName))
    labelTry.isSuccess should be(true)
    val copiedLabel = labelTry.get
    copiedLabel.label should be(newLabelName)
    copiedLabel.id.get != originalLabel.id.get should be(true)
    copiedLabel.hTableTTL should equal(originalLabel.hTableTTL)

    val copiedLabelMetaMap = copiedLabel.metas(useCache = false).map(m => m.seq -> m.name).toMap
    val copiedLabelIndiceMap = copiedLabel.indices(useCache = false).map(m => m.seq -> m.metaSeqs).toMap
    val originalLabelMetaMap = originalLabel.metas(useCache = false).map(m => m.seq -> m.name).toMap
    val originalLabelIndiceMap = originalLabel.indices(useCache = false).map(m => m.seq -> m.metaSeqs).toMap

    copiedLabelMetaMap should be(originalLabelMetaMap)
    copiedLabelIndiceMap should be(originalLabelIndiceMap)

    copiedLabel.metas().sortBy(m => m.id.get).map(m => m.name) should be(originalLabel.metas().sortBy(m => m.id.get).map(m => m.name))
    copiedLabel.indices().sortBy(m => m.id.get).map(m => m.metaSeqs) should be(originalLabel.indices().sortBy(m => m.id.get).map(m => m.metaSeqs))
  }

  def checkLabelTTL(labelName: String, serviceName: String, setTTL: Option[Int], checkTTL: Option[Int]) = {
    Management.deleteLabel(labelName)
    val ttlOption = if (setTTL.isDefined) s""", "hTableTTL": ${setTTL.get}""" else ""
    val createLabelJson =
      s"""{
      "label": "$labelName",
      "srcServiceName": "$serviceName",
      "srcColumnName": "id",
      "srcColumnType": "long",
      "tgtServiceName": "$serviceName",
      "tgtColumnName": "id",
      "tgtColumnType": "long",
      "indices":[],
      "props":[],
      "hTableName": "$labelName"
      $ttlOption
    }"""
    val labelOpts = parser.toLabelElements(Json.parse(createLabelJson))

    labelOpts.foreach { label  =>
      label.hTableTTL should be(checkTTL)
    }

  }

  test("copy label test") {
    val labelToCopy = s"${TestUtil.testLabelName}_copied"
    Label.findByName(labelToCopy) match {
      case None =>
      //
      case Some(oldLabel) =>
        Label.delete(oldLabel.id.get)

    }
    checkCopyLabel(TestUtil.testLabelName, labelToCopy)
  }

  test("swap label test") {
    val labelLeft = TestUtil.testLabelName
    val labelRight = TestUtil.testLabelName2
    Management.swapLabelNames(labelLeft, labelRight)
    Label.findByName(labelLeft, false).get.schemaVersion should be("v3")
    Label.findByName(labelRight, false).get.schemaVersion should be("v4")
    Management.swapLabelNames(labelLeft, labelRight)
    Label.findByName(labelLeft, false).get.schemaVersion should be("v4")
    Label.findByName(labelRight, false).get.schemaVersion should be("v3")
  }

  test("check created service without ttl") {
    // createService
    val svc_without_ttl = "s2graph_without_ttl"
    val createServiceJson = s"""{"serviceName" : "$svc_without_ttl"}"""
    val (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) =
      parser.toServiceElements(Json.parse(createServiceJson))

    val tryService = management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
    assert(tryService.isSuccess)
    val service = tryService.get
    assert(service.hTableTTL.isDefined)
    service.hTableTTL.get should be(Integer.MAX_VALUE)

    // check labels
    checkLabelTTL("label_without_ttl", svc_without_ttl, None, service.hTableTTL)
    checkLabelTTL("label_with_ttl", svc_without_ttl, Some(86400), Some(86400))

    // check copied labels
    Management.deleteLabel("label_without_ttl_copied")
    checkCopyLabel("label_without_ttl", "label_without_ttl_copied")
    Management.deleteLabel("label_with_ttl_copied")
    checkCopyLabel("label_with_ttl", "label_with_ttl_copied")
  }

  test("check created service with ttl") {
    // createService
    val svc_with_ttl = "s2graph_with_ttl"
    val ttl_val = 86400
    val (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) =
      parser.toServiceElements(Json.parse(s"""{"serviceName" : "$svc_with_ttl", "hTableTTL":$ttl_val}"""))

    val tryService = management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
    assert(tryService.isSuccess)
    val service = tryService.get
    assert(service.hTableTTL.isDefined)
    service.hTableTTL.get should be(ttl_val)

    // check labels
    checkLabelTTL("label_without_ttl", svc_with_ttl, None, service.hTableTTL)
    checkLabelTTL("label_with_ttl", svc_with_ttl, Some(Integer.MAX_VALUE), Some(Integer.MAX_VALUE))

    // check copied labels
    Management.deleteLabel("label_without_ttl_copied")
    checkCopyLabel("label_without_ttl", "label_without_ttl_copied")
    Management.deleteLabel("label_with_ttl_copied")
    checkCopyLabel("label_with_ttl", "label_with_ttl_copied")
  }

}
