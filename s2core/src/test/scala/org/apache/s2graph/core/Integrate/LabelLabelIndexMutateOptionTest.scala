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

package org.apache.s2graph.core.Integrate

import org.apache.s2graph.core._
import org.scalatest.{BeforeAndAfterEach, Tag}
import play.api.libs.json._

class LabelLabelIndexMutateOptionTest extends IntegrateCommon with BeforeAndAfterEach {

  import TestUtil._

  // called by start test, once
  override def initTestData(): Unit = {
    super.initTestData()

    val insert = "insert"
    val e = "e"
    val weight = "weight"
    val is_hidden = "is_hidden"

    insertEdgesSync(
      toEdge(1, insert, e, 0, 1, testLabelNameLabelIndex),
      toEdge(1, insert, e, 0, 2, testLabelNameLabelIndex),
      toEdge(1, insert, e, 0, 3, testLabelNameLabelIndex)
    )
  }

  def getQuery(ids: Seq[Int], direction: String, indexName: String): Query =
    Query(
      vertices = ids.map(graph.toVertex(testServiceName, testColumnName, _)),
      steps = Vector(
        Step(Seq(QueryParam(testLabelNameLabelIndex, direction = direction, indexName = indexName)))
      )
    )

  /**
    * "indices": [
    * {"name": "$index1", "propNames": ["weight", "time", "is_hidden", "is_blocked"]},
    * {"name": "$idxStoreInDropDegree", "propNames": ["time"], "options": { "in": {"storeDegree": false }, "out": {"method": "drop", "storeDegree": false }}},
    * {"name": "$idxStoreOutDropDegree", "propNames": ["weight"], "options": { "out": {"storeDegree": false}, "in": { "method": "drop", "storeDegree": false }}},
    * {"name": "$idxStoreIn", "propNames": ["is_hidden"], "options": { "out": {"method": "drop", "storeDegree": false }}},
    * {"name": "$idxStoreOut", "propNames": ["weight", "is_blocked"], "options": { "in": {"method": "drop", "storeDegree": false }, "out": {"method": "normal" }}},
    * {"name": "$idxDropInStoreDegree", "propNames": ["is_blocked"], "options": { "in": {"method": "drop" }, "out": {"method": "drop", "storeDegree": false }}},
    * {"name": "$idxDropOutStoreDegree", "propNames": ["weight", "is_blocked", "_timestamp"], "options": { "in": {"method": "drop", "storeDegree": false }, "out": {"method": "drop"}}}
    * ],
    **/

  /**
    * index without no options
    */
  test("normal index should store in/out direction Edges with Degrees") {
    var edges = getEdgesSync(getQuery(Seq(0), "out", index1))
    (edges \ "results").as[Seq[JsValue]].size should be(3)
    (edges \\ "_degree").map(_.as[Long]).sum should be(3)

    edges = getEdgesSync(getQuery(Seq(1, 2, 3), "in", index1))
    (edges \ "results").as[Seq[JsValue]].size should be(3)
    (edges \\ "_degree").map(_.as[Long]).sum should be(3)
  }

  /**
    * { "out": {"method": "drop", "storeDegree": false } }
    */
  test("storeDegree: store out direction Edge and drop Degree") {
    val edges = getEdgesSync(getQuery(Seq(0), "out", idxStoreOutDropDegree))
    (edges \ "results").as[Seq[JsValue]].size should be(3)
    (edges \\ "_degree").map(_.as[Long]).sum should be(0)
  }

  /**
    * { "in": { "method": "drop", "storeDegree": false } }
    */
  test("storeDegree: store in direction Edge and drop Degree") {
    val edges = getEdgesSync(getQuery(Seq(1, 2, 3), "in", idxStoreInDropDegree))
    (edges \ "results").as[Seq[JsValue]].size should be(3)
    (edges \\ "_degree").map(_.as[Long]).sum should be(0)
  }

  /**
    * { "out": {"method": "drop", "storeDegree": false } }
    */
  test("index for in direction should drop out direction edge") {
    val edges = getEdgesSync(getQuery(Seq(0), "out", idxStoreIn))
    (edges \ "results").as[Seq[JsValue]].size should be(0)
    (edges \\ "_degree").map(_.as[Long]).sum should be(0)
  }

  test("index for in direction should store in direction edge") {
    val edges = getEdgesSync(getQuery(Seq(1, 2, 3), "in", idxStoreIn))
    (edges \ "results").as[Seq[JsValue]].size should be(3)
    (edges \\ "_degree").map(_.as[Long]).sum should be(3)
  }

  /**
    * { "in": {"method": "drop", "storeDegree": false } }
    */
  test("index for out direction should store out direction edge") {
    val edges = getEdgesSync(getQuery(Seq(0), "out", idxStoreOut))
    (edges \ "results").as[Seq[JsValue]].size should be(3)
    (edges \\ "_degree").map(_.as[Long]).sum should be(3)
  }

  test("index for out direction should drop in direction edge") {
    val edges = getEdgesSync(getQuery(Seq(1, 2, 3), "in", idxStoreOut))
    (edges \ "results").as[Seq[JsValue]].size should be(0)
    (edges \\ "_degree").map(_.as[Long]).sum should be(0)
  }

  /**
    * { "out": {"method": "drop", "storeDegree": false} }
    */
  ignore("index for in direction should drop in direction edge and store degree") {
    val edges = getEdgesSync(getQuery(Seq(1, 2, 3), "in", idxDropInStoreDegree))
    (edges \ "results").as[Seq[JsValue]].size should be(0)
    (edges \\ "_degree").map(_.as[Long]).sum should be(3)
  }

  /**
    * { "in": {"method": "drop", "storeDegree": false }, "out": {"method": "drop"} }
    */
  ignore("index for out direction should drop out direction edge and store degree") {
    val edges = getEdgesSync(getQuery(Seq(0), "out", idxDropOutStoreDegree))
    (edges \ "results").as[Seq[JsValue]].size should be(0)
    (edges \\ "_degree").map(_.as[Long]).sum should be(3)
  }
}
