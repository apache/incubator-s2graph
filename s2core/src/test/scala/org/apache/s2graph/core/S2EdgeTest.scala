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

import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.schema.{ServiceColumn, LabelMeta}
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs, VertexId}
import org.apache.s2graph.core.utils.logger
import org.scalatest.FunSuite
import play.api.libs.json.{JsObject, Json}

class S2EdgeTest extends FunSuite with TestCommon with TestCommonWithModels {
  import S2Edge._
  initTests()

  val testLabelMeta1 = LabelMeta(Option(-1), labelV2.id.get, "is_blocked", 1.toByte, "true", "boolean")
  val testLabelMeta3 = LabelMeta(Option(-1), labelV2.id.get, "time", 3.toByte, "-1", "long")

//  test("toLogString") {
//    val testServiceName = serviceNameV2
//    val testLabelName = labelNameV2
//    val bulkQueries = List(
//      ("1445240543366", "update", "{\"is_blocked\":true}"),
//      ("1445240543362", "insert", "{\"is_hidden\":false}"),
//      ("1445240543364", "insert", "{\"is_hidden\":false,\"weight\":10}"),
//      ("1445240543363", "delete", "{}"),
//      ("1445240543365", "update", "{\"time\":1, \"weight\":-10}"))
//
//    val (srcId, tgtId, labelName) = ("1", "2", testLabelName)
//
//    val bulkEdge = (for ((ts, op, props) <- bulkQueries) yield {
//      val properties = fromJsonToProperties(Json.parse(props).as[JsObject])
//      Edge.toEdge(srcId, tgtId, labelName, "out", properties, ts.toLong, op).toLogString
//    }).mkString("\n")
//
//    val attachedProps = "\"from\":\"1\",\"to\":\"2\",\"label\":\"" + testLabelName +
//      "\",\"service\":\"" + testServiceName + "\""
//    val expected = Seq(
//      Seq("1445240543366", "update", "e", "1", "2", testLabelName, "{" + attachedProps + ",\"is_blocked\":true}"),
//      Seq("1445240543362", "insert", "e", "1", "2", testLabelName, "{" + attachedProps + ",\"is_hidden\":false}"),
//      Seq("1445240543364", "insert", "e", "1", "2", testLabelName, "{" + attachedProps + ",\"is_hidden\":false,\"weight\":10}"),
//      Seq("1445240543363", "delete", "e", "1", "2", testLabelName, "{" + attachedProps + "}"),
//      Seq("1445240543365", "update", "e", "1", "2", testLabelName, "{" + attachedProps + ",\"time\":1,\"weight\":-10}")
//    ).map(_.mkString("\t")).mkString("\n")
//
//    assert(bulkEdge === expected)
//  }

  test("buildOperation") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = builder.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 1)

    val snapshotEdge = None
    val propsWithTs = Map(timestampProp)
    val requestEdge = builder.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L

    val newPropsWithTs = Map(
      timestampProp,
      testLabelMeta1 -> InnerValLikeWithTs(InnerVal.withBoolean(false, schemaVersion), 1)
    )

    val edgeMutate = S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, propsWithTs, newPropsWithTs)
    logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.isDefined)
    assert(edgeMutate.edgesToInsert.nonEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }

  test("buildMutation: snapshotEdge: None with newProps") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = builder.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 1)

    val snapshotEdge = None
    val propsWithTs = Map(timestampProp)
    val requestEdge = builder.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L

    val newPropsWithTs = Map(
      timestampProp,
      testLabelMeta1 -> InnerValLikeWithTs(InnerVal.withBoolean(false, schemaVersion), 1)
    )

    val edgeMutate = S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, propsWithTs, newPropsWithTs)
    logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.isDefined)
    assert(edgeMutate.edgesToInsert.nonEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }

  test("buildMutation: oldPropsWithTs == newPropsWithTs, Drop all requests") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = builder.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 1)

    val snapshotEdge = None
    val propsWithTs = Map(timestampProp)
    val requestEdge = builder.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L

    val newPropsWithTs = propsWithTs

    val edgeMutate = S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, propsWithTs, newPropsWithTs)
    logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.isEmpty)
    assert(edgeMutate.edgesToInsert.isEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }

  test("buildMutation: All props older than snapshotEdge's LastDeletedAt") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = builder.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 1)
    val oldPropsWithTs = Map(
      timestampProp,
      LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 3)
    )

    val propsWithTs = Map(
      timestampProp,
      testLabelMeta3 -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 2),
      LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 3)
    )

    val _snapshotEdge = builder.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, op = GraphUtil.operations("delete"), propsWithTs = propsWithTs)

    val snapshotEdge = Option(_snapshotEdge)


    val requestEdge = builder.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L
    val edgeMutate = S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, oldPropsWithTs, propsWithTs)
    logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.nonEmpty)
    assert(edgeMutate.edgesToInsert.isEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }

  test("buildMutation: All props newer than snapshotEdge's LastDeletedAt") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = builder.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 1)
    val oldPropsWithTs = Map(
      timestampProp,
      LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 3)
    )

    val propsWithTs = Map(
      timestampProp,
      testLabelMeta3 -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 4),
      LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 3)
    )

    val _snapshotEdge = builder.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, op = GraphUtil.operations("delete"), propsWithTs = propsWithTs)

    val snapshotEdge = Option(_snapshotEdge)

    val requestEdge = builder.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L
    val edgeMutate = S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, oldPropsWithTs, propsWithTs)
    logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.nonEmpty)
    assert(edgeMutate.edgesToInsert.nonEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }
}
