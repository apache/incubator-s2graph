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

import org.scalatest.FunSuite

import org.apache.s2graph.core.mysqls.{LabelMeta, ServiceColumn}
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs, VertexId}
import org.apache.s2graph.core.utils.Logger

class S2EdgeTest extends FunSuite with TestCommon with TestCommonWithModels {

  initTests()

  val testLabelMeta1 =
    LabelMeta(Option(-1), labelV2.id.get, "is_blocked", 1.toByte, "true", "boolean")
  val testLabelMeta3 = LabelMeta(Option(-1), labelV2.id.get, "time", 3.toByte, "-1", "long")

  test("buildOperation") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = graph.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(
      InnerVal.withLong(0, schemaVersion),
      1)

    val snapshotEdge = None
    val propsWithTs = Map(timestampProp)
    val requestEdge =
      graph.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L

    val newPropsWithTs = Map(
      timestampProp,
      testLabelMeta1 -> InnerValLikeWithTs(InnerVal.withBoolean(false, schemaVersion), 1)
    )

    val edgeMutate =
      S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, propsWithTs, newPropsWithTs)
    Logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.isDefined)
    assert(edgeMutate.edgesToInsert.nonEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }

  test("buildMutation: snapshotEdge: None with newProps") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = graph.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(
      InnerVal.withLong(0, schemaVersion),
      1)

    val snapshotEdge = None
    val propsWithTs = Map(timestampProp)
    val requestEdge =
      graph.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L

    val newPropsWithTs = Map(
      timestampProp,
      testLabelMeta1 -> InnerValLikeWithTs(InnerVal.withBoolean(false, schemaVersion), 1)
    )

    val edgeMutate =
      S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, propsWithTs, newPropsWithTs)
    Logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.isDefined)
    assert(edgeMutate.edgesToInsert.nonEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }

  test("buildMutation: oldPropsWithTs == newPropsWithTs, Drop all requests") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = graph.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(
      InnerVal.withLong(0, schemaVersion),
      1)

    val snapshotEdge = None
    val propsWithTs = Map(timestampProp)
    val requestEdge =
      graph.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L

    val newPropsWithTs = propsWithTs

    val edgeMutate =
      S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, propsWithTs, newPropsWithTs)
    Logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.isEmpty)
    assert(edgeMutate.edgesToInsert.isEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }

  test("buildMutation: All props older than snapshotEdge's LastDeletedAt") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = graph.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(
      InnerVal.withLong(0, schemaVersion),
      1)
    val oldPropsWithTs = Map(
      timestampProp,
      LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 3)
    )

    val propsWithTs = Map(
      timestampProp,
      testLabelMeta3 -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 2),
      LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 3)
    )

    val _snapshotEdge = graph.newEdge(srcVertex,
      tgtVertex,
      labelV2,
      labelWithDirV2.dir,
      op = GraphUtil.operations("delete"),
      propsWithTs = propsWithTs)

    val snapshotEdge = Option(_snapshotEdge)

    val requestEdge =
      graph.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L
    val edgeMutate =
      S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, oldPropsWithTs, propsWithTs)
    Logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.nonEmpty)
    assert(edgeMutate.edgesToInsert.isEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }

  test("buildMutation: All props newer than snapshotEdge's LastDeletedAt") {
    val schemaVersion = "v2"
    val vertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("dummy", schemaVersion))
    val srcVertex = graph.newVertex(vertexId)
    val tgtVertex = srcVertex

    val timestampProp = LabelMeta.timestamp -> InnerValLikeWithTs(
      InnerVal.withLong(0, schemaVersion),
      1)
    val oldPropsWithTs = Map(
      timestampProp,
      LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 3)
    )

    val propsWithTs = Map(
      timestampProp,
      testLabelMeta3 -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 4),
      LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, schemaVersion), 3)
    )

    val _snapshotEdge = graph.newEdge(srcVertex,
      tgtVertex,
      labelV2,
      labelWithDirV2.dir,
      op = GraphUtil.operations("delete"),
      propsWithTs = propsWithTs)

    val snapshotEdge = Option(_snapshotEdge)

    val requestEdge =
      graph.newEdge(srcVertex, tgtVertex, labelV2, labelWithDirV2.dir, propsWithTs = propsWithTs)

    val newVersion = 0L
    val edgeMutate =
      S2Edge.buildMutation(snapshotEdge, requestEdge, newVersion, oldPropsWithTs, propsWithTs)
    Logger.info(edgeMutate.toLogString)

    assert(edgeMutate.newSnapshotEdge.nonEmpty)
    assert(edgeMutate.edgesToInsert.nonEmpty)
    assert(edgeMutate.edgesToDelete.isEmpty)
  }
}
