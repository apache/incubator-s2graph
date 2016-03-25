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

import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs, VertexId}
import org.apache.s2graph.core.utils.logger
import org.scalatest.FunSuite

class EdgeTest extends FunSuite with TestCommon with TestCommonWithModels {

  versions map { n =>
    val ver = s"v$n"
    val tag = getTag(ver)
    initTests(ver)

    
    val label = labelName(ver)
    val dirLabel = labelWithDir(ver)
//    test(s"toLogString $ver", tag) {
//      val bulkQueries = List(
//        ("1445240543366", "update", "{\"is_blocked\":true}"),
//        ("1445240543362", "insert", "{\"is_hidden\":false}"),
//        ("1445240543364", "insert", "{\"is_hidden\":false,\"weight\":10}"),
//        ("1445240543363", "delete", "{}"),
//        ("1445240543365", "update", "{\"time\":1, \"weight\":-10}"))
//
//      val (srcId, tgtId, labelName) = ("1", "2", label)
//
//      val bulkEdge = (for ((ts, op, props) <- bulkQueries) yield {
//        Management.toEdge(ts.toLong, op, srcId, tgtId, labelName, "out", props).toLogString
//      }).mkString("\n")
//
//      val expected = Seq(
//        Seq("1445240543366", "update", "e", "1", "2", label, "{\"is_blocked\":true}"),
//        Seq("1445240543362", "insert", "e", "1", "2", label, "{\"is_hidden\":false}"),
//        Seq("1445240543364", "insert", "e", "1", "2", label, "{\"is_hidden\":false,\"weight\":10}"),
//        Seq("1445240543363", "delete", "e", "1", "2", label),
//        Seq("1445240543365", "update", "e", "1", "2", label, "{\"time\":1,\"weight\":-10}")
//      ).map(_.mkString("\t")).mkString("\n")
//
//      assert(bulkEdge === expected)
//    }

    test(s"buildOperation $ver", tag) {
      val vertexId = VertexId(0, InnerVal.withStr("dummy", ver))
      val srcVertex = Vertex(vertexId)
      val tgtVertex = srcVertex

      val timestampProp = LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 1)

      val snapshotEdge = None
      val propsWithTs = Map(timestampProp)
      val requestEdge = Edge(srcVertex, tgtVertex, dirLabel, propsWithTs = propsWithTs)
      val newVersion = 0L

      val newPropsWithTs = Map(
        timestampProp,
        1.toByte -> InnerValLikeWithTs(InnerVal.withBoolean(false, ver), 1)
      )

      val edgeMutate = Edge.buildMutation(snapshotEdge, requestEdge, newVersion, propsWithTs, newPropsWithTs)
      logger.info(edgeMutate.toLogString)

      assert(edgeMutate.newSnapshotEdge.isDefined)
      assert(edgeMutate.edgesToInsert.nonEmpty)
      assert(edgeMutate.edgesToDelete.isEmpty)
    }

//    test(s"buildMutation: snapshotEdge: None with newProps $ver", tag) {
//      val vertexId = VertexId(0, InnerVal.withStr("dummy", ver))
//      val srcVertex = Vertex(vertexId)
//      val tgtVertex = srcVertex
//
//      val timestampProp = LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 1)
//
//      val snapshotEdge = None
//      val propsWithTs = Map(timestampProp)
//      val requestEdge = Edge(srcVertex, tgtVertex, dirLabel, propsWithTs = propsWithTs)
//      val newVersion = 0L
//
//      val newPropsWithTs = Map(
//        timestampProp,
//        1.toByte -> InnerValLikeWithTs(InnerVal.withBoolean(false, ver), 1)
//      )
//
//      val edgeMutate = Edge.buildMutation(snapshotEdge, requestEdge, newVersion, propsWithTs, newPropsWithTs)
//      logger.info(edgeMutate.toLogString)
//
//      assert(edgeMutate.newSnapshotEdge.isDefined)
//      assert(edgeMutate.edgesToInsert.nonEmpty)
//      assert(edgeMutate.edgesToDelete.isEmpty)
//    }
//
//    test(s"buildMutation: oldPropsWithTs == newPropsWithTs, Drop all requests $ver", tag) {
//      val vertexId = VertexId(0, InnerVal.withStr("dummy", ver))
//      val srcVertex = Vertex(vertexId)
//      val tgtVertex = srcVertex
//
//      val timestampProp = LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 1)
//
//      val snapshotEdge = None
//      val propsWithTs = Map(timestampProp)
//      val requestEdge = Edge(srcVertex, tgtVertex, dirLabel, propsWithTs = propsWithTs)
//      val newVersion = 0L
//
//      val newPropsWithTs = propsWithTs
//
//      val edgeMutate = Edge.buildMutation(snapshotEdge, requestEdge, newVersion, propsWithTs, newPropsWithTs)
//      logger.info(edgeMutate.toLogString)
//
//      assert(edgeMutate.newSnapshotEdge.isEmpty)
//      assert(edgeMutate.edgesToInsert.isEmpty)
//      assert(edgeMutate.edgesToDelete.isEmpty)
//    }
//
//    test(s"buildMutation: All props older than snapshotEdge's LastDeletedAt $ver", tag) {
//      val vertexId = VertexId(0, InnerVal.withStr("dummy", ver))
//      val srcVertex = Vertex(vertexId)
//      val tgtVertex = srcVertex
//
//      val timestampProp = LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 1)
//      val oldPropsWithTs = Map(
//        timestampProp,
//        LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 3)
//      )
//
//      val propsWithTs = Map(
//        timestampProp,
//        3.toByte -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 2),
//        LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 3)
//      )
//
//      val snapshotEdge =
//        Option(Edge(srcVertex, tgtVertex, dirLabel, op = GraphUtil.operations("delete"), propsWithTs = oldPropsWithTs))
//
//      val requestEdge = Edge(srcVertex, tgtVertex, dirLabel, propsWithTs = propsWithTs)
//      val newVersion = 0L
//      val edgeMutate = Edge.buildMutation(snapshotEdge, requestEdge, newVersion, oldPropsWithTs, propsWithTs)
//      logger.info(edgeMutate.toLogString)
//
//      assert(edgeMutate.newSnapshotEdge.nonEmpty)
//      assert(edgeMutate.edgesToInsert.isEmpty)
//      assert(edgeMutate.edgesToDelete.isEmpty)
//    }
//
//    test(s"buildMutation: All props newer than snapshotEdge's LastDeletedAt $ver", tag) {
//      val vertexId = VertexId(0, InnerVal.withStr("dummy", ver))
//      val srcVertex = Vertex(vertexId)
//      val tgtVertex = srcVertex
//
//      val timestampProp = LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 1)
//      val oldPropsWithTs = Map(
//        timestampProp,
//        LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 3)
//      )
//
//      val propsWithTs = Map(
//        timestampProp,
//        3.toByte -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 4),
//        LabelMeta.lastDeletedAt -> InnerValLikeWithTs(InnerVal.withLong(0, ver), 3)
//      )
//
//      val snapshotEdge =
//        Option(Edge(srcVertex, tgtVertex, dirLabel, op = GraphUtil.operations("delete"), propsWithTs = oldPropsWithTs))
//
//      val requestEdge = Edge(srcVertex, tgtVertex, dirLabel, propsWithTs = propsWithTs)
//      val newVersion = 0L
//      val edgeMutate = Edge.buildMutation(snapshotEdge, requestEdge, newVersion, oldPropsWithTs, propsWithTs)
//      logger.info(edgeMutate.toLogString)
//
//      assert(edgeMutate.newSnapshotEdge.nonEmpty)
//      assert(edgeMutate.edgesToInsert.nonEmpty)
//      assert(edgeMutate.edgesToDelete.isEmpty)
//    }
  }
}

