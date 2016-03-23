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

package org.apache.s2graph.core.storage.hbase

import org.apache.s2graph.core.mysqls.{Label, LabelIndex, LabelMeta}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.{HBaseTest, IndexEdge, TestCommonWithModels, Vertex}
import org.scalatest.{FunSuite, Matchers}


class IndexEdgeTest extends FunSuite with Matchers with TestCommonWithModels {
  initTests()

  /**
   * check if storage serializer/deserializer can translate from/to bytes array.
   * @param l: label for edge.
   * @param ts: timestamp for edge.
   * @param to: to VertexId for edge.
   * @param props: expected props of edge.
   */
  def check(l: Label, ts: Long, to: InnerValLike, props: Map[Byte, InnerValLike]): Unit = {
    val from = InnerVal.withLong(1, l.schemaVersion)
    val vertexId = SourceVertexId(GraphType.DEFAULT_COL_ID, from)
    val tgtVertexId = TargetVertexId(GraphType.DEFAULT_COL_ID, to)
    val vertex = Vertex(vertexId, ts)
    val tgtVertex = Vertex(tgtVertexId, ts)
    val labelWithDir = LabelWithDirection(l.id.get, 0)

    val indexEdge = IndexEdge(vertex, tgtVertex, labelWithDir, 0, ts, LabelIndex.DefaultSeq, props)
    val _indexEdgeOpt = graph.storage.indexEdgeDeserializer(l.schemaVersion).fromKeyValues(queryParam,
      graph.storage.indexEdgeSerializer(indexEdge).toKeyValues, l.schemaVersion, None)

    _indexEdgeOpt should not be empty
    indexEdge should be(_indexEdgeOpt.get)
  }


  /** note that props have to be properly set up for equals */
  test("test serializer/deserializer for index edge.", HBaseTest) {
    val ts = System.currentTimeMillis()
    for {
      l <- Seq(label, labelV2, labelV3, labelV4)
    } {
      val to = InnerVal.withLong(101, l.schemaVersion)
      val tsInnerVal = InnerVal.withLong(ts, l.schemaVersion)
      val props = Map(LabelMeta.timeStampSeq -> tsInnerVal,
        1.toByte -> InnerVal.withDouble(2.1, l.schemaVersion))

      check(l, ts, to, props)
    }
  }

  test("test serializer/deserializer for degree edge.", HBaseTest) {
    val ts = System.currentTimeMillis()
    for {
      l <- Seq(label, labelV2, labelV3, labelV4)
    } {
      val to = InnerVal.withStr("0", l.schemaVersion)
      val tsInnerVal = InnerVal.withLong(ts, l.schemaVersion)
      val props = Map(
        LabelMeta.degreeSeq -> InnerVal.withLong(10, l.schemaVersion),
        LabelMeta.timeStampSeq -> tsInnerVal)

      check(l, ts, to, props)
    }
  }

  test("test serializer/deserializer for incrementCount index edge.", HBaseTest) {
    val ts = System.currentTimeMillis()
    for {
      l <- Seq(label, labelV2, labelV3, labelV4)
    } {
      val to = InnerVal.withLong(101, l.schemaVersion)

      val tsInnerVal = InnerVal.withLong(ts, l.schemaVersion)
      val props = Map(LabelMeta.timeStampSeq -> tsInnerVal,
        1.toByte -> InnerVal.withDouble(2.1, l.schemaVersion),
        LabelMeta.countSeq -> InnerVal.withLong(10, l.schemaVersion))

      check(l, ts, to, props)
    }
  }
}
