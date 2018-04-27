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

import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.TestCommonWithModels
import org.scalatest.{FunSuite, Matchers}


class IndexEdgeTest extends FunSuite with Matchers with TestCommonWithModels {
  initTests()

  val testLabelMeta = LabelMeta(Option(-1), labelV2.id.get, "affinity_score", 1.toByte, "0.0", "double")
  /**
   * check if storage serializer/deserializer can translate from/to bytes array.
   * @param l: label for edge.
   * @param ts: timestamp for edge.
   * @param to: to VertexId for edge.
   * @param props: expected props of edge.
   */
  def check(l: Label, ts: Long, to: InnerValLike, props: Map[LabelMeta, InnerValLikeWithTs]): Unit = {
    val from = InnerVal.withLong(1, l.schemaVersion)
    val vertexId = SourceVertexId(ServiceColumn.Default, from)
    val tgtVertexId = TargetVertexId(ServiceColumn.Default, to)
    val vertex = builder.newVertex(vertexId, ts)
    val tgtVertex = builder.newVertex(tgtVertexId, ts)
    val labelWithDir = LabelWithDirection(l.id.get, 0)
    val labelOpt = Option(l)
    val edge = builder.newEdge(vertex, tgtVertex, l, labelWithDir.dir, 0, ts, props, tsInnerValOpt = Option(InnerVal.withLong(ts, l.schemaVersion)))
    val indexEdge = edge.edgesWithIndex.find(_.labelIndexSeq == LabelIndex.DefaultSeq).head
    val kvs = graph.getStorage(l).serDe.indexEdgeSerializer(indexEdge).toKeyValues
    val _indexEdgeOpt = graph.getStorage(l).serDe.indexEdgeDeserializer(l.schemaVersion).fromKeyValues(kvs, None)

    _indexEdgeOpt should not be empty
    edge == _indexEdgeOpt.get should be(true)
  }


  /** note that props have to be properly set up for equals */
  test("test serializer/deserializer for index edge.") {
    val ts = System.currentTimeMillis()
    for {
      l <- Seq(label, labelV2, labelV3, labelV4)
    } {
      val to = InnerVal.withLong(101, l.schemaVersion)
      val tsInnerValWithTs = InnerValLikeWithTs.withLong(ts, ts, l.schemaVersion)
      val props = Map(LabelMeta.timestamp -> tsInnerValWithTs,
        testLabelMeta -> InnerValLikeWithTs.withDouble(2.1, ts, l.schemaVersion))

      check(l, ts, to, props)
    }
  }

  test("test serializer/deserializer for degree edge.") {
    val ts = System.currentTimeMillis()
    for {
      l <- Seq(label, labelV2, labelV3, labelV4)
    } {
      val to = InnerVal.withStr("0", l.schemaVersion)
      val tsInnerValWithTs = InnerValLikeWithTs.withLong(ts, ts, l.schemaVersion)
      val props = Map(
        LabelMeta.degree -> InnerValLikeWithTs.withLong(10, ts, l.schemaVersion),
        LabelMeta.timestamp -> tsInnerValWithTs)

      check(l, ts, to, props)
    }
  }

  test("test serializer/deserializer for incrementCount index edge.") {
    val ts = System.currentTimeMillis()
    for {
      l <- Seq(label, labelV2, labelV3, labelV4)
    } {
      val to = InnerVal.withLong(101, l.schemaVersion)
      val tsInnerValWithTs = InnerValLikeWithTs.withLong(ts, ts, l.schemaVersion)
      val props = Map(LabelMeta.timestamp -> tsInnerValWithTs,
        testLabelMeta -> InnerValLikeWithTs.withDouble(2.1, ts, l.schemaVersion),
        LabelMeta.count -> InnerValLikeWithTs.withLong(10, ts, l.schemaVersion))


      check(l, ts, to, props)
    }
  }
}
