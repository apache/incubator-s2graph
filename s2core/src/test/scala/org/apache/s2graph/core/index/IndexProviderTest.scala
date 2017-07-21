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

package org.apache.s2graph.core.index

import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.{Management, S2Vertex}
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs}
import scala.collection.JavaConversions._

class IndexProviderTest extends IntegrateCommon {
  val indexProvider = IndexProvider(config)
  val numOfTry = 1

  test("test vertex write/query") {
    import TestUtil._
//    Management.addVertexProp(testServiceName, testColumnName, "time", "long")

    val testService = Service.findByName(TestUtil.testServiceName).get
    val testColumn = ServiceColumn.find(testService.id.get, TestUtil.testColumnName).get
    val vertexId = graph.newVertexId(testServiceName)(testColumnName)(1L)

    val propsWithTs = Map(
//      testColumn.metasInvMap("time") -> InnerVal.withLong(1L, "v4")
      ColumnMeta.timestamp -> InnerVal.withLong(1L, "v4")
    )
    val otherPropsWithTs = Map(
//      testColumn.metasInvMap("time") -> InnerVal.withLong(2L, "v4")
      ColumnMeta.timestamp -> InnerVal.withLong(2L, "v4")
    )
    val vertex = graph.newVertex(vertexId)
    S2Vertex.fillPropsWithTs(vertex, propsWithTs)

    val otherVertex = graph.newVertex(vertexId)
    S2Vertex.fillPropsWithTs(otherVertex, otherPropsWithTs)

    val numOfOthers = 10
    val vertices = Seq(vertex) ++ (0 until numOfOthers).map(_ => otherVertex)

    println(s"[# of vertices]: ${vertices.size}")
    vertices.foreach(v => println(s"[Vertex]: $v"))
    indexProvider.mutateVertices(vertices)

    (0 until numOfTry).foreach { ith =>
      var ids = indexProvider.fetchVertexIds("_timestamp: 1")
      ids.head shouldBe vertex.id

      ids.foreach { id =>
        println(s"[Id]: $id")
      }
    }
  }
  test("test edge write/query ") {
    import TestUtil._
    val testLabelName = TestUtil.testLabelName
    val testLabel = Label.findByName(testLabelName).getOrElse(throw new IllegalArgumentException)
    val vertexId = graph.newVertexId(testServiceName)(testColumnName)(1L)
    val otherVertexId = graph.newVertexId(testServiceName)(testColumnName)(2L)
    val vertex = graph.newVertex(vertexId)
    val otherVertex = graph.newVertex(otherVertexId)

    val propsWithTs = Map(
      LabelMeta.timestamp -> InnerValLikeWithTs.withLong(1L, 1L, "v4"),
      testLabel.metaPropsInvMap("time") -> InnerValLikeWithTs.withLong(10L, 1L, "v4")
    )
    val otherPropsWithTs = Map(
      LabelMeta.timestamp -> InnerValLikeWithTs.withLong(2L, 2L, "v4"),
      testLabel.metaPropsInvMap("time") -> InnerValLikeWithTs.withLong(20L, 1L, "v4")
    )
    val edge = graph.newEdge(vertex, vertex, testLabel, 0, propsWithTs = propsWithTs)
    val otherEdge = graph.newEdge(otherVertex, otherVertex, testLabel, 0, propsWithTs = otherPropsWithTs)
    val numOfOthers = 10
    val edges = Seq(edge) ++ (0 until numOfOthers).map(_ => otherEdge)

    println(s"[# of edges]: ${edges.size}")
    edges.foreach(e => println(s"[Edge]: $e"))
    indexProvider.mutateEdges(edges)

    // match
    (0 until numOfTry).foreach { _ =>

      val ids = indexProvider.fetchEdgeIds("time: 10 AND _timestamp: 1")
      ids.head shouldBe edge.edgeId

      ids.foreach { id =>
        println(s"[Id]: $id")
      }
    }

    // match and not
    (0 until numOfTry).foreach { _ =>
      val ids = indexProvider.fetchEdgeIds("time: 20 AND NOT _timestamp: 1")
      //    ids.size shouldBe 0
      ids.size shouldBe numOfOthers

      ids.foreach { id =>
        id shouldBe otherEdge.edgeId
        println(s"[Id]: $id")
      }
    }

    // range
    (0 until numOfTry).foreach { _ =>
      val ids = indexProvider.fetchEdgeIds("time: [0 TO 10]")
      //    ids.size shouldBe 0
      ids.size shouldBe 1

      ids.foreach { id =>
        id shouldBe edge.edgeId
        println(s"[Id]: $id")
      }
    }
  }
}
