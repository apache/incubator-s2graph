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

package org.apache.s2graph.core.Integrate.tinkerpop

import org.apache.s2graph.core.mysqls.Label
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{S2Graph, S2Vertex, TestCommonWithModels}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.{Edge, T, Vertex}
import org.scalatest.{FunSuite, Matchers}

class S2GraphTest extends FunSuite with Matchers with TestCommonWithModels {

  import scala.collection.JavaConversions._
  import scala.concurrent.ExecutionContext.Implicits.global

  initTests()

  val g = new S2Graph(config)

  def printEdges(edges: Seq[Edge]): Unit =
    edges.foreach { edge =>
      logger.debug(s"[FetchedEdge]: $edge")
    }

  import scala.language.implicitConversions

  def newVertexId(id: Any, label: Label = labelV2): VertexId =
    g.newVertexId(label.srcService, label.srcColumn, id)

  def addVertex(id: AnyRef, label: Label = labelV2): S2Vertex =
    g.addVertex(
        T.label,
        label.srcService.serviceName + S2Vertex.VertexLabelDelimiter + label.srcColumnName,
        T.id,
        id)
      .asInstanceOf[S2Vertex]

  val srcId = Long.box(20)
  val range = (100 until 110)
  testData(srcId, range)

  //  val testProps = Seq(
  //    Prop("affinity_score", "0.0", DOUBLE),
  //    Prop("is_blocked", "false", BOOLEAN),
  //    Prop("time", "0", INT),
  //    Prop("weight", "0", INT),
  //    Prop("is_hidden", "true", BOOLEAN),
  //    Prop("phone_number", "xxx-xxx-xxxx", STRING),
  //    Prop("score", "0.1", FLOAT),
  //    Prop("age", "10", INT)
  //  )
  def testData(srcId: AnyRef, range: Range, label: Label = labelV2): Unit = {
    val src = addVertex(srcId)

    for {
      i <- range
    } {
      val tgt = addVertex(Int.box(i))

      src.addEdge(labelV2.label,
                  tgt,
                  "age",
                  Int.box(10),
                  "affinity_score",
                  Double.box(0.1),
                  "is_blocked",
                  Boolean.box(true),
                  "ts",
                  Long.box(i))
    }
  }

  test("test traversal.") {
    val vertices = g.traversal().V(newVertexId(srcId)).out(labelV2.label).toSeq

    vertices.size should be(range.size)
    range.reverse.zip(vertices).foreach {
      case (tgtId, vertex) =>
        val vertexId = g.newVertexId(labelV2.tgtService, labelV2.tgtColumn, tgtId)
        val expectedId = g.newVertex(vertexId)
        vertex.asInstanceOf[S2Vertex].innerId should be(expectedId.innerId)
    }
  }

  test("test traversal. limit 1") {
    val vertexIdParams = Seq(newVertexId(srcId))
    val t: GraphTraversal[Vertex, Double] =
      g.traversal().V(vertexIdParams: _*).outE(labelV2.label).limit(1).values("affinity_score")
    for {
      affinityScore <- t
    } {
      logger.debug(s"$affinityScore")
      affinityScore should be(0.1)
    }
  }
  test("test traversal. 3") {

    val l = label

    val srcA = addVertex(Long.box(1), l)
    val srcB = addVertex(Long.box(2), l)
    val srcC = addVertex(Long.box(3), l)

    val tgtA = addVertex(Long.box(101), l)
    val tgtC = addVertex(Long.box(103), l)

    srcA.addEdge(l.label, tgtA)
    srcA.addEdge(l.label, tgtC)
    tgtC.addEdge(l.label, srcB)
    tgtA.addEdge(l.label, srcC)

    val vertexIdParams = Seq(srcA.id)
    val vertices = g.traversal().V(vertexIdParams: _*).out(l.label).out(l.label).toSeq
    vertices.size should be(2)
    vertices.foreach { v =>
      val vertex = v.asInstanceOf[S2Vertex]
      // TODO: we have too many id. this is ugly and confusing so fix me.
      vertex.id.innerId == srcB.id.innerId || vertex.id.innerId == srcC.id.innerId should be(true)
      logger.debug(s"[Vertex]: $v")
    }
  }
}
