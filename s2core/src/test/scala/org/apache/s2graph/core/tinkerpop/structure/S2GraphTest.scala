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

package org.apache.s2graph.core.tinkerpop.structure

import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core.mysqls.Label
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{Management, S2Graph, S2Vertex, TestCommonWithModels}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.{Edge, T, Vertex}
import org.scalatest.{FunSuite, Matchers}


class S2GraphTest extends FunSuite with Matchers with TestCommonWithModels {

  import scala.collection.JavaConversions._
  import scala.concurrent.ExecutionContext.Implicits.global

  initTests()

  val g = new S2Graph(config)

  def printEdges(edges: Seq[Edge]): Unit = {
    edges.foreach { edge =>
      logger.debug(s"[FetchedEdge]: $edge")
    }
  }

  import scala.language.implicitConversions

//  def newVertexId(id: Any, label: Label = labelV2) = g.newVertexId(label.srcService, label.srcColumn, id)
//
//  def addVertex(id: AnyRef, label: Label = labelV2) =
//    g.addVertex(T.label, label.srcService.serviceName + S2Vertex.VertexLabelDelimiter + label.srcColumnName,
//      T.id, id).asInstanceOf[S2Vertex]
//
//  val srcId = Long.box(20)
//  val range = (100 until 110)
//  testData(srcId, range)

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
//  def testData(srcId: AnyRef, range: Range, label: Label = labelV2) = {
//    val src = addVertex(srcId)
//
//    for {
//      i <- range
//    } {
//      val tgt = addVertex(Int.box(i))
//
//      src.addEdge(labelV2.label, tgt,
//        "age", Int.box(10),
//        "affinity_score", Double.box(0.1),
//        "is_blocked", Boolean.box(true),
//        "ts", Long.box(i))
//    }
//  }

//  test("test traversal.") {
//    val vertices = g.traversal().V(newVertexId(srcId)).out(labelV2.label).toSeq
//
//    vertices.size should be(range.size)
//    range.reverse.zip(vertices).foreach { case (tgtId, vertex) =>
//      val vertexId = g.newVertexId(labelV2.tgtService, labelV2.tgtColumn, tgtId)
//      val expectedId = g.newVertex(vertexId)
//      vertex.asInstanceOf[S2Vertex].innerId should be(expectedId.innerId)
//    }
//  }
//
//  test("test traversal. limit 1") {
//    val vertexIdParams = Seq(newVertexId(srcId))
//    val t: GraphTraversal[Vertex, Double] = g.traversal().V(vertexIdParams: _*).outE(labelV2.label).limit(1).values("affinity_score")
//    for {
//      affinityScore <- t
//    } {
//      logger.debug(s"$affinityScore")
//      affinityScore should be (0.1)
//    }
//  }
//  test("test traversal. 3") {
//
//    val l = label
//
//    val srcA = addVertex(Long.box(1), l)
//    val srcB = addVertex(Long.box(2), l)
//    val srcC = addVertex(Long.box(3), l)
//
//    val tgtA = addVertex(Long.box(101), l)
//    val tgtC = addVertex(Long.box(103), l)
//
//    srcA.addEdge(l.label, tgtA)
//    srcA.addEdge(l.label, tgtC)
//    tgtC.addEdge(l.label, srcB)
//    tgtA.addEdge(l.label, srcC)
//
//    val vertexIdParams = Seq(srcA.id)
//    val vertices = g.traversal().V(vertexIdParams: _*).out(l.label).out(l.label).toSeq
//    vertices.size should be(2)
//    vertices.foreach { v =>
//      val vertex = v.asInstanceOf[S2Vertex]
//      // TODO: we have too many id. this is ugly and confusing so fix me.
//      vertex.id.innerId == srcB.id.innerId || vertex.id.innerId == srcC.id.innerId should be(true)
//      logger.debug(s"[Vertex]: $v")
//    }
//  }
//  test("add vertex without params.") {
//    val vertex = g.addVertex().asInstanceOf[S2Vertex]
//    vertex.id.column.service.serviceName should be(g.DefaultService.serviceName)
//    vertex.id.column.columnName should be(g.DefaultColumn.columnName)
//  }
//  val s2Graph = graph.asInstanceOf[S2Graph]
//  val mnt = s2Graph.getManagement()
//  override val service = s2Graph.DefaultService
//
//  val personColumn = Management.createServiceColumn(service.serviceName, "person", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "0", "integer")))
//  val softwareColumn = Management.createServiceColumn(service.serviceName, "software", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("lang", "-", "string")))
//  //    val vertexColumn = Management.createServiceColumn(service.serviceName, "vertex", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "-1", "integer"), Prop("lang", "scala", "string")))
//
//  val created = mnt.createLabel("created", service.serviceName, "person", "integer", service.serviceName, "software", "integer",
//    true, service.serviceName, Nil, Seq(Prop("weight", "0.0", "float")), "strong", None, None)
//
//  val knows = mnt.createLabel("knows", service.serviceName, "person", "integer", service.serviceName, "person", "integer",
//    true, service.serviceName, Nil, Seq(Prop("weight", "0.0", "float")), "strong", None, None)
//

  test("tinkerpop class graph test.") {
//    val marko = graph.addVertex(T.label, "person", T.id, Int.box(1))
//    marko.property("name", "marko")
//    marko.property("age", Int.box(29))
//    val vadas = graph.addVertex(T.label, "person", T.id, Int.box(2))
//    vadas.property("name", "vadas", "age", Int.box(27))
//    val lop = graph.addVertex(T.label, "software", T.id, Int.box(3), "name", "lop", "lang", "java")
//    val josh = graph.addVertex(T.label, "person", T.id, Int.box(4), "name", "josh", "age", Int.box(32))
//    val ripple = graph.addVertex(T.label, "software", T.id, Int.box(5), "name", "ripple", "lang", "java")
//    val peter = graph.addVertex(T.label, "person", T.id, Int.box(6), "name", "peter", "age", Int.box(35))
//
//    marko.addEdge("knows", vadas, T.id, Int.box(7), "weight", Float.box(0.5f))
//    marko.addEdge("knows", josh, T.id, Int.box(8), "weight", Float.box(1.0f))
//    marko.addEdge("created", lop, T.id, Int.box(9), "weight", Float.box(0.4f))
//    josh.addEdge("created", ripple, T.id, Int.box(10), "weight", Float.box(1.0f))
//    josh.addEdge("created", lop, T.id, Int.box(11), "weight", Float.box(0.4f))
//    peter.addEdge("created", lop, T.id, Int.box(12), "weight", Float.box(0.2f))
//    graph.tx().commit()
//
//    graph.traversal().V().inV()
//    val verticees = s2Graph.traversal().V().asAdmin().toSeq
//
//
//    val vs = verticees.toList
//    val edgeId = graph.traversal().V().has("name", "marko").outE("knows").as("e").inV().has("name", "vadas").select[Edge]("e").toList.head.id()
//    val edges = graph.traversal().E(edgeId).toList()
////    .as("e").inV().has("name", "vadas").select[Edge]("e").asAdmin().toSeq
////    graph.traversal.V.has("name", outVertexName).outE(edgeLabel).as("e").inV.has("name", inVertexName).select[Edge]("e").next.id
//    logger.error(edgeId.toString)
//    val x = edges.mkString("\n")
//    logger.error(x)
  }

  test("addVertex with empty parameter") {

  }
}