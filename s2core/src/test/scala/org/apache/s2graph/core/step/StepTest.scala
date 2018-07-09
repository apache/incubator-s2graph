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

package org.apache.s2graph.core.step

import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}
import rx.lang.scala.{Observable, Subscription}

class StepTest extends FunSuite with Matchers {

  trait GraphE {
    def id: String
  }

  case class V(id: String) extends GraphE

  case class E(id: String, src: V, tgt: V) extends GraphE

  object GraphModels {
    /**
      * vertices: [A, B]
      * edges: [E(A, B), E(B, A)]
      */
    val va = V("V_A")
    val vb = V("V_B")

    val e1 = E("E1", va, vb)
    val e2 = E("E2", vb, va)

    val allVertices = List(va, vb)
    val allEdges = List(e1, e2)
  }

  case class VertexStep(vid: String) extends RxStep[Unit, V] {
    override def apply(in: Unit): Observable[V] = {
      val vertices = GraphModels.allVertices.filter(v => vid == v.id)
      Observable.from(vertices)
    }
  }

  case class EdgeStep(dir: String) extends RxStep[V, E] {
    override def apply(in: V): Observable[E] = {
      val edges = if (dir == "OUT") {
        GraphModels.allEdges.filter(e => in == e.src)
      } else {
        GraphModels.allEdges.filter(e => in == e.tgt)
      }

      Observable.from(edges)
    }
  }

  case class EdgeToVertexStep() extends RxStep[E, V] {
    override def apply(in: E): Observable[V] = {
      Observable.just(in.tgt)
    }
  }

  test("basic step") {
    val v1: RxStep[Unit, V] = VertexStep("V_A")

    val e1: RxStep[V, E] = EdgeStep("OUT")
    val e2 = EdgeStep("IN")

    val g = v1(())
      .flatMap(v => e1(v) ++ e2(v))
      .flatMap(EdgeToVertexStep())
      .flatMap(v => e1(v) ++ e2(v))
      .distinct

    val expected = List(
      E("E1", V("V_A"), V("V_B")),
      E("E2", V("V_B"), V("V_A"))
    ).sortBy(_.id)

    val actual = g.toBlocking.toList.sortBy(_.id)

    println(actual)
    actual shouldBe expected
  }
}
