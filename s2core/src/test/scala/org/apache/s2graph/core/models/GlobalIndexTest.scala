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

package org.apache.s2graph.core.models

import org.apache.s2graph.core.TestCommonWithModels
import org.apache.s2graph.core.mysqls.GlobalIndex
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import scala.collection.JavaConversions._

class GlobalIndexTest extends FunSuite with Matchers with TestCommonWithModels with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    initTests()
  }

  override def afterAll(): Unit = {
    graph.shutdown()
  }

  test("test buildGlobalIndex.") {
    Seq(GlobalIndex.EdgeType, GlobalIndex.VertexType).foreach { elementType =>
      management.buildGlobalIndex(elementType, "test_global", Seq("weight", "date", "name"))
    }
  }

  test("findGlobalIndex.") {
    // (weight: 34) AND (weight: [1 TO 100])
//    Seq(GlobalIndex.EdgeType, GlobalIndex.VertexType).foreach { elementType =>
//      val idx1 = management.buildGlobalIndex(elementType, "test-1", Seq("weight", "age", "name"))
//      val idx2 = management.buildGlobalIndex(elementType, "test-2", Seq("gender", "age"))
//      val idx3 = management.buildGlobalIndex(elementType, "test-3", Seq("class"))
//
//      var hasContainers = Seq(
//        new HasContainer("weight", P.eq(Int.box(34))),
//        new HasContainer("age", P.between(Int.box(1), Int.box(100)))
//      )
//
//      GlobalIndex.findGlobalIndex(elementType, hasContainers) shouldBe(Option(idx1))
//
//      hasContainers = Seq(
//        new HasContainer("gender", P.eq(Int.box(34))),
//        new HasContainer("age", P.eq(Int.box(34))),
//        new HasContainer("class", P.eq(Int.box(34)))
//      )
//
//      GlobalIndex.findGlobalIndex(elementType, hasContainers) shouldBe(Option(idx2))
//
//      hasContainers = Seq(
//        new HasContainer("class", P.eq(Int.box(34))),
//        new HasContainer("_", P.eq(Int.box(34)))
//      )
//
//      GlobalIndex.findGlobalIndex(elementType, hasContainers) shouldBe(Option(idx3))
//
//      hasContainers = Seq(
//        new HasContainer("key", P.eq(Int.box(34))),
//        new HasContainer("value", P.eq(Int.box(34)))
//      )
//
//      GlobalIndex.findGlobalIndex(elementType, hasContainers) shouldBe(None)
//    }
  }
}
