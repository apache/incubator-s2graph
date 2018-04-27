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

package org.apache.s2graph.core.storage.rocks

import org.apache.s2graph.core.TestCommonWithModels
import org.apache.s2graph.core.schema.{Service, ServiceColumn}
import org.apache.tinkerpop.gremlin.structure.T
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConversions._

class RocksStorageTest  extends FunSuite with Matchers with TestCommonWithModels {
  initTests()

//  test("VertexTest: shouldNotGetConcurrentModificationException()") {
//    val service = Service.findByName(serviceName, useCache = false).getOrElse {
//      throw new IllegalStateException("service not found.")
//    }
//    val column = ServiceColumn.find(service.id.get, columnName).getOrElse {
//      throw new IllegalStateException("column not found.")
//    }
//
//    val vertexId = graph.elementBuilder.newVertexId(service, column, 1L)
//
//    val vertex = graph.elementBuilder.newVertex(vertexId)
//    for (i <- (0 until 10)) {
//      vertex.addEdge(labelName, vertex)
//    }
//
//    println(graph.edges().toSeq)
//    println("*" * 100)
//    vertex.remove()
//    println(graph.vertices().toSeq)
//  }
}
