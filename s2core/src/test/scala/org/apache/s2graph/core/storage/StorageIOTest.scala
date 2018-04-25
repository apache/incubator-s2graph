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

package org.apache.s2graph.core.storage

import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorageSerDe
import org.apache.s2graph.core.storage.serde.{StorageDeserializable, StorageSerializable}
import org.apache.s2graph.core.{S2Vertex, S2VertexLike, TestCommonWithModels}
import org.scalatest.{FunSuite, Matchers}

class StorageIOTest extends FunSuite with Matchers with TestCommonWithModels {

  initTests()

  test("AsynchbaseStorageIO: VertexSerializer/Deserializer") {
    def check(vertex: S2VertexLike,
              op: S2VertexLike => StorageSerializable[S2VertexLike],
              deserializer: StorageDeserializable[S2VertexLike]): Boolean = {
      val sKeyValues = op(vertex).toKeyValues
      val deserialized = deserializer.fromKeyValues(sKeyValues, None)
      vertex == deserialized
    }

    val serDe: StorageSerDe = new AsynchbaseStorageSerDe(graph)
    val service = Service.findByName(serviceName, useCache = false).getOrElse {
      throw new IllegalStateException("service not found.")
    }
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse {
      throw new IllegalStateException("column not found.")
    }

    val vertexId = builder.newVertexId(service, column, 1L)
    val vertex = builder.newVertex(vertexId)

    check(vertex, serDe.vertexSerializer, serDe.vertexDeserializer(vertex.serviceColumn.schemaVersion))
  }

}
