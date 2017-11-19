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

import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.{StorageIO, StorageSerDe, serde}
import org.apache.s2graph.core.types.HBaseType

class RocksStorageSerDe(val graph: S2GraphLike) extends StorageSerDe {

  override def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge) =
    new serde.snapshotedge.tall.SnapshotEdgeSerializable(snapshotEdge)

  override def indexEdgeSerializer(indexEdge: IndexEdge) =
    new serde.indexedge.tall.IndexEdgeSerializable(indexEdge, RocksHelper.longToBytes)

  override def vertexSerializer(vertex: S2VertexLike) =
      new serde.vertex.tall.VertexSerializable(vertex, RocksHelper.intToBytes)


  private val snapshotEdgeDeserializer = new serde.snapshotedge.tall.SnapshotEdgeDeserializable(graph)
  override def snapshotEdgeDeserializer(schemaVer: String) = snapshotEdgeDeserializer

  private val indexEdgeDeserializable =
    new serde.indexedge.tall.IndexEdgeDeserializable(graph,
      RocksHelper.bytesToLong, tallSchemaVersions = HBaseType.ValidVersions.toSet)

  override def indexEdgeDeserializer(schemaVer: String) = indexEdgeDeserializable

  private val vertexDeserializer =
      new serde.vertex.tall.VertexDeserializable(graph)

  override def vertexDeserializer(schemaVer: String) = vertexDeserializer

}
