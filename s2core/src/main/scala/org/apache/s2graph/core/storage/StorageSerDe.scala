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

import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.serde.Deserializable
import org.apache.s2graph.core.storage.serde.indexedge.tall.IndexEdgeDeserializable

trait StorageSerDe {
  /**
    * Compatibility table
    * | label schema version | snapshot edge | index edge | vertex | note |
    * | v1 | serde.snapshotedge.wide | serde.indexedge.wide | serde.vertex | do not use this. this exist only for backward compatibility issue |
    * | v2 | serde.snapshotedge.wide | serde.indexedge.wide | serde.vertex | do not use this. this exist only for backward compatibility issue |
    * | v3 | serde.snapshotedge.tall | serde.indexedge.wide | serde.vertex | recommended with HBase. current stable schema |
    * | v4 | serde.snapshotedge.tall | serde.indexedge.tall | serde.vertex | experimental schema. use scanner instead of get |
    *
    */

  /**
    * create serializer that knows how to convert given snapshotEdge into kvs: Seq[SKeyValue]
    * so we can store this kvs.
    *
    * @param snapshotEdge : snapshotEdge to serialize
    * @return serializer implementation for StorageSerializable which has toKeyValues return Seq[SKeyValue]
    */
  def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): serde.Serializable[SnapshotEdge]

  /**
    * create serializer that knows how to convert given indexEdge into kvs: Seq[SKeyValue]
    *
    * @param indexEdge : indexEdge to serialize
    * @return serializer implementation
    */
  def indexEdgeSerializer(indexEdge: IndexEdge): serde.Serializable[IndexEdge]

  /**
    * create serializer that knows how to convert given vertex into kvs: Seq[SKeyValue]
    *
    * @param vertex : vertex to serialize
    * @return serializer implementation
    */
  def vertexSerializer(vertex: S2VertexLike): serde.Serializable[S2VertexLike]

  /**
    * create deserializer that can parse stored CanSKeyValue into snapshotEdge.
    * note that each storage implementation should implement implicit type class
    * to convert storage dependent dataType into common SKeyValue type by implementing CanSKeyValue
    *
    * ex) Asynchbase use it's KeyValue class and CanSKeyValue object has implicit type conversion method.
    * if any storaage use different class to represent stored byte array,
    * then that storage implementation is responsible to provide implicit type conversion method on CanSKeyValue.
    **/
  def snapshotEdgeDeserializer(schemaVer: String): Deserializable[SnapshotEdge]

  def indexEdgeDeserializer(schemaVer: String): IndexEdgeDeserializable

  def vertexDeserializer(schemaVer: String): Deserializable[S2VertexLike]


}
