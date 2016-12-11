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

package org.apache.s2graph.core.storage.serde.vertex

import org.apache.s2graph.core.mysqls.{ColumnMeta, Label}
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.storage.{CanSKeyValue, Deserializable}
import org.apache.s2graph.core.types.{HBaseType, InnerVal, InnerValLike, VertexId}
import org.apache.s2graph.core.{S2Graph, QueryParam, S2Vertex}

import scala.collection.mutable.ListBuffer

class VertexDeserializable(graph: S2Graph,
                           bytesToInt: (Array[Byte], Int) => Int = bytesToInt) extends Deserializable[S2Vertex] {
  def fromKeyValues[T: CanSKeyValue](_kvs: Seq[T],
                                          cacheElementOpt: Option[S2Vertex]): Option[S2Vertex] = {
    try {
      val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

      val kv = kvs.head
      val version = HBaseType.DEFAULT_VERSION
      val (vertexId, _) = VertexId.fromBytes(kv.row, 0, kv.row.length, version)

      var maxTs = Long.MinValue
      val propsMap = new collection.mutable.HashMap[ColumnMeta, InnerValLike]
      val belongLabelIds = new ListBuffer[Int]

      for {
        kv <- kvs
      } {
        val propKey =
          if (kv.qualifier.length == 1) kv.qualifier.head.toInt
          else bytesToInt(kv.qualifier, 0)

        val ts = kv.timestamp
        if (ts > maxTs) maxTs = ts

        if (S2Vertex.isLabelId(propKey)) {
          belongLabelIds += S2Vertex.toLabelId(propKey)
        } else {
          val v = kv.value
          val (value, _) = InnerVal.fromBytes(v, 0, v.length, version)
          val columnMeta = vertexId.column.metasMap(propKey)
          propsMap += (columnMeta -> value)
        }
      }
      assert(maxTs != Long.MinValue)
      val vertex = graph.newVertex(vertexId, maxTs, S2Vertex.EmptyProps, belongLabelIds = belongLabelIds)
      S2Vertex.fillPropsWithTs(vertex, propsMap.toMap)
      Option(vertex)
    } catch {
      case e: Exception => None
    }
  }
}
