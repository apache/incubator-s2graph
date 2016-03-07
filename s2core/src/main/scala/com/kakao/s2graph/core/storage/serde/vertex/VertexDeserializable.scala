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

package com.kakao.s2graph.core.storage.serde.vertex

import com.kakao.s2graph.core.storage.{CanSKeyValue, Deserializable}
import com.kakao.s2graph.core.types.{InnerVal, InnerValLike, VertexId}
import com.kakao.s2graph.core.{QueryParam, Vertex}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer

class VertexDeserializable extends Deserializable[Vertex] {
  def fromKeyValuesInner[T: CanSKeyValue](queryParam: QueryParam,
                                     _kvs: Seq[T],
                                     version: String,
                                     cacheElementOpt: Option[Vertex]): Vertex = {

    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

    val kv = kvs.head
    val (vertexId, _) = VertexId.fromBytes(kv.row, 0, kv.row.length, version)

    var maxTs = Long.MinValue
    val propsMap = new collection.mutable.HashMap[Int, InnerValLike]
    val belongLabelIds = new ListBuffer[Int]

    for {
      kv <- kvs
    } {
      val propKey =
        if (kv.qualifier.length == 1) kv.qualifier.head.toInt
        else Bytes.toInt(kv.qualifier)

      val ts = kv.timestamp
      if (ts > maxTs) maxTs = ts

      if (Vertex.isLabelId(propKey)) {
        belongLabelIds += Vertex.toLabelId(propKey)
      } else {
        val v = kv.value
        val (value, _) = InnerVal.fromBytes(v, 0, v.length, version)
        propsMap += (propKey -> value)
      }
    }
    assert(maxTs != Long.MinValue)
    Vertex(vertexId, maxTs, propsMap.toMap, belongLabelIds = belongLabelIds)
  }
}

