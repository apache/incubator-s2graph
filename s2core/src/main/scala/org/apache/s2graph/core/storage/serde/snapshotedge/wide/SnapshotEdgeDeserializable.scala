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

package org.apache.s2graph.core.storage.serde.snapshotedge.wide

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.mysqls.{LabelIndex, LabelMeta}
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.storage.{CanSKeyValue, Deserializable, StorageDeserializable}
import org.apache.s2graph.core.types.TargetVertexId
import org.apache.s2graph.core.{Edge, QueryParam, SnapshotEdge, Vertex}

class SnapshotEdgeDeserializable extends Deserializable[SnapshotEdge] {

  def statusCodeWithOp(byte: Byte): (Byte, Byte) = {
    val statusCode = byte >> 4
    val op = byte & ((1 << 4) - 1)
    (statusCode.toByte, op.toByte)
  }

  override def fromKeyValuesInner[T: CanSKeyValue](queryParam: QueryParam,
                                                   _kvs: Seq[T],
                                                   version: String,
                                                   cacheElementOpt: Option[SnapshotEdge]): SnapshotEdge = {
    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
    assert(kvs.size == 1)

    val kv = kvs.head
    val schemaVer = queryParam.label.schemaVersion
    val cellVersion = kv.timestamp

    val (srcVertexId, labelWithDir, _, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.id, e.labelWithDir, LabelIndex.DefaultSeq, true, 0)
    }.getOrElse(parseRow(kv, schemaVer))

    val (tgtVertexId, props, op, ts, statusCode, _pendingEdgeOpt) = {
      val (tgtVertexId, _) = TargetVertexId.fromBytes(kv.qualifier, 0, kv.qualifier.length, schemaVer)
      var pos = 0
      val (statusCode, op) = statusCodeWithOp(kv.value(pos))
      pos += 1
      val (props, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
      val kvsMap = props.toMap
      val ts = kvsMap(LabelMeta.timeStampSeq).innerVal.toString.toLong

      pos = endAt
      val _pendingEdgeOpt =
        if (pos == kv.value.length) None
        else {
          val (pendingEdgeStatusCode, pendingEdgeOp) = statusCodeWithOp(kv.value(pos))
          pos += 1
          //          val versionNum = Bytes.toLong(kv.value, pos, 8)
          //          pos += 8
          val (pendingEdgeProps, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
          pos = endAt
          val lockTs = Option(Bytes.toLong(kv.value, pos, 8))

          val pendingEdge =
            Edge(Vertex(srcVertexId, cellVersion),
              Vertex(tgtVertexId, cellVersion),
              labelWithDir, pendingEdgeOp,
              cellVersion, pendingEdgeProps.toMap,
              statusCode = pendingEdgeStatusCode, lockTs = lockTs)
          Option(pendingEdge)
        }

      (tgtVertexId, kvsMap, op, ts, statusCode, _pendingEdgeOpt)
    }

    SnapshotEdge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts),
      labelWithDir, op, cellVersion, props, statusCode = statusCode,
      pendingEdgeOpt = _pendingEdgeOpt, lockTs = None)
  }
}
