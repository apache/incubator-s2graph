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

package org.apache.s2graph.core.storage.serde.snapshotedge.tall

import org.apache.hadoop.hbase.util.Bytes

import org.apache.s2graph.core.{S2Graph, SnapshotEdge}
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, LabelMeta, ServiceColumn}
import org.apache.s2graph.core.storage.{CanSKeyValue, Deserializable, SKeyValue}
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.types.{
  LabelWithDirection,
  SourceAndTargetVertexIdPair,
  SourceVertexId
}

class SnapshotEdgeDeserializable(graph: S2Graph) extends Deserializable[SnapshotEdge] {

  def statusCodeWithOp(byte: Byte): (Byte, Byte) = {
    val statusCode = byte >> 4
    val op = byte & ((1 << 4) - 1)
    (statusCode.toByte, op.toByte)
  }

  override def fromKeyValuesInner[T: CanSKeyValue](
      checkLabel: Option[Label],
      _kvs: Seq[T],
      version: String,
      cacheElementOpt: Option[SnapshotEdge]
  ): SnapshotEdge = {
    val kvs = _kvs.map { kv =>
      implicitly[CanSKeyValue[T]].toSKeyValue(kv)
    }
    assert(kvs.size == 1)

    val kv = kvs.head
    val label = checkLabel.get
    val schemaVer = label.schemaVersion
    val cellVersion = kv.timestamp

    /** rowKey */
    def parseRowV3(kv: SKeyValue, version: String) = {
      var pos = 0
      val (srcIdAndTgtId, srcIdAndTgtIdLen) = SourceAndTargetVertexIdPair
        .fromBytes(kv.row, pos, kv.row.length, version)
      pos += srcIdAndTgtIdLen
      val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
      pos += 4
      val (labelIdxSeq, isInverted) =
        bytesToLabelIndexSeqWithIsInverted(kv.row, pos)

      val rowLen = srcIdAndTgtIdLen + 4 + 1
      (srcIdAndTgtId.srcInnerId,
       srcIdAndTgtId.tgtInnerId,
       labelWithDir,
       labelIdxSeq,
       isInverted,
       rowLen)

    }

    val (srcInnerId, tgtInnerId, labelWithDir, _, _, _) = cacheElementOpt
      .map { e =>
        (e.srcVertex.innerId, e.tgtVertex.innerId, e.labelWithDir, LabelIndex.DefaultSeq, true, 0)
      }
      .getOrElse(parseRowV3(kv, schemaVer))

    val srcVertexId = SourceVertexId(ServiceColumn.Default, srcInnerId)
    val tgtVertexId = SourceVertexId(ServiceColumn.Default, tgtInnerId)

    val (props, op, ts, statusCode, _pendingEdgeOpt, tsInnerVal) = {
      var pos = 0
      val (statusCode, op) = statusCodeWithOp(kv.value(pos))
      pos += 1
      val (props, endAt) =
        bytesToKeyValuesWithTs(kv.value, pos, schemaVer, label)
      val kvsMap = props.toMap
      val tsInnerVal = kvsMap(LabelMeta.timestamp).innerVal
      val ts = tsInnerVal.toString.toLong

      pos = endAt
      val _pendingEdgeOpt =
        if (pos == kv.value.length) None
        else {
          val (pendingEdgeStatusCode, pendingEdgeOp) = statusCodeWithOp(kv.value(pos))
          pos += 1
          //          val versionNum = Bytes.toLong(kv.value, pos, 8)
          //          pos += 8
          val (pendingEdgeProps, endAt) =
            bytesToKeyValuesWithTs(kv.value, pos, schemaVer, label)
          pos = endAt
          val lockTs = Option(Bytes.toLong(kv.value, pos, 8))

          val pendingEdge =
            graph.newEdge(
              graph.newVertex(srcVertexId, cellVersion),
              graph.newVertex(tgtVertexId, cellVersion),
              label,
              labelWithDir.dir,
              pendingEdgeOp,
              cellVersion,
              pendingEdgeProps.toMap,
              statusCode = pendingEdgeStatusCode,
              lockTs = lockTs,
              tsInnerValOpt = Option(tsInnerVal)
            )
          Option(pendingEdge)
        }

      (kvsMap, op, ts, statusCode, _pendingEdgeOpt, tsInnerVal)
    }

    graph.newSnapshotEdge(
      graph.newVertex(srcVertexId, ts),
      graph.newVertex(tgtVertexId, ts),
      label,
      labelWithDir.dir,
      op,
      cellVersion,
      props,
      statusCode = statusCode,
      pendingEdgeOpt = _pendingEdgeOpt,
      lockTs = None,
      tsInnerValOpt = Option(tsInnerVal)
    )
  }
}
