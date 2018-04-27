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
import org.apache.s2graph.core.{S2Edge, SnapshotEdge}
import org.apache.s2graph.core.schema.LabelIndex
import org.apache.s2graph.core.storage.serde.Serializable
import org.apache.s2graph.core.storage.serde.StorageSerializable._
import org.apache.s2graph.core.types.VertexId



/**
 * this class serialize
 * @param snapshotEdge
 */
class SnapshotEdgeSerializable(snapshotEdge: SnapshotEdge) extends Serializable[SnapshotEdge] {

  override def ts = snapshotEdge.version
  override def table = snapshotEdge.label.hbaseTableName.getBytes("UTF-8")

  def statusCodeWithOp(statusCode: Byte, op: Byte): Array[Byte] = {
    val byte = (((statusCode << 4) | op).toByte)
    Array.fill(1)(byte.toByte)
  }
  def valueBytes() = Bytes.add(statusCodeWithOp(snapshotEdge.statusCode, snapshotEdge.op), snapshotEdge.propsToKeyValuesWithTs)


  override def toRowKey: Array[Byte] = {
    val srcIdBytes = VertexId.toSourceVertexId(snapshotEdge.srcVertex.id).bytes
    val labelWithDirBytes = snapshotEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(LabelIndex.DefaultSeq, isInverted = true)

    Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
  }

  override def toQualifier: Array[Byte] =
    VertexId.toTargetVertexId(snapshotEdge.tgtVertex.id).bytes

  override def toValue: Array[Byte] =
    snapshotEdge.pendingEdgeOpt match {
      case None => valueBytes()
      case Some(pendingEdge) =>
        val opBytes = statusCodeWithOp(pendingEdge.getStatusCode(), pendingEdge.getOp())
        val versionBytes = Array.empty[Byte]
        val propsBytes = S2Edge.serializePropsWithTs(pendingEdge)
        val lockBytes = Bytes.toBytes(pendingEdge.getLockTs().get)
        Bytes.add(Bytes.add(valueBytes(), opBytes, versionBytes), Bytes.add(propsBytes, lockBytes))
    }

}
