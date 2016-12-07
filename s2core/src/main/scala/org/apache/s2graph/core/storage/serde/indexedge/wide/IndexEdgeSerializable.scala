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

package org.apache.s2graph.core.storage.serde.indexedge.wide

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.storage.{SKeyValue, Serializable, StorageSerializable}
import org.apache.s2graph.core.types.{InnerValLike, VertexId}
import org.apache.s2graph.core.{GraphUtil, IndexEdge}
import org.apache.s2graph.core.storage.StorageSerializable._

class IndexEdgeSerializable(indexEdge: IndexEdge, longToBytes: Long => Array[Byte] = longToBytes)
    extends Serializable[IndexEdge] {
  import StorageSerializable._

  override def ts: Long = indexEdge.version
  override def table: Array[Byte] = indexEdge.label.hbaseTableName.getBytes()

  def idxPropsMap: Map[LabelMeta, InnerValLike] = indexEdge.orders.toMap
  def idxPropsBytes: Array[Byte] = propsToBytes(indexEdge.orders)

  override def toRowKey: Array[Byte] = {
    val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
    val labelWithDirBytes = indexEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes =
      labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

    Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
  }

  override def toQualifier: Array[Byte] = {
    val tgtIdBytes = VertexId.toTargetVertexId(indexEdge.tgtVertex.id).bytes
    if (indexEdge.degreeEdge) Array.empty[Byte]
    else {
      if (indexEdge.op == GraphUtil.operations("incrementCount")) {
        Bytes.add(idxPropsBytes, tgtIdBytes, Array.fill(1)(indexEdge.op))
      } else {
        idxPropsMap.get(LabelMeta.to) match {
          case None => Bytes.add(idxPropsBytes, tgtIdBytes)
          case Some(vId) => idxPropsBytes
        }
      }
    }
  }

  override def toValue: Array[Byte] =
    if (indexEdge.degreeEdge) {
      longToBytes(indexEdge.property(LabelMeta.degree).innerVal.toString().toLong)
    } else if (indexEdge.op == GraphUtil.operations("incrementCount")) {
      longToBytes(indexEdge.property(LabelMeta.count).innerVal.toString().toLong)
    } else {
      propsToKeyValues(indexEdge.metas.toSeq)
    }

}
