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

package com.kakao.s2graph.core.storage.serde.indexedge.tall

import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.{SKeyValue, Serializable, StorageSerializable}
import com.kakao.s2graph.core.types.VertexId
import com.kakao.s2graph.core.{GraphUtil, IndexEdge}
import org.apache.hadoop.hbase.util.Bytes


class IndexEdgeSerializable(indexEdge: IndexEdge) extends Serializable[IndexEdge] {
   import StorageSerializable._

   val label = indexEdge.label
   val table = label.hbaseTableName.getBytes()
   val cf = Serializable.edgeCf

   val idxPropsMap = indexEdge.orders.toMap
   val idxPropsBytes = propsToBytes(indexEdge.orders)

   override def toKeyValues: Seq[SKeyValue] = {
     val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
     val labelWithDirBytes = indexEdge.labelWithDir.bytes
     val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

     val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
     //    logger.error(s"${row.toList}\n${srcIdBytes.toList}\n${labelWithDirBytes.toList}\n${labelIndexSeqWithIsInvertedBytes.toList}")

     val qualifier =
       if (indexEdge.degreeEdge) Array.empty[Byte]
       else
         idxPropsMap.get(LabelMeta.toSeq) match {
           case None => Bytes.add(idxPropsBytes, VertexId.toTargetVertexId(indexEdge.tgtVertex.id).bytes)
           case Some(vId) => idxPropsBytes
         }

     /** TODO search usage of op byte. if there is no, then remove opByte */
     val rowBytes = Bytes.add(row, Array.fill(1)(GraphUtil.defaultOpByte), qualifier)
     //    val qualifierBytes = Array.fill(1)(indexEdge.op)
     val qualifierBytes = Array.empty[Byte]

     val value =
       if (indexEdge.degreeEdge)
         Bytes.toBytes(indexEdge.propsWithTs(LabelMeta.degreeSeq).innerVal.toString().toLong)
       else if (indexEdge.op == GraphUtil.operations("incrementCount"))
         Bytes.toBytes(indexEdge.propsWithTs(LabelMeta.countSeq).innerVal.toString().toLong)
       else propsToKeyValues(indexEdge.metas.toSeq)

     val kv = SKeyValue(table, rowBytes, cf, qualifierBytes, value, indexEdge.version)

     //        logger.debug(s"[Ser]: ${kv.row.toList}, ${kv.qualifier.toList}, ${kv.value.toList}")
     Seq(kv)
   }
 }
