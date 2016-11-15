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

package org.apache.s2graph.core.storage.serde.indexedge.tall

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.storage.{CanSKeyValue, Deserializable, StorageDeserializable}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{GraphUtil, IndexEdge, Vertex}
import scala.collection.immutable

object IndexEdgeDeserializable{
  def getNewInstance() = new IndexEdgeDeserializable()
}
class IndexEdgeDeserializable(bytesToLongFunc: (Array[Byte], Int) => Long = bytesToLong) extends Deserializable[IndexEdge] {
   import StorageDeserializable._

   type QualifierRaw = (Array[(LabelMeta, InnerValLike)], VertexId, Byte, Boolean, Int)
   type ValueRaw = (Array[(LabelMeta, InnerValLike)], Int)

   override def fromKeyValuesInner[T: CanSKeyValue](checkLabel: Option[Label],
                                                    _kvs: Seq[T],
                                                    schemaVer: String,
                                                    cacheElementOpt: Option[IndexEdge]): IndexEdge = {

     assert(_kvs.size == 1)

     //     val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
     val kv = implicitly[CanSKeyValue[T]].toSKeyValue(_kvs.head)
//     logger.debug(s"[DES]: ${kv.toLogString}}")

     val version = kv.timestamp
     //    logger.debug(s"[Des]: ${kv.row.toList}, ${kv.qualifier.toList}, ${kv.value.toList}")
     var pos = 0
     val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(kv.row, pos, kv.row.length, schemaVer)
     pos += srcIdLen
     val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
     pos += 4
     val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(kv.row, pos)
     pos += 1

     val label = checkLabel.getOrElse(Label.findById(labelWithDir.labelId))
//     val op = kv.row(pos)
//     pos += 1

     if (pos == kv.row.length) {
       // degree
       //      val degreeVal = Bytes.toLong(kv.value)
       val degreeVal = bytesToLongFunc(kv.value, 0)
       val ts = kv.timestamp
       val tsInnerValLikeWithTs = InnerValLikeWithTs.withLong(ts, ts, schemaVer)
       val props = Map(LabelMeta.timestamp -> tsInnerValLikeWithTs,
         LabelMeta.degree -> InnerValLikeWithTs.withLong(degreeVal, ts, schemaVer))
       val tgtVertexId = VertexId(HBaseType.DEFAULT_COL_ID, InnerVal.withStr("0", schemaVer))
       IndexEdge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), label, labelWithDir.dir, GraphUtil.defaultOpByte, ts, labelIdxSeq, props,  tsInnerValOpt = Option(tsInnerValLikeWithTs.innerVal))
     } else {
       // not degree edge
       val (idxPropsRaw, endAt) = bytesToProps(kv.row, pos, schemaVer)
       pos = endAt


       val (tgtVertexIdRaw, tgtVertexIdLen) = if (endAt == kv.row.length - 1) {
         (HBaseType.defaultTgtVertexId, 0)
       } else {
         TargetVertexId.fromBytes(kv.row, endAt, kv.row.length - 1, schemaVer)
       }
       val op = kv.row(kv.row.length-1)

       val allProps = immutable.Map.newBuilder[LabelMeta, InnerValLikeWithTs]
       val index = label.indicesMap.getOrElse(labelIdxSeq, throw new RuntimeException(s"invalid index seq: ${label.id.get}, ${labelIdxSeq}"))

       /** process indexProps */
       val size = idxPropsRaw.length
       (0 until size).foreach { ith =>
         val meta = index.sortKeyTypesArray(ith)
         val (k, v) = idxPropsRaw(ith)
         if (k == LabelMeta.degree) allProps += LabelMeta.degree -> InnerValLikeWithTs(v, version)
         else allProps += meta -> InnerValLikeWithTs(v, version)
       }
//       for {
//         (meta, (k, v)) <- index.sortKeyTypes.zip(idxPropsRaw)
//       } {
//         if (k == LabelMeta.degree) allProps += LabelMeta.degree -> InnerValLikeWithTs(v, version)
//         else {
//           allProps += meta -> InnerValLikeWithTs(v, version)
//         }
//       }

       /** process props */
       if (op == GraphUtil.operations("incrementCount")) {
         //        val countVal = Bytes.toLong(kv.value)
         val countVal = bytesToLongFunc(kv.value, 0)
         allProps += (LabelMeta.count -> InnerValLikeWithTs.withLong(countVal, version, schemaVer))
       } else {
         val (props, endAt) = bytesToKeyValues(kv.value, 0, kv.value.length, schemaVer, label)
         props.foreach { case (k, v) =>
           allProps += (k -> InnerValLikeWithTs(v, version))
         }
       }
       val _mergedProps = allProps.result()
       val (mergedProps, tsInnerValLikeWithTs) = _mergedProps.get(LabelMeta.timestamp) match {
         case None =>
           val tsInnerVal = InnerValLikeWithTs.withLong(version, version, schemaVer)
           val mergedProps = _mergedProps + (LabelMeta.timestamp -> InnerValLikeWithTs.withLong(version, version, schemaVer))
           (mergedProps, tsInnerVal)
         case Some(tsInnerVal) =>
           (_mergedProps, tsInnerVal)
       }
//       val mergedProps =
//         if (_mergedProps.contains(LabelMeta.timestamp)) _mergedProps
//         else _mergedProps + (LabelMeta.timestamp -> InnerValLikeWithTs.withLong(version, version, schemaVer))

       /** process tgtVertexId */
       val tgtVertexId =
         mergedProps.get(LabelMeta.to) match {
           case None => tgtVertexIdRaw
           case Some(vId) => TargetVertexId(HBaseType.DEFAULT_COL_ID, vId.innerVal)
         }


       IndexEdge(Vertex(srcVertexId, version), Vertex(tgtVertexId, version), label, labelWithDir.dir, op, version, labelIdxSeq, mergedProps, tsInnerValOpt = Option(tsInnerValLikeWithTs.innerVal))

     }
   }
 }
