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

import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.{GraphUtil, IndexEdge, Vertex}
import scala.collection.immutable

class IndexEdgeDeserializable(bytesToLongFunc: (Array[Byte], Int) => Long = bytesToLong) extends Deserializable[IndexEdge] {
   import StorageDeserializable._

   type QualifierRaw = (Array[(LabelMeta, InnerValLike)], VertexId, Byte, Boolean, Int)
   type ValueRaw = (Array[(LabelMeta, InnerValLike)], Int)

   private def parseDegreeQualifier(kv: SKeyValue, schemaVer: String): QualifierRaw = {
     //    val degree = Bytes.toLong(kv.value)
     val degree = bytesToLongFunc(kv.value, 0)
     val idxPropsRaw = Array(LabelMeta.degree -> InnerVal.withLong(degree, schemaVer))
     val tgtVertexIdRaw = VertexId(HBaseType.DEFAULT_COL_ID, InnerVal.withStr("0", schemaVer))
     (idxPropsRaw, tgtVertexIdRaw, GraphUtil.operations("insert"), false, 0)
   }

   private def parseQualifier(kv: SKeyValue, schemaVer: String): QualifierRaw = {
     var qualifierLen = 0
     var pos = 0
     val (idxPropsRaw, idxPropsLen, tgtVertexIdRaw, tgtVertexIdLen) = {
       val (props, endAt) = bytesToProps(kv.qualifier, pos, schemaVer)
       pos = endAt
       qualifierLen += endAt
       val (tgtVertexId, tgtVertexIdLen) = if (endAt == kv.qualifier.length) {
         (HBaseType.defaultTgtVertexId, 0)
       } else {
         TargetVertexId.fromBytes(kv.qualifier, endAt, kv.qualifier.length, schemaVer)
       }
       qualifierLen += tgtVertexIdLen
       (props, endAt, tgtVertexId, tgtVertexIdLen)
     }
     val (op, opLen) =
       if (kv.qualifier.length == qualifierLen) (GraphUtil.defaultOpByte, 0)
       else (kv.qualifier(qualifierLen), 1)

     qualifierLen += opLen

     (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdLen != 0, qualifierLen)
   }

   override def fromKeyValuesInner[T: CanSKeyValue](checkLabel: Option[Label],
                                                    _kvs: Seq[T],
                                                    schemaVer: String,
                                                    cacheElementOpt: Option[IndexEdge]): IndexEdge = {
     assert(_kvs.size == 1)

//     val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

     val kv = implicitly[CanSKeyValue[T]].toSKeyValue(_kvs.head)
     val version = kv.timestamp

     val (srcVertexId, labelWithDir, labelIdxSeq, _, _) = cacheElementOpt.map { e =>
       (e.srcVertex.id, e.labelWithDir, e.labelIndexSeq, false, 0)
     }.getOrElse(parseRow(kv, schemaVer))

     val label = checkLabel.getOrElse(Label.findById(labelWithDir.labelId))

     val (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdInQualifier, _) =
       if (kv.qualifier.isEmpty) parseDegreeQualifier(kv, schemaVer)
       else parseQualifier(kv, schemaVer)

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
//     for {
//       (seq, (k, v)) <- index.sortKeyTypes.zip(idxPropsRaw)
//     } {
//       if (k == LabelMeta.degree) allProps += LabelMeta.degree -> InnerValLikeWithTs(v, version)
//       else allProps += seq -> InnerValLikeWithTs(v, version)
//     }

     /** process props */
     if (op == GraphUtil.operations("incrementCount")) {
       //      val countVal = Bytes.toLong(kv.value)
       val countVal = bytesToLongFunc(kv.value, 0)
       allProps += (LabelMeta.count -> InnerValLikeWithTs.withLong(countVal, version, schemaVer))
     } else if (kv.qualifier.isEmpty) {
       val countVal = bytesToLongFunc(kv.value, 0)
       allProps += (LabelMeta.degree -> InnerValLikeWithTs.withLong(countVal, version, schemaVer))
     } else {
       val (props, _) = bytesToKeyValues(kv.value, 0, kv.value.length, schemaVer, label)
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
//     val mergedProps =
//       if (_mergedProps.contains(LabelMeta.timestamp)) _mergedProps
//            else _mergedProps + (LabelMeta.timestamp -> InnerValLikeWithTs.withLong(version, version, schemaVer))

     /** process tgtVertexId */
     val tgtVertexId =
       mergedProps.get(LabelMeta.to) match {
         case None => tgtVertexIdRaw
         case Some(vId) => TargetVertexId(HBaseType.DEFAULT_COL_ID, vId.innerVal)
       }

     IndexEdge(Vertex(srcVertexId, version), Vertex(tgtVertexId, version), label, labelWithDir.dir, op, version, labelIdxSeq, mergedProps, tsInnerValOpt = Option(tsInnerValLikeWithTs.innerVal))

   }
 }
