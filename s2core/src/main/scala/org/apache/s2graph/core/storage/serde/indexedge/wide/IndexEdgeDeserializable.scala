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

import org.apache.s2graph.core.mysqls.{ServiceColumn, Label, LabelMeta}
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core._
import scala.collection.immutable

class IndexEdgeDeserializable(graph: S2Graph,
                              bytesToLongFunc: (Array[Byte], Int) => Long = bytesToLong) extends Deserializable[S2Edge] {
   import StorageDeserializable._

   type QualifierRaw = (Array[(LabelMeta, InnerValLike)], VertexId, Byte, Boolean, Int)
   type ValueRaw = (Array[(LabelMeta, InnerValLike)], Int)

   private def parseDegreeQualifier(kv: SKeyValue, schemaVer: String): QualifierRaw = {
     //    val degree = Bytes.toLong(kv.value)
     val degree = bytesToLongFunc(kv.value, 0)
     val idxPropsRaw = Array(LabelMeta.degree -> InnerVal.withLong(degree, schemaVer))
     val tgtVertexIdRaw = VertexId(ServiceColumn.Default, InnerVal.withStr("0", schemaVer))
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
                                                    cacheElementOpt: Option[S2Edge]): S2Edge = {
     assert(_kvs.size == 1)

//     val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

     val kv = implicitly[CanSKeyValue[T]].toSKeyValue(_kvs.head)
     val version = kv.timestamp

//     val (srcVertexId, labelWithDir, labelIdxSeq, _, _) = cacheElementOpt.map { e =>
//       (e.srcVertex.id, e.labelWithDir, e.labelIndexSeq, false, 0)
//     }.getOrElse(parseRow(kv, schemaVer))
     val (srcVertexId, labelWithDir, labelIdxSeq, _, _) = parseRow(kv, schemaVer)

     val label = checkLabel.getOrElse(Label.findById(labelWithDir.labelId))
     val srcVertex = graph.newVertex(srcVertexId, version)
     //TODO:
     val edge = graph.newEdge(srcVertex, null,
       label, labelWithDir.dir, GraphUtil.defaultOpByte, version, S2Edge.EmptyState)
     var tsVal = version

     val (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdInQualifier, _) =
       if (kv.qualifier.isEmpty) parseDegreeQualifier(kv, schemaVer)
       else parseQualifier(kv, schemaVer)

     val index = label.indicesMap.getOrElse(labelIdxSeq, throw new RuntimeException(s"invalid index seq: ${label.id.get}, ${labelIdxSeq}"))

     /** process indexProps */
     val size = idxPropsRaw.length
     (0 until size).foreach { ith =>
       val meta = index.sortKeyTypesArray(ith)
       val (k, v) = idxPropsRaw(ith)
       if (k == LabelMeta.timestamp) tsVal = v.value.asInstanceOf[BigDecimal].longValue()

       if (k == LabelMeta.degree) {
         edge.property(LabelMeta.degree.name, v.value, version)
       } else {
         edge.property(meta.name, v.value, version)
       }
     }

     /** process props */
     if (op == GraphUtil.operations("incrementCount")) {
       //        val countVal = Bytes.toLong(kv.value)
       val countVal = bytesToLongFunc(kv.value, 0)
       edge.property(LabelMeta.count.name, countVal, version)
     } else {
       val (props, endAt) = bytesToKeyValues(kv.value, 0, kv.value.length, schemaVer, label)
       props.foreach { case (k, v) =>
         if (k == LabelMeta.timestamp) tsVal = v.value.asInstanceOf[BigDecimal].longValue()

         edge.property(k.name, v.value, version)
       }
     }
     /** process tgtVertexId */
     val tgtVertexId =
       if (edge.checkProperty(LabelMeta.to.name)) {
         val vId = edge.property(LabelMeta.to.name).asInstanceOf[S2Property[_]].innerValWithTs
         TargetVertexId(ServiceColumn.Default, vId.innerVal)
       } else tgtVertexIdRaw

     edge.property(LabelMeta.timestamp.name, tsVal, version)
     edge.tgtVertex = graph.newVertex(tgtVertexId, version)
     edge.op = op
     edge.tsInnerValOpt = Option(InnerVal.withLong(tsVal, schemaVer))
     edge
   }
 }
