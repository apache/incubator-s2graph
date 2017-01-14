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
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, LabelMeta, ServiceColumn}
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.storage.{CanSKeyValue, Deserializable, StorageDeserializable}
import org.apache.s2graph.core.types._

object IndexEdgeDeserializable{
  def getNewInstance(graph: S2Graph) = new IndexEdgeDeserializable(graph)
}
class IndexEdgeDeserializable(graph: S2Graph,
                              bytesToLongFunc: (Array[Byte], Int) => Long = bytesToLong) extends Deserializable[S2Edge] {
   import StorageDeserializable._

   type QualifierRaw = (Array[(LabelMeta, InnerValLike)], VertexId, Byte, Boolean, Int)
   type ValueRaw = (Array[(LabelMeta, InnerValLike)], Int)

   override def fromKeyValues[T: CanSKeyValue](_kvs: Seq[T],
                                               cacheElementOpt: Option[S2Edge]): Option[S2Edge] = {

     try {
       assert(_kvs.size == 1)

       //     val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
       val kv = implicitly[CanSKeyValue[T]].toSKeyValue(_kvs.head)
       //     logger.debug(s"[DES]: ${kv.toLogString}}")

       val version = kv.timestamp

       var pos = 0
       val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(kv.row, pos, kv.row.length, HBaseType.DEFAULT_VERSION)
       pos += srcIdLen
       val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
       pos += 4
       val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(kv.row, pos)
       pos += 1

       if (isInverted) None
       else {
         val label = Label.findById(labelWithDir.labelId)
         val schemaVer = label.schemaVersion
         val srcVertex = graph.newVertex(srcVertexId, version)
         //TODO:
         val edge = graph.newEdge(srcVertex, null,
           label, labelWithDir.dir, GraphUtil.defaultOpByte, version, S2Edge.EmptyState)
         var tsVal = version
         val isTallSchema = label.schemaVersion == HBaseType.VERSION4
         val isDegree = if (isTallSchema) pos == kv.row.length else kv.qualifier.isEmpty

         if (isDegree) {
           // degree
           //      val degreeVal = Bytes.toLong(kv.value)
           val degreeVal = bytesToLongFunc(kv.value, 0)
           val tgtVertexId = VertexId(ServiceColumn.Default, InnerVal.withStr("0", schemaVer))

           edge.propertyInner(LabelMeta.timestamp.name, version, version)
           edge.propertyInner(LabelMeta.degree.name, degreeVal, version)
           edge.tgtVertex = graph.newVertex(tgtVertexId, version)
           edge.op = GraphUtil.defaultOpByte
           edge.tsInnerValOpt = Option(InnerVal.withLong(tsVal, schemaVer))
         } else {
           // not degree edge
           val (idxPropsRaw, endAt) =
             if (isTallSchema) bytesToProps(kv.row, pos, schemaVer)
             else {
               bytesToProps(kv.qualifier, 0, schemaVer)
             }
           pos = endAt

           val (tgtVertexIdRaw, tgtVertexIdLen) = if (isTallSchema) {
             if (endAt == kv.row.length - 1) {
               (HBaseType.defaultTgtVertexId, 0)
             } else {
               TargetVertexId.fromBytes(kv.row, endAt, kv.row.length - 1, schemaVer)
             }
           } else {
             if (endAt == kv.qualifier.length) {
               (HBaseType.defaultTgtVertexId, 0)
             } else {
               TargetVertexId.fromBytes(kv.qualifier, endAt, kv.qualifier.length, schemaVer)
             }
           }
           pos += tgtVertexIdLen

           val op =
             if (isTallSchema) kv.row(kv.row.length - 1)
             else {
               if (kv.qualifier.length == pos) GraphUtil.defaultOpByte
               else kv.qualifier(kv.qualifier.length - 1)
             }

           val index = label.indicesMap.getOrElse(labelIdxSeq, throw new RuntimeException(s"invalid index seq: ${label.id.get}, ${labelIdxSeq}"))
           /** process indexProps */
           val size = idxPropsRaw.length
           (0 until size).foreach { ith =>
             val meta = index.sortKeyTypesArray(ith)
             val (k, v) = idxPropsRaw(ith)
             if (k == LabelMeta.timestamp) tsVal = v.value.asInstanceOf[BigDecimal].longValue()

             if (k == LabelMeta.degree) {
               edge.propertyInner(LabelMeta.degree.name, v.value, version)
             } else {
               edge.propertyInner(meta.name, v.value, version)
             }
           }

           /** process props */
           if (op == GraphUtil.operations("incrementCount")) {
             //        val countVal = Bytes.toLong(kv.value)
             val countVal = bytesToLongFunc(kv.value, 0)
             edge.propertyInner(LabelMeta.count.name, countVal, version)
           } else {
             val (props, endAt) = bytesToKeyValues(kv.value, 0, kv.value.length, schemaVer, label)
             props.foreach { case (k, v) =>
               if (k == LabelMeta.timestamp) tsVal = v.value.asInstanceOf[BigDecimal].longValue()

               edge.propertyInner(k.name, v.value, version)
             }
           }

           /** process tgtVertexId */
           val tgtVertexId =
             if (edge.checkProperty(LabelMeta.to.name)) {
               val vId = edge.property(LabelMeta.to.name).asInstanceOf[S2Property[_]].innerValWithTs
               TargetVertexId(ServiceColumn.Default, vId.innerVal)
             } else tgtVertexIdRaw

           edge.propertyInner(LabelMeta.timestamp.name, tsVal, version)
           edge.tgtVertex = graph.newVertex(tgtVertexId, version)
           edge.op = op
           edge.tsInnerValOpt = Option(InnerVal.withLong(tsVal, schemaVer))
         }
         Option(edge)
       }
     } catch {
       case e: Exception =>
         None
     }
   }
 }
