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

import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.{GraphUtil, IndexEdge, QueryParam, Vertex}
import scala.collection.immutable

import scala.collection.immutable

import scala.collection.immutable

class IndexEdgeDeserializable(bytesToLongFunc: (Array[Byte], Int) => Long = bytesToLong) extends Deserializable[IndexEdge] {


  import StorageDeserializable._

  type QualifierRaw = (Array[(Byte, InnerValLike)], VertexId, Byte, Boolean, Int)
  type ValueRaw = (Array[(Byte, InnerValLike)], Int)

  private def parseDegreeQualifier(kv: SKeyValue, version: String): QualifierRaw = {
    //    val degree = Bytes.toLong(kv.value)
    val degree = bytesToLongFunc(kv.value, 0)
    val idxPropsRaw = Array(LabelMeta.degreeSeq -> InnerVal.withLong(degree, version))
    val tgtVertexIdRaw = VertexId(HBaseType.DEFAULT_COL_ID, InnerVal.withStr("0", version))
    (idxPropsRaw, tgtVertexIdRaw, GraphUtil.operations("insert"), false, 0)
  }

  private def parseQualifier(kv: SKeyValue, version: String): QualifierRaw = {
    var qualifierLen = 0
    var pos = 0
    val (idxPropsRaw, idxPropsLen, tgtVertexIdRaw, tgtVertexIdLen) = {
      val (props, endAt) = bytesToProps(kv.qualifier, pos, version)
      pos = endAt
      qualifierLen += endAt
      val (tgtVertexId, tgtVertexIdLen) = if (endAt == kv.qualifier.length) {
        (HBaseType.defaultTgtVertexId, 0)
      } else {
        TargetVertexId.fromBytes(kv.qualifier, endAt, kv.qualifier.length, version)
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

  private def parseValue(kv: SKeyValue, version: String): ValueRaw = {
    val (props, endAt) = bytesToKeyValues(kv.value, 0, kv.value.length, version)
    (props, endAt)
  }

  private def parseDegreeValue(kv: SKeyValue, version: String): ValueRaw = {
    (Array.empty[(Byte, InnerValLike)], 0)
  }

  override def fromKeyValuesInner[T: CanSKeyValue](queryParam: QueryParam,
                                                   _kvs: Seq[T],
                                                   schemaVer: String,
                                                   cacheElementOpt: Option[IndexEdge]): IndexEdge = {
    assert(_kvs.size == 1)

    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

    val kv = kvs.head
    val version = kv.timestamp

    val (srcVertexId, labelWithDir, labelIdxSeq, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.id, e.labelWithDir, e.labelIndexSeq, false, 0)
    }.getOrElse(parseRow(kv, schemaVer))

    val (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdInQualifier, _) =
      if (kv.qualifier.isEmpty) parseDegreeQualifier(kv, schemaVer)
      else parseQualifier(kv, schemaVer)

    val allProps = immutable.Map.newBuilder[Byte, InnerValLikeWithTs]
    val index = queryParam.label.indicesMap.getOrElse(labelIdxSeq, throw new RuntimeException(s"invalid index seq: ${queryParam.label.id.get}, ${labelIdxSeq}"))

    /** process indexProps */
    for {
      (seq, (k, v)) <- index.metaSeqs.zip(idxPropsRaw)
    } {
      if (k == LabelMeta.degreeSeq) allProps += k -> InnerValLikeWithTs(v, version)
      else allProps += seq -> InnerValLikeWithTs(v, version)
    }

    /** process props */
    if (op == GraphUtil.operations("incrementCount")) {
      //      val countVal = Bytes.toLong(kv.value)
      val countVal = bytesToLongFunc(kv.value, 0)
      allProps += (LabelMeta.countSeq -> InnerValLikeWithTs.withLong(countVal, version, schemaVer))
    } else if (kv.qualifier.isEmpty) {
      val countVal = bytesToLongFunc(kv.value, 0)
      allProps += (LabelMeta.degreeSeq -> InnerValLikeWithTs.withLong(countVal, version, schemaVer))
    } else {
      val (props, _) = bytesToKeyValues(kv.value, 0, kv.value.length, schemaVer)
      props.foreach { case (k, v) => allProps += (k -> InnerValLikeWithTs(v, version)) }
    }

    val _mergedProps = allProps.result()
    val mergedProps =
      if (_mergedProps.contains(LabelMeta.timeStampSeq)) _mergedProps
      else _mergedProps + (LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(version, version, schemaVer))

    /** process tgtVertexId */
    val tgtVertexId =
      mergedProps.get(LabelMeta.toSeq) match {
        case None => tgtVertexIdRaw
        case Some(vId) => TargetVertexId(HBaseType.DEFAULT_COL_ID, vId.innerVal)
      }

    IndexEdge(Vertex(srcVertexId, version), Vertex(tgtVertexId, version), labelWithDir, op, version, labelIdxSeq, mergedProps)

  }
}
