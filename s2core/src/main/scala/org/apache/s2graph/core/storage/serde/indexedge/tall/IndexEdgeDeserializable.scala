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
import org.apache.s2graph.core.storage.{CanSKeyValue, Deserializable, StorageDeserializable}
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.types._

object IndexEdgeDeserializable {
  def getNewInstance(graph: S2Graph): IndexEdgeDeserializable =
    new IndexEdgeDeserializable(graph)
}

class IndexEdgeDeserializable(graph: S2Graph,
                              bytesToLongFunc: (Array[Byte], Int) => Long = bytesToLong)
    extends Deserializable[S2Edge] {

  import StorageDeserializable._

  type QualifierRaw =
    (Array[(LabelMeta, InnerValLike)], VertexId, Byte, Boolean, Int)
  type ValueRaw = (Array[(LabelMeta, InnerValLike)], Int)

  override def fromKeyValuesInner[T: CanSKeyValue](checkLabel: Option[Label],
                                                   _kvs: Seq[T],
                                                   schemaVer: String,
                                                   cacheElementOpt: Option[S2Edge]): S2Edge = {

    assert(_kvs.size == 1)

    //     val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
    val kv = implicitly[CanSKeyValue[T]].toSKeyValue(_kvs.head)
    //     logger.debug(s"[DES]: ${kv.toLogString}}")

    val version = kv.timestamp
    //    logger.debug(s"[Des]: ${kv.row.toList}, ${kv.qualifier.toList}, ${kv.value.toList}")
    var pos = 0
    val (srcVertexId, srcIdLen) =
      SourceVertexId.fromBytes(kv.row, pos, kv.row.length, schemaVer)
    pos += srcIdLen
    val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
    pos += 4
    val (labelIdxSeq, isInverted) =
      bytesToLabelIndexSeqWithIsInverted(kv.row, pos)
    pos += 1

    val label = checkLabel.getOrElse(Label.findById(labelWithDir.labelId))

    val srcVertex = graph.newVertex(srcVertexId, version)
    val edge = graph.newEdge(
      srcVertex,
      null,
      label,
      labelWithDir.dir,
      GraphUtil.defaultOpByte,
      version,
      S2Edge.EmptyState
    )
    var tsVal = version

    if (pos == kv.row.length) {
      // degree
      //      val degreeVal = Bytes.toLong(kv.value)
      val degreeVal = bytesToLongFunc(kv.value, 0)
      val tgtVertexId =
        VertexId(ServiceColumn.Default, InnerVal.withStr("0", schemaVer))

      edge.property(LabelMeta.timestamp.name, version, version)
      edge.property(LabelMeta.degree.name, degreeVal, version)
      edge.tgtVertex = graph.newVertex(tgtVertexId, version)
      edge.op = GraphUtil.defaultOpByte
      edge.tsInnerValOpt = Option(InnerVal.withLong(tsVal, schemaVer))
      edge
    } else {
      // not degree edge
      val (idxPropsRaw, endAt) = bytesToProps(kv.row, pos, schemaVer)
      pos = endAt

      val (tgtVertexIdRaw, tgtVertexIdLen) = if (endAt == kv.row.length - 1) {
        (HBaseType.defaultTgtVertexId, 0)
      } else {
        TargetVertexId.fromBytes(kv.row, endAt, kv.row.length - 1, schemaVer)
      }
      val op = kv.row(kv.row.length - 1)

      val index = label.indicesMap.getOrElse(
        labelIdxSeq,
        throw new RuntimeException(s"invalid index seq: ${label.id.get}, ${labelIdxSeq}")
      )

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
        val (props, endAt) =
          bytesToKeyValues(kv.value, 0, kv.value.length, schemaVer, label)
        props.foreach {
          case (k, v) =>
            if (k == LabelMeta.timestamp) tsVal = v.value.asInstanceOf[BigDecimal].longValue()

            edge.property(k.name, v.value, version)
        }
      }

      /** process tgtVertexId */
      val tgtVertexId =
        if (edge.checkProperty(LabelMeta.to.name)) {
          val vId = edge
            .property(LabelMeta.to.name)
            .asInstanceOf[S2Property[_]]
            .innerValWithTs
          TargetVertexId(ServiceColumn.Default, vId.innerVal)
        } else tgtVertexIdRaw

      edge.tgtVertex = graph.newVertex(tgtVertexId, version)
      edge.op = op
      edge.tsInnerValOpt = Option(InnerVal.withLong(tsVal, schemaVer))
      edge
    }
  }
}
