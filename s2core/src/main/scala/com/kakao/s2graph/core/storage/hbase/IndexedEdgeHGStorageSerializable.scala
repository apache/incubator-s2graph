package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.types.VertexId
import com.kakao.s2graph.core.{GraphUtil, Graph, JSONParser, EdgeWithIndex}
import com.kakao.s2graph.core.storage.{GKeyValue, GraphSerializable}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 10/29/15.
 */
case class IndexedEdgeHGStorageSerializable(indexedEdge: EdgeWithIndex) extends HGStorageSerializable[EdgeWithIndex] with JSONParser with GraphSerializable {

  val label = indexedEdge.label
  val table = label.hbaseTableName.getBytes()
  val cf = Graph.edgeCf
  val idxPropsMap = indexedEdge.orders.toMap
  val idxPropsBytes = propsToBytes(indexedEdge.orders)

  /** version 1 and version 2 share same code for serialize row key part */
  override def toKeyValues: Seq[GKeyValue] = {
    val srcIdBytes = VertexId.toSourceVertexId(indexedEdge.srcVertex.id).bytes
    val labelWithDirBytes = indexedEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(indexedEdge.labelIndexSeq, isInverted = false)
    val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)

    val tgtIdBytes = VertexId.toTargetVertexId(indexedEdge.tgtVertex.id).bytes
    val qualifier =
      if (indexedEdge.op == GraphUtil.operations("incrementCount")) {
        Bytes.add(idxPropsBytes, tgtIdBytes, Array.fill(1)(indexedEdge.op))
      } else {
        idxPropsMap.get(LabelMeta.toSeq) match {
          case None => Bytes.add(idxPropsBytes, tgtIdBytes)
          case Some(vId) => idxPropsBytes
        }
      }

    val value = propsToKeyValues(indexedEdge.metas.toSeq)
    val kv = HGKeyValue(table, row, cf, qualifier, value, indexedEdge.ts)
    Seq(kv)
  }


}
