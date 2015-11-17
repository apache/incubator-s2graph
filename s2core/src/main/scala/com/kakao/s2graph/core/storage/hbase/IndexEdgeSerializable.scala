package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.{StorageSerializable, SKeyValue}
import com.kakao.s2graph.core.types.VertexId
import com.kakao.s2graph.core.utils.logger
import com.kakao.s2graph.core.{GraphUtil, IndexEdge}
import org.apache.hadoop.hbase.util.Bytes

case class IndexEdgeSerializable(indexEdge: IndexEdge) extends HSerializable[IndexEdge] {

  import StorageSerializable._

  val label = indexEdge.label
  val table = label.hbaseTableName.getBytes()
  val cf = HSerializable.edgeCf

  val idxPropsMap = indexEdge.orders.toMap
  val idxPropsBytes = propsToBytes(indexEdge.orders)

  /** version 1 and version 2 share same code for serialize row key part */
  override def toKeyValues: Seq[SKeyValue] = {
    val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
    val labelWithDirBytes = indexEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

    val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
//    logger.error(s"${row.toList}\n${srcIdBytes.toList}\n${labelWithDirBytes.toList}\n${labelIndexSeqWithIsInvertedBytes.toList}")
    val tgtIdBytes = VertexId.toTargetVertexId(indexEdge.tgtVertex.id).bytes
    val qualifier =
      if (indexEdge.op == GraphUtil.operations("incrementCount")) {
        Bytes.add(idxPropsBytes, tgtIdBytes, Array.fill(1)(indexEdge.op))
      } else {
        idxPropsMap.get(LabelMeta.toSeq) match {
          case None => Bytes.add(idxPropsBytes, tgtIdBytes)
          case Some(vId) => idxPropsBytes
        }
      }

    val value = propsToKeyValues(indexEdge.metas.toSeq)
    val kv = SKeyValue(table, row, cf, qualifier, value, indexEdge.version)

    Seq(kv)
  }
}
