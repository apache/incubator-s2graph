package com.kakao.s2graph.core.storage.hbase

import java.util.UUID

import com.kakao.s2graph.core.SnapshotEdge
import com.kakao.s2graph.core.mysqls.LabelIndex
import com.kakao.s2graph.core.storage.{StorageSerializable, SKeyValue}
import com.kakao.s2graph.core.types.{HBaseType, SourceAndTargetVertexIdPair, VertexId}
import org.apache.hadoop.hbase.util.Bytes

import scala.util.Random

class SnapshotEdgeSerializable(snapshotEdge: SnapshotEdge) extends HSerializable[SnapshotEdge] {
  import StorageSerializable._

  val label = snapshotEdge.label
  val table = label.hbaseTableName.getBytes()
  val cf = HSerializable.edgeCf

  def valueBytes() = Bytes.add(Array.fill(1)(snapshotEdge.op), propsToKeyValuesWithTs(snapshotEdge.props.toList))

  override def toKeyValues: Seq[SKeyValue] = {
    label.schemaVersion match {
      case HBaseType.VERSION3 => toKeyValuesInnerV3
      case _ => toKeyValuesInner
    }
  }

  private def toKeyValuesInner: Seq[SKeyValue] = {
    val srcIdBytes = VertexId.toSourceVertexId(snapshotEdge.srcVertex.id).bytes
    val labelWithDirBytes = snapshotEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(LabelIndex.DefaultSeq, isInverted = true)

    val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
    val tgtIdBytes = VertexId.toTargetVertexId(snapshotEdge.tgtVertex.id).bytes

    val qualifier = tgtIdBytes

    val value = Bytes.add(valueBytes(), snapshotEdge.lockTsOpt.map(Bytes.toBytes(_)).getOrElse(Array.empty[Byte]))

    val kv = SKeyValue(table, row, cf, qualifier, value, snapshotEdge.version)
    Seq(kv)
  }

  private def toKeyValuesInnerV3: Seq[SKeyValue] = {
    val srcIdAndTgtIdBytes = SourceAndTargetVertexIdPair(snapshotEdge.srcVertex.innerId, snapshotEdge.tgtVertex.innerId).bytes
    val labelWithDirBytes = snapshotEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(LabelIndex.DefaultSeq, isInverted = true)

    val row = Bytes.add(srcIdAndTgtIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)

    val qualifier = Array.empty[Byte]

    val value = Bytes.add(valueBytes(), snapshotEdge.lockTsOpt.map(Bytes.toBytes(_)).getOrElse(Array.empty[Byte]))

    val kv = SKeyValue(table, row, cf, qualifier, value, snapshotEdge.version)
    Seq(kv)
  }
}
