package org.apache.s2graph.core.storage.redis

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.mysqls.LabelIndex
import org.apache.s2graph.core.types.v2.InnerVal
import org.apache.s2graph.core.{GraphUtil, GraphExceptions, SnapshotEdge}
import org.apache.s2graph.core.storage.{SKeyValue, Serializable, StorageSerializable}
import org.apache.s2graph.core.types.{SourceAndTargetVertexIdPair, GraphType}

/**
 * @author Junki Kim (wishoping@gmail.com), Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Feb/19.
 */
class RedisSnapshotEdgeSerializable(snapshotEdge: SnapshotEdge) extends Serializable[SnapshotEdge] {
  import StorageSerializable._

  val label = snapshotEdge.label

  override def toKeyValues: Seq[SKeyValue] = {
    label.schemaVersion match {
      case GraphType.VERSION4 => toKeyValuesInnerV4
      case _ => throw new GraphExceptions.UnsupportedVersionException(s">> Redis storage can only support v4: current schema version ${label.schemaVersion}")
    }
  }

  def statusCodeWithOp(statusCode: Byte, op: Byte): Array[Byte] = {
    val byte = (((statusCode << 4) | op).toByte)
    Array.fill(1)(byte.toByte)
  }

  def valueBytes() = Bytes.add(statusCodeWithOp(snapshotEdge.statusCode, snapshotEdge.op),
    propsToKeyValuesWithTs(snapshotEdge.props.toList))

  private def toKeyValuesInnerV4: Seq[SKeyValue]  = {
    val srcIdAndTgtIdBytes = SourceAndTargetVertexIdPair(snapshotEdge.srcVertex.innerId, snapshotEdge.tgtVertex.innerId).bytes
    val labelWithDirBytes = snapshotEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsSnapshot(LabelIndex.DefaultSeq, isSnapshot = true)

    val row = Bytes.add(
      srcIdAndTgtIdBytes.drop(GraphUtil.bytesForMurMurHash),
      labelWithDirBytes,
      labelIndexSeqWithIsInvertedBytes
    )

    val timestamp = InnerVal(BigDecimal(snapshotEdge.version)).bytes

    val value = snapshotEdge.pendingEdgeOpt match {
      case None => Bytes.add(timestamp, valueBytes())
      case Some(pendingEdge) =>
        val opBytes = statusCodeWithOp(pendingEdge.statusCode, pendingEdge.op)
        val propsBytes = propsToKeyValuesWithTs(pendingEdge.propsWithTs.toSeq)
        val lockBytes = Bytes.toBytes(pendingEdge.lockTs.get)
        timestamp ++ valueBytes() ++ opBytes ++ propsBytes ++ lockBytes
    }

    val kv = SKeyValue(Array.empty[Byte], row, Array.empty[Byte], Array.empty[Byte], value, snapshotEdge.version, operation = SKeyValue.SnapshotPut)
    Seq(kv)
  }
}
