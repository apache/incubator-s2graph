package com.kakao.s2graph.core.types.v2

import com.kakao.s2graph.core.types._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/10/15.
 */
object EdgeRowKey extends HBaseDeserializable {
  import HBaseType._
  import HBaseDeserializable._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION2): (EdgeRowKey, Int) = {
    var pos = offset
    var numOfBytesUsedTotal = 0
    val (compositeId, numOfBytesUsed) = SourceVertexId.fromBytes(bytes, pos, len, version)
    numOfBytesUsedTotal += numOfBytesUsed
    pos += numOfBytesUsed
    val labelWithDir = LabelWithDirection(Bytes.toInt(bytes, pos, 4))
    numOfBytesUsedTotal += 4
    pos += 4

    val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(bytes, pos)
    numOfBytesUsedTotal += 1
    (EdgeRowKey(compositeId, labelWithDir, labelOrderSeq, isInverted), numOfBytesUsedTotal)
  }
}
case class EdgeRowKey(srcVertexId: VertexId,
                      labelWithDir: LabelWithDirection,
                      labelOrderSeq: Byte,
                      isInverted: Boolean) extends EdgeRowKeyLike {
  import HBaseSerializable._
  lazy val id = VertexId.toSourceVertexId(srcVertexId)
  def bytes = {
    Bytes.add(id.bytes, labelWithDir.bytes,
      labelOrderSeqWithIsInverted(labelOrderSeq, isInverted))
  }
}
