package com.daumkakao.s2graph.core.types2

import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/6/15.
 */
object EdgeRowKey extends HBaseDeserializable {
  def labelOrderSeqWithIsInverted(labelOrderSeq: Byte, isInverted: Boolean): Array[Byte] = {
    assert(labelOrderSeq < (1 << 6))
    val byte = labelOrderSeq << 1 | (if (isInverted) 1 else 0)
    Array.fill(1)(byte.toByte)
  }
  def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                ver: String = DEFAULT_VERSION): EdgeRowKey = {
    var pos = offset
    val compositeId = CompositeId.fromBytes(bytes, pos, true, true, ver)
    pos += compositeId.bytes.length
    val labelWithDir = LabelWithDirection(Bytes.toInt(bytes, pos, 4))
    pos += 4
    val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(bytes, pos)
    EdgeRowKey(compositeId, labelWithDir, labelOrderSeq, isInverted)
  }
}
case class EdgeRowKey(srcVertexId: CompositeId,
                       labelWithDir: LabelWithDirection,
                       labelOrderSeq: Byte,
                       isInverted: Boolean) extends HBaseSerializable {
  import EdgeRowKey._
  val id = CompositeId(srcVertexId.colId, srcVertexId.innerId,
    srcVertexId.isEdge, useHash = true)
  val bytes = {
    Bytes.add(id.bytes, labelWithDir.bytes,
      labelOrderSeqWithIsInverted(labelOrderSeq, isInverted))
  }
}
