package com.daumkakao.s2graph.core.types2.v1

import com.daumkakao.s2graph.core.types2._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/10/15.
 */
object EdgeRowKey extends HBaseDeserializable {
  import HBaseDeserializable._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): EdgeRowKey = {
    var pos = offset
    val compositeId = VertexIdWithoutColId.fromBytes(bytes, pos, len, version)
    pos += compositeId.bytes.length
    val labelWithDir = LabelWithDirection(Bytes.toInt(bytes, pos, 4))
    pos += 4
    val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(bytes, pos)
    EdgeRowKey(compositeId, labelWithDir, labelOrderSeq, isInverted)
  }
}
case class EdgeRowKey(srcVertexId: VertexId,
                      labelWithDir: LabelWithDirection,
                      labelOrderSeq: Byte,
                      isInverted: Boolean) extends EdgeRowKeyLike {
  import HBaseDeserializable._
  val id = VertexId.toVertexIdWithoutColId(srcVertexId)
  val bytes = {
    Bytes.add(id.bytes, labelWithDir.bytes,
      labelOrderSeqWithIsInverted(labelOrderSeq, isInverted))
  }
}
