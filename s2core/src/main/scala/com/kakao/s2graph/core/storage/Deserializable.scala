package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core.storage.{SKeyValue, StorageDeserializable}
import com.kakao.s2graph.core.types.{LabelWithDirection, SourceVertexId, VertexId}
import org.apache.hadoop.hbase.util.Bytes


trait Deserializable[E] extends StorageDeserializable[E] {
  import StorageDeserializable._

  type RowKeyRaw = (VertexId, LabelWithDirection, Byte, Boolean, Int)

  /** version 1 and version 2 share same code for parsing row key part */
  def parseRow(kv: SKeyValue, version: String): RowKeyRaw = {
    var pos = 0
    val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(kv.row, pos, kv.row.length, version)
    pos += srcIdLen
    val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
    pos += 4
    val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(kv.row, pos)

    val rowLen = srcIdLen + 4 + 1
    (srcVertexId, labelWithDir, labelIdxSeq, isInverted, rowLen)
  }
}