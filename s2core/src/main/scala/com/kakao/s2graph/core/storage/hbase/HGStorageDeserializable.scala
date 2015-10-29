package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.QueryParam
import com.kakao.s2graph.core.storage.{GKeyValue, GraphDeserializable}
import com.kakao.s2graph.core.types.{SourceVertexId, LabelWithDirection, VertexId}
import org.apache.hadoop.hbase.util.Bytes


trait HGStorageDeserializable[E] extends GraphDeserializable {

  type RowKeyRaw = (VertexId, LabelWithDirection, Byte, Boolean, Int)

  def fromKeyValues(queryParam: QueryParam, kvs: Seq[GKeyValue], version: String, cacheElementOpt: Option[E]): E

  /** version 1 and version 2 share same code for parsing row key part */
  def parseRow(kv: GKeyValue, version: String): RowKeyRaw = {
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