package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.storage.GKeyValue
import com.kakao.s2graph.core.types.{InnerValLikeWithTs, InnerValLike}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue

object HGKeyValue {
  def apply(kv: KeyValue): GKeyValue = {
    HGKeyValue(Array.empty[Byte], kv.key(), kv.family(), kv.qualifier(), kv.value(), kv.timestamp())
  }
}

case class HGKeyValue(table: Array[Byte],
                      row: Array[Byte],
                      cf: Array[Byte],
                      qualifier: Array[Byte],
                      value: Array[Byte],
                      timestamp: Long) extends GKeyValue
