package com.kakao.s2graph.core.storage.hbase

import org.hbase.async.KeyValue

object HKeyValue {
  def apply(kv: KeyValue): HKeyValue = {
    HKeyValue(Array.empty[Byte], kv.key(), kv.family(), kv.qualifier(), kv.value(), kv.timestamp())
  }
}

case class HKeyValue(table: Array[Byte],
                     row: Array[Byte],
                     cf: Array[Byte],
                     qualifier: Array[Byte],
                     value: Array[Byte],
                     timestamp: Long)
