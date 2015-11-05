package com.kakao.s2graph.core.storage

import org.hbase.async.KeyValue

//object SKeyValue {
//  def apply(kv: KeyValue): SKeyValue = {
//    SKeyValue(Array.empty[Byte], kv.key(), kv.family(), kv.qualifier(), kv.value(), kv.timestamp())
//  }
//}

case class SKeyValue(table: Array[Byte], row: Array[Byte], cf: Array[Byte], qualifier: Array[Byte], value: Array[Byte], timestamp: Long)

trait CanSKeyValue[T] {
  def toSKeyValue(from: T): SKeyValue
}

object CanSKeyValue {

  // For asyncbase KeyValues
  implicit val asyncKeyValue = new CanSKeyValue[KeyValue] {
    def toSKeyValue(kv: KeyValue): SKeyValue = {
      SKeyValue(Array.empty[Byte], kv.key(), kv.family(), kv.qualifier(), kv.value(), kv.timestamp())
    }
  }

  // For hbase KeyValues
}

