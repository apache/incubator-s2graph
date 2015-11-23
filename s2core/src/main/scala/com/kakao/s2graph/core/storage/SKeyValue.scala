package com.kakao.s2graph.core.storage

import org.hbase.async.KeyValue

case class SKeyValue(table: Array[Byte], row: Array[Byte], cf: Array[Byte], qualifier: Array[Byte], value: Array[Byte], timestamp: Long) {
  def toLogString = {
    Map("table" -> table.toList, "row" -> row.toList, "cf" -> cf.toList, "qualifier" -> qualifier.toList, "value" -> value.toList, "timestamp" -> timestamp).toString
  }
}

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

  // For asyncbase KeyValues
  implicit val sKeyValue = new CanSKeyValue[SKeyValue] {
    def toSKeyValue(kv: SKeyValue): SKeyValue = kv
  }

  // For hbase KeyValues
}

