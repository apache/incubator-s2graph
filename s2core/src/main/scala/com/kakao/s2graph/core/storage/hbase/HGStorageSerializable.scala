package com.kakao.s2graph.core.storage.hbase

trait HGStorageSerializable[E] {
  def toKeyValues: Seq[HKeyValue]
}