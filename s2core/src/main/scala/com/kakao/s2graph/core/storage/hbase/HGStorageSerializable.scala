package com.kakao.s2graph.core.storage.hbase

trait HGStorageSerializable {
  def toKeyValues: Seq[HKeyValue]
}