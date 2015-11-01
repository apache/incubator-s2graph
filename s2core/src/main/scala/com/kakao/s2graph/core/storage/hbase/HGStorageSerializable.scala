package com.kakao.s2graph.core.storage.hbase

object HGStorageSerializable {
  val vertexCf = "v".getBytes()
  val edgeCf = "e".getBytes()
}

trait HGStorageSerializable {
  def toKeyValues: Seq[HKeyValue]
}