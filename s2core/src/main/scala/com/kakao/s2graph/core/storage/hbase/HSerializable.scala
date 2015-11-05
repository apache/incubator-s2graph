package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.storage.StorageSerializable

object HSerializable {
  val vertexCf = "v".getBytes()
  val edgeCf = "e".getBytes()
}

trait HSerializable[E] extends StorageSerializable[E]
