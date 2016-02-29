package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core.storage.StorageSerializable

object Serializable {
  val vertexCf = "v".getBytes()
  val edgeCf = "e".getBytes()
}

trait Serializable[E] extends StorageSerializable[E]
