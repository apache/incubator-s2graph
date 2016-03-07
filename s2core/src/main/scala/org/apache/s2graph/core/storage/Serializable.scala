package org.apache.s2graph.core.storage

import StorageSerializable

object Serializable {
  val vertexCf = "v".getBytes()
  val edgeCf = "e".getBytes()
}

trait Serializable[E] extends StorageSerializable[E]
