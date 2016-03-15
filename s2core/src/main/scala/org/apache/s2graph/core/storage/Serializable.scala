package org.apache.s2graph.core.storage

object Serializable {
  val vertexCf = "v".getBytes()
  val edgeCf = "e".getBytes()
}

trait Serializable[E] extends StorageSerializable[E]
