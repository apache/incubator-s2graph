package com.daumkakao.s2graph.core.types2

/**
 * Created by shon on 6/6/15.
 */
object VertexRowKey extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): VertexRowKey = {
    val id = CompositeId.fromBytes(bytes, offset, isEdge = false, useHash = true)
    VertexRowKey(id)
  }
}
case class VertexRowKey(id: CompositeId) extends HBaseSerializable {
  val bytes: Array[Byte] = id.bytes
}
