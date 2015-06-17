package com.daumkakao.s2graph.core.types2.v1

import com.daumkakao.s2graph.core.types2.{VertexRowKeyLike, VertexId, HBaseDeserializable}

/**
 * Created by shon on 6/10/15.
 */
object VertexRowKey extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): VertexRowKey = {
    val id = VertexId.fromBytes(bytes, offset, len, version)
    VertexRowKey(id)
  }
}
case class VertexRowKey(id: VertexId) extends VertexRowKeyLike
