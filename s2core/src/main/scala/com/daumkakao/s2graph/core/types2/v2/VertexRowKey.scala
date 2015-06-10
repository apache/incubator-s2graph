package com.daumkakao.s2graph.core.types2.v2

import com.daumkakao.s2graph.core.types2.{VertexRowKeyLike, HBaseSerializable, VertexId, HBaseDeserializable}

/**
 * Created by shon on 6/10/15.
 */
object VertexRowKey extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION2): VertexRowKey = {
    val id = VertexId.fromBytes(bytes, offset, len, version)
    VertexRowKey(id)
  }
}
case class VertexRowKey(id: VertexId) extends VertexRowKeyLike
