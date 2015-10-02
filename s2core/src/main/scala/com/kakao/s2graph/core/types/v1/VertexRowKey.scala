package com.kakao.s2graph.core.types.v1

import com.kakao.s2graph.core.types._

/**
 * Created by shon on 6/10/15.
 */
object VertexRowKey extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): (VertexRowKey, Int) = {
    val (id, numOfBytesUsed) = VertexId.fromBytes(bytes, offset, len, version)
    (VertexRowKey(id), numOfBytesUsed)
  }
}
case class VertexRowKey(id: VertexId) extends VertexRowKeyLike
