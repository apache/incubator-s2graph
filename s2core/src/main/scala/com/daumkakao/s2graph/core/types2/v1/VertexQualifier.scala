package com.daumkakao.s2graph.core.types2.v1

import com.daumkakao.s2graph.core.types2._

/**
 * Created by shon on 6/10/15.
 */
object VertexQualifier extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): (VertexQualifier, Int) = {
    (VertexQualifier(bytes(offset).toInt), 1)
  }
}
case class VertexQualifier(propKey: Int) extends VertexQualifierLike {
  assert(propKey <= Byte.MaxValue)
  def bytes: Array[Byte] = Array[Byte](propKey.toByte)
}
