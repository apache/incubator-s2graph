package com.daumkakao.s2graph.core.types2

/**
 * Created by shon on 6/6/15.
 */
object VertexQualifier extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): VertexQualifier = {
    VertexQualifier(bytes(offset))
  }
}
case class VertexQualifier(propKey: Byte) extends HBaseSerializable {
  val bytes: Array[Byte] = Array[Byte](propKey)
}
