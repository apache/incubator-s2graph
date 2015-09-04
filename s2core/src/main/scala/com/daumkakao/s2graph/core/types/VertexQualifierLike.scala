package com.daumkakao.s2graph.core.types

/**
 * Created by shon on 6/10/15.
 */
object VertexQualifier extends HBaseDeserializable  {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (VertexQualifierLike, Int) = {
    version match {
      case VERSION2 => v2.VertexQualifier.fromBytes(bytes, offset, len, version)
      case VERSION1 => v1.VertexQualifier.fromBytes(bytes, offset, len, version)
      case _ => throw notSupportedEx(version)
    }
  }
  def apply(propKey: Int)(version: String): VertexQualifierLike = {
    version match {
      case VERSION2 => v2.VertexQualifier(propKey)
      case VERSION1 => v1.VertexQualifier(propKey)
      case _ => throw notSupportedEx(version)
    }
  }
}
trait VertexQualifierLike extends HBaseSerializable {

  val propKey: Int
  def bytes: Array[Byte]

}
