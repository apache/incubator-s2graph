package com.kakao.s2graph.core.types

/**
 * Created by shon on 6/10/15.
 */
object EdgeQualifierInverted extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (EdgeQualifierInvertedLike, Int) = {
    version match {
      case VERSION2 => v2.EdgeQualifierInverted.fromBytes(bytes, offset, len, version)
      case VERSION1 => v1.EdgeQualifierInverted.fromBytes(bytes, offset, len, version)
      case _ => throw notSupportedEx(version)
    }
  }
  def apply(tgtVertexId: VertexId)(version: String): EdgeQualifierInvertedLike = {
    version match {
      case VERSION2 => v2.EdgeQualifierInverted(tgtVertexId)
      case VERSION1 => v1.EdgeQualifierInverted(tgtVertexId)
      case _ => throw notSupportedEx(version)
    }
  }
}
trait EdgeQualifierInvertedLike extends HBaseSerializable {
  val tgtVertexId: VertexId
  def bytes: Array[Byte]
}
