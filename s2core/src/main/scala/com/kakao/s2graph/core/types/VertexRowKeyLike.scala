package com.kakao.s2graph.core.types

/**
 * Created by shon on 6/10/15.
 */
object VertexRowKey extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (VertexRowKeyLike, Int) = {
    version match {
      case VERSION2 => v2.VertexRowKey.fromBytes(bytes, offset, len, version)
      case VERSION1 => v1.VertexRowKey.fromBytes(bytes, offset, len, version)
      case _ => throw notSupportedEx(version)
    }
  }
  def apply(id: VertexId)(version: String): VertexRowKeyLike = {
    version match {
      case VERSION2 => v2.VertexRowKey(id)
      case VERSION1 => v1.VertexRowKey(id)
      case _ => throw notSupportedEx(version)
    }
  }
}
trait VertexRowKeyLike extends HBaseSerializable {
  val id: VertexId
  def bytes: Array[Byte] = id.bytes
}
