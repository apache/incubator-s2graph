package com.daumkakao.s2graph.core.types2.v1

import com.daumkakao.s2graph.core.types2.{HBaseDeserializable, VertexQualifierLike}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/10/15.
 */
object VertexQualifier extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): VertexQualifier = {
    VertexQualifier(bytes(offset).toInt)
  }
}
case class VertexQualifier(propKey: Int) extends VertexQualifierLike {
  assert(propKey <= Byte.MaxValue)
  val bytes: Array[Byte] = Array[Byte](propKey.toByte)
}
