package com.daumkakao.s2graph.core.types.v1

import com.daumkakao.s2graph.core.types._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/10/15.
 */
object VertexQualifier extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): (VertexQualifier, Int) = {
    val (propKey, numOfBytesUsed) = if (len == 1) {
      (bytes(offset).toInt, 1)
    } else {
      (Bytes.toInt(bytes, offset, 4), 4)
    }
    (VertexQualifier(propKey), numOfBytesUsed)
//    (VertexQualifier(bytes(offset).toInt), 1)
  }
}
case class VertexQualifier(propKey: Int) extends VertexQualifierLike {
//  assert(propKey <= Byte.MaxValue)
  def bytes: Array[Byte] = {
    if (propKey <= Byte.MaxValue) {
      Array[Byte](propKey.toByte)
    } else {
      Bytes.toBytes(propKey)
    }
  }
}
