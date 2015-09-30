package com.kakao.s2graph.core.types.v1

import com.kakao.s2graph.core.types._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/10/15.
 */
object EdgeValueInverted extends HBaseDeserializable {
  import HBaseType._
  import HBaseDeserializable._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): (EdgeValueInverted, Int) = {
    var pos = offset
    val op = bytes(pos)
    pos += 1
    var (props, endAt) = bytesToKeyValuesWithTs(bytes, pos, version)
    (EdgeValueInverted(op, props), endAt - offset)
  }
}
case class EdgeValueInverted(op: Byte,
                             props: Seq[(Byte, InnerValLikeWithTs)]) extends EdgeValueInvertedLike {
  import HBaseSerializable._
  def bytes: Array[Byte] = {
    Bytes.add(Array.fill(1)(op), propsToKeyValuesWithTs(props))
  }
}
