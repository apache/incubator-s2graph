package com.daumkakao.s2graph.core.types2.v2

import com.daumkakao.s2graph.core.types2.{EdgeValueInvertedLike, InnerValLikeWithTs, HBaseDeserializable}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/10/15.
 */
object EdgeValueInverted extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION2): EdgeValueInverted = {
    var pos = offset
    val op = bytes(pos)
    pos += 1
    var (props, endAt) = bytesToKeyValuesWithTs(bytes, pos, version)
    EdgeValueInverted(op, props)
  }
}
case class EdgeValueInverted(op: Byte,
                             props: Seq[(Byte, InnerValLikeWithTs)]) extends EdgeValueInvertedLike {
  import HBaseDeserializable._
  val bytes: Array[Byte] = {
    Bytes.add(Array.fill(1)(op), propsToKeyValuesWithTs(props))
  }
}
