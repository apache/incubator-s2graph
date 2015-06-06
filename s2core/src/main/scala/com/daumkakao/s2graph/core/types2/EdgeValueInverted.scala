package com.daumkakao.s2graph.core.types2

import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/6/15.
 */
object EdgeValueInverted extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): EdgeValueInverted = {
    var pos = offset
    val op = bytes(pos)
    pos += 1
    var (props, endAt) = bytesToKeyValuesWithTs(bytes, pos, version)
    EdgeValueInverted(op, props)
  }
}
case class EdgeValueInverted(op: Byte,
                             props: Seq[(Byte, InnerValLikeWithTs)]) extends HBaseSerializable {

  val bytes: Array[Byte] = {
    Bytes.add(Array.fill(1)(op), propsToKeyValuesWithTs(props))
  }
}
