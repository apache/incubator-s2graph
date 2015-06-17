package com.daumkakao.s2graph.core.types2.v1

import com.daumkakao.s2graph.core.types2.{EdgeValueLike, InnerValLike, HBaseDeserializable}

/**
 * Created by shon on 6/10/15.
 */
object EdgeValue extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): EdgeValue = {
    val (props, endAt) = bytesToKeyValues(bytes, offset, 0, version)
    EdgeValue(props)
  }
}
case class EdgeValue(props: Seq[(Byte, InnerValLike)]) extends EdgeValueLike {
  import HBaseDeserializable._
  val bytes: Array[Byte] = propsToKeyValues(props)
}
