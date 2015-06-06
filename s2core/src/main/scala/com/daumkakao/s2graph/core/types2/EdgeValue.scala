package com.daumkakao.s2graph.core.types2

/**
 * Created by shon on 6/6/15.
 */
object EdgeValue extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): EdgeValue = {
    val (props, endAt) = bytesToKeyValues(bytes, offset, 0, version)
    EdgeValue(props)
  }
}
case class EdgeValue(props: Seq[(Byte, InnerValLike)]) extends HBaseSerializable {

  val bytes: Array[Byte] = propsToKeyValues(props)
}
