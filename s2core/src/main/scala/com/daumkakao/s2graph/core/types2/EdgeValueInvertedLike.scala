package com.daumkakao.s2graph.core.types2

/**
 * Created by shon on 6/10/15.
 */
object EdgeValueInverted extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): EdgeValueInvertedLike = {
    version match {
      case VERSION2 => v2.EdgeValueInverted.fromBytes(bytes, offset, len, version)
      case VERSION1 => v1.EdgeValueInverted.fromBytes(bytes, offset, len, version)
      case _ => throw notSupportedEx(version)
    }
  }
  def newInstance(op: Byte,
                  props: Seq[(Byte, InnerValLikeWithTs)] = Seq.empty[(Byte, InnerValLikeWithTs)])
                 (version: String = DEFAULT_VERSION): EdgeValueInvertedLike = {
    version match {
      case VERSION2 => v2.EdgeValueInverted(op, props)
      case VERSION1 => v1.EdgeValueInverted(op, props)
      case _ => throw notSupportedEx(version)
    }
  }
}
trait EdgeValueInvertedLike extends HBaseSerializable {
  val op: Byte
  val props: Seq[(Byte, InnerValLikeWithTs)]
  val bytes: Array[Byte]
}
