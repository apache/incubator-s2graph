//package com.kakao.s2graph.core.types
//
///**
// * Created by shon on 6/10/15.
// */
//object EdgeValue extends HBaseDeserializable {
//  import HBaseType._
//  def fromBytes(bytes: Array[Byte],
//                offset: Int,
//                len: Int,
//                version: String = DEFAULT_VERSION): (EdgeValueLike, Int) = {
//    version match {
//      case VERSION2 => v2.EdgeValue.fromBytes(bytes, offset, len, version)
//      case VERSION1 => v1.EdgeValue.fromBytes(bytes, offset, len, version)
//      case _ => throw notSupportedEx(version)
//    }
//  }
//  def apply(props: Seq[(Byte, InnerValLike)])(version: String): EdgeValueLike = {
//    version match {
//      case VERSION2 => v2.EdgeValue(props)
//      case VERSION1 => v1.EdgeValue(props)
//      case _ => throw notSupportedEx(version)
//    }
//  }
//}
//trait EdgeValueLike extends HBaseSerializable {
//  val props: Seq[(Byte, InnerValLike)]
//  def bytes: Array[Byte]
//}
