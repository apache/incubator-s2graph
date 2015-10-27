//package com.kakao.s2graph.core.types.v2
//
//import com.kakao.s2graph.core.types._
//
///**
// * Created by shon on 6/10/15.
// */
//object EdgeQualifierInverted extends HBaseDeserializable {
//  import HBaseType._
//  def fromBytes(bytes: Array[Byte],
//                offset: Int,
//                len: Int,
//                version: String = VERSION1): (EdgeQualifierInverted, Int) = {
//    val (tgtVertexId, numOfBytesUsed) = TargetVertexId.fromBytes(bytes, offset, len, version)
//    (EdgeQualifierInverted(tgtVertexId), numOfBytesUsed)
//  }
//}
//case class EdgeQualifierInverted(tgtVertexId: VertexId) extends EdgeQualifierInvertedLike {
//
//  def bytes: Array[Byte] = {
//    VertexId.toTargetVertexId(tgtVertexId).bytes
//  }
//}
