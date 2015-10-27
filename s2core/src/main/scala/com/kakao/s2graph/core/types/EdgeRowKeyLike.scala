//package com.kakao.s2graph.core.types
//
///**
// * Created by shon on 6/10/15.
// */
//object EdgeRowKey extends HBaseDeserializable {
//  import HBaseType._
//  def fromBytes(bytes: Array[Byte],
//                offset: Int,
//                len: Int,
//                version: String = DEFAULT_VERSION): (EdgeRowKeyLike, Int) = {
//    version match {
//      case VERSION2 => v2.EdgeRowKey.fromBytes(bytes, offset, len, version)
//      case VERSION1 => v1.EdgeRowKey.fromBytes(bytes, offset, len, version)
//      case _ => throw notSupportedEx(version)
//    }
//  }
//
//  def apply(srcVertexId: VertexId,
//                  labelWithDir: LabelWithDirection,
//                  labelOrderSeq: Byte,
//                  isInverted: Boolean,
//                  idxProps: Seq[(Byte, InnerValLike)] = Seq.empty[(Byte, InnerValLike)],
//                  tgtVertexId: VertexId = null)(version: String = DEFAULT_VERSION): EdgeRowKeyLike = {
//    version match {
//      case VERSION2 => v2.EdgeRowKey(srcVertexId, labelWithDir, labelOrderSeq, isInverted)
//      case VERSION1 => v1.EdgeRowKey(srcVertexId, labelWithDir, labelOrderSeq, isInverted)
//      case _ => throw notSupportedEx(version)
//    }
//  }
//}
//
//trait EdgeRowKeyLike extends HBaseSerializable {
//  val srcVertexId: VertexId
//  val labelWithDir: LabelWithDirection
//  val labelOrderSeq: Byte
//  val isInverted: Boolean
//
//  def bytes: Array[Byte]
//}
