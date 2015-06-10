//package com.daumkakao.s2graph.core.types2
//
//import com.daumkakao.s2graph.core.GraphUtil
//
///**
// * Created by shon on 6/10/15.
// */
//object CompositeId extends HBaseDeserializable {
//  val defaultColId = 0
//  val defaultInnerId = 0
//  val isDescOrder = false
//}
//trait CompositeIdLike extends HBaseSerializable {
//  val colId: Int
//  val innerId: InnerValLike
//  val isEdge: Boolean
//  val useHash: Boolean
//
//  val hash = GraphUtil.murmur3(innerId.value.toString)
//  def updateIsEdge(otherIsEdge: Boolean) = CompositeId(colId, innerId, otherIsEdge, useHash)
//  def updateUseHash(otherUseHash: Boolean) = CompositeId(colId, innerId, isEdge, otherUseHash)
//  override def equals(obj: Any) = {
//    obj match {
//      case other: CompositeId =>
//        if (isEdge == other.isEdge && useHash == other.useHash) {
//          if (isEdge) {
//            innerId == other.innerId
//          } else {
//            colId == other.colId && innerId == other.innerId
//          }
//        } else {
//          false
//        }
//      case _ => false
//    }
//  }
//}
