package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.GraphUtil
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 5/29/15.
 */
object CompositeId {
  val defaultColId = 0
  val defaultInnerId = 0
  val isDescOrder = false
//  val emptyCompositeId = new CompositeId(defaultColId,
//    InnerVal.withLong(defaultInnerId), isEdge = true, useHash = true)

  def apply(bytes: Array[Byte], offset: Int, isEdge: Boolean, useHash: Boolean): CompositeId = {
    var pos = offset
    if (useHash) {
      // skip over murmur hash
      pos += GraphUtil.bytesForMurMurHash
    }
    val innerId = InnerVal(bytes, pos)
    pos += innerId.bytes.length
    if (isEdge) {
      new CompositeId(defaultColId, innerId, true, useHash)
    } else {
      val cId = Bytes.toInt(bytes, pos, 4)
      new CompositeId(cId, innerId, false, useHash)
    }
  }
}
// TODO: colId range < (1<<15??) id length??
class CompositeId(val colId: Int,
                  val innerId: InnerVal,
                  val isEdge: Boolean,
                  val useHash: Boolean) extends HBaseType {
  //    play.api.Logger.debug(s"$this")
  lazy val hash = GraphUtil.murmur3(innerId.value.toString)
  lazy val bytes = {
    var ret = if (useHash) Bytes.toBytes(hash) else Array.empty[Byte]
    isEdge match {
      case true => Bytes.add(ret, innerId.bytes)
      case false =>
        Bytes.add(ret, innerId.bytes, Bytes.toBytes(colId))
    }
  }
  lazy val bytesInUse = bytes.length
  def updateIsEdge(otherIsEdge: Boolean) = new CompositeId(colId, innerId, otherIsEdge, useHash)
  def updateUseHash(otherUseHash: Boolean) = new CompositeId(colId, innerId, isEdge, otherUseHash)
  override def equals(obj: Any) = {
    obj match {
      case other: CompositeId =>
        if (isEdge == other.isEdge && useHash == other.useHash) {
          if (isEdge) {
            innerId == other.innerId
          } else {
            colId == other.colId && innerId == other.innerId
          }
        } else {
          false
        }
      case _ => false
    }
  }
}
