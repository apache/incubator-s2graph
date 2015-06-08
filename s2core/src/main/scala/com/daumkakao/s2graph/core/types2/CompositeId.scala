package com.daumkakao.s2graph.core.types2

import com.daumkakao.s2graph.core.GraphUtil
import org.apache.hadoop.hbase.util.Bytes


object CompositeId {
  val defaultColId = 0
  val defaultInnerId = 0
  val isDescOrder = false
  val VERSION2 = "v2"
  val VERSION1 = "v1"
  val DEFAULTVER = VERSION2

  //  val emptyCompositeId = new CompositeId(defaultColId,
  //    InnerVal.withLong(defaultInnerId), isEdge = true, useHash = true)
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                isEdge: Boolean,
                useHash: Boolean, ver: String = DEFAULTVER): CompositeId = {

    var pos = offset
    if (useHash) {
      // skip over murmur hash
      pos += GraphUtil.bytesForMurMurHash
    }
    val innerId = InnerVal.fromBytes(bytes, pos, 0, ver)
    pos += innerId.bytes.length
    if (isEdge) {
      new CompositeId(defaultColId, innerId, true, useHash)
    } else {
      val cId = Bytes.toInt(bytes, pos, 4)
      new CompositeId(cId, innerId, false, useHash)
    }
  }
}
/**
 * version 1, version 2 use same encoding rule for compositeId.
 * only difference between them is different innerVal class used.
 * Created by shon on 6/6/15.
 */

case class CompositeId(val colId: Int,
                  val innerId: InnerValLike,
                  val isEdge: Boolean,
                  val useHash: Boolean) extends HBaseSerializable {
  //    play.api.Logger.debug(s"$this")
  val hash = GraphUtil.murmur3(innerId.value.toString)
  val bytes = {
    var ret = if (useHash) Bytes.toBytes(hash) else Array.empty[Byte]
    isEdge match {
      case true => Bytes.add(ret, innerId.bytes)
      case false =>
        Bytes.add(ret, innerId.bytes, Bytes.toBytes(colId))
    }
  }
  lazy val bytesInUse = bytes.length
  def updateIsEdge(otherIsEdge: Boolean) = CompositeId(colId, innerId, otherIsEdge, useHash)
  def updateUseHash(otherUseHash: Boolean) = CompositeId(colId, innerId, isEdge, otherUseHash)
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