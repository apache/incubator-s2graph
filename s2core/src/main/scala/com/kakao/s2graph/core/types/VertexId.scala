package com.kakao.s2graph.core.types

import com.kakao.s2graph.core.GraphUtil
import com.kakao.s2graph.core.types.HBaseType._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/10/15.
 */
object VertexId extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (VertexId, Int) = {
    /** since murmur hash is prepended, skip numOfBytes for murmur hash */
    var pos = offset + GraphUtil.bytesForMurMurHash

    val (innerId, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, len, version, isVertexId = true)
    pos += numOfBytesUsed
    val colId = Bytes.toInt(bytes, pos, 4)
    (VertexId(colId, innerId), GraphUtil.bytesForMurMurHash + numOfBytesUsed + 4)
  }

  def apply(colId: Int, innerId: InnerValLike): VertexId = new VertexId(colId, innerId)

  def toSourceVertexId(vid: VertexId) = {
    SourceVertexId(vid.colId, vid.innerId)
  }

  def toTargetVertexId(vid: VertexId) = {
    TargetVertexId(vid.colId, vid.innerId)
  }
}

class VertexId protected (val colId: Int, val innerId: InnerValLike) extends HBaseSerializable {
  val storeHash: Boolean = true
  val storeColId: Boolean = true
  lazy val hashBytes =
//    if (storeHash) Bytes.toBytes(GraphUtil.murmur3(innerId.toString))
    if (storeHash) Bytes.toBytes(GraphUtil.murmur3(innerId.toIdString()))
    else Array.empty[Byte]

  lazy val colIdBytes: Array[Byte] =
    if (storeColId) Bytes.toBytes(colId)
    else Array.empty[Byte]

  def bytes: Array[Byte] = Bytes.add(hashBytes, innerId.bytes, colIdBytes)

  override def toString(): String = {
    colId.toString() + "," + innerId.toString()
//    s"VertexId($colId, $innerId)"
  }

  override def hashCode(): Int = {
    val ret = if (storeColId) {
      colId * 31 + innerId.hashCode()
    } else {
      innerId.hashCode()
    }
//    logger.debug(s"VertexId.hashCode: $ret")
    ret
  }
  override def equals(obj: Any): Boolean = {
    val ret = obj match {
      case other: VertexId => colId == other.colId && innerId.toIdString() == other.innerId.toIdString()
      case _ => false
    }
//    logger.debug(s"VertexId.equals: $this, $obj => $ret")
    ret
  }

  def compareTo(other: VertexId): Int = {
    Bytes.compareTo(bytes, other.bytes)
  }
  def <(other: VertexId): Boolean = compareTo(other) < 0
  def <=(other: VertexId): Boolean = compareTo(other) <= 0
  def >(other: VertexId): Boolean = compareTo(other) > 0
  def >=(other: VertexId): Boolean = compareTo(other) >= 0
}

object SourceVertexId extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (VertexId, Int) = {
    /** since murmur hash is prepended, skip numOfBytes for murmur hash */
    val pos = offset + GraphUtil.bytesForMurMurHash
    val (innerId, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, len, version, isVertexId = true)

    (SourceVertexId(DEFAULT_COL_ID, innerId), GraphUtil.bytesForMurMurHash + numOfBytesUsed)
  }

}


case class SourceVertexId(override val colId: Int,
                          override val innerId: InnerValLike) extends VertexId(colId, innerId) {
  override val storeColId: Boolean = false
}

object TargetVertexId extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (VertexId, Int) = {
    /** murmur has is not prepended so start from offset */
    val (innerId, numOfBytesUsed) = InnerVal.fromBytes(bytes, offset, len, version, isVertexId = true)
    (TargetVertexId(DEFAULT_COL_ID, innerId), numOfBytesUsed)
  }
}

case class TargetVertexId(override val colId: Int,
                          override val innerId: InnerValLike)
  extends  VertexId(colId, innerId) {
  override val storeColId: Boolean = false
  override val storeHash: Boolean = false

}

object SourceAndTargetVertexIdPair extends HBaseDeserializable {
  val delimiter = ":"
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (SourceAndTargetVertexIdPair, Int) = {
    val pos = offset + GraphUtil.bytesForMurMurHash
    val (srcId, srcBytesLen) = InnerVal.fromBytes(bytes, pos, len, version, isVertexId = true)
    val (tgtId, tgtBytesLen) = InnerVal.fromBytes(bytes, pos + srcBytesLen, len, version, isVertexId = true)
    (SourceAndTargetVertexIdPair(srcId, tgtId), GraphUtil.bytesForMurMurHash + srcBytesLen + tgtBytesLen)
  }
}

case class SourceAndTargetVertexIdPair(val srcInnerId: InnerValLike, val tgtInnerId: InnerValLike) extends HBaseSerializable {
  val colId = DEFAULT_COL_ID
  import SourceAndTargetVertexIdPair._
  override def bytes: Array[Byte] = {
    val hashBytes = Bytes.toBytes(GraphUtil.murmur3(srcInnerId + delimiter + tgtInnerId))
    Bytes.add(hashBytes, srcInnerId.bytes, tgtInnerId.bytes)
  }
}

