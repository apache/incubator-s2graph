package com.daumkakao.s2graph.core.types2

import com.daumkakao.s2graph.core.GraphUtil
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/10/15.
 */
object VertexId extends HBaseDeserializable {
  val DEFAULT_COL_ID = 0

  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): VertexId = {
    /** since murmur hash is prepended, skip numOfBytes for murmur hash */
    var pos = offset + GraphUtil.bytesForMurMurHash

    val innerId = InnerVal.fromBytes(bytes, pos, len, version)
    pos += innerId.bytes.length
    val colId = Bytes.toInt(bytes, pos, 4)
    VertexId(colId, innerId)
  }

  def apply(colId: Int, innerId: InnerValLike): VertexId = new VertexId(colId, innerId)

//  def toVertexId(colId: Int, innerId: InnerValLike) = {
//    VertexId(colId, innerId)
//  }
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
  val hashBytes =
    if (storeHash) Bytes.toBytes(GraphUtil.murmur3(innerId.value.toString))
    else Array.empty[Byte]

  val colIdBytes: Array[Byte] =
    if (storeColId) Bytes.toBytes(colId)
    else Array.empty[Byte]

  val bytes: Array[Byte] = Bytes.add(hashBytes, innerId.bytes, colIdBytes)

  override def toString(): String = {
    s"VertexId($colId, $innerId)"
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: VertexId => colId == other.colId && innerId == other.innerId
      case _ => false
    }
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
  import VertexId._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): VertexId = {
    /** since murmur hash is prepended, skip numOfBytes for murmur hash */
    var pos = offset + GraphUtil.bytesForMurMurHash
    val innerId = InnerVal.fromBytes(bytes, pos, len, version)

    SourceVertexId(DEFAULT_COL_ID, innerId)
  }

//  def toVertexId(colId: Int, innerId: InnerValLike): VertexId = {
//    VertexId(colId, innerId)
//  }
}

//case class VertexIdWithoutHash(override val colId: Int,
//                               override val innerId: InnerValLike)
//  extends VertexId(colId, innerId) {
//  override val storeHash: Boolean = false
//}
case class SourceVertexId(override val colId: Int,
                          override val innerId: InnerValLike)
  extends VertexId(colId, innerId) {
  override val storeColId: Boolean = false

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: SourceVertexId =>
        val left = innerId.bytes.drop(GraphUtil.bytesForMurMurHash)
        val right = other.innerId.bytes.drop(GraphUtil.bytesForMurMurHash)
        Bytes.compareTo(left, right) == 0
      case _ => false
    }
  }
}

object TargetVertexId extends HBaseDeserializable {
  import VertexId._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): VertexId = {
    /** murmur has is not prepended so start from offset */
    val innerId = InnerVal.fromBytes(bytes, offset, len, version)
    TargetVertexId(DEFAULT_COL_ID, innerId)
  }

//  def toVertexId(colId: Int, innerId: InnerValLike): VertexId = {
//    VertexId(colId, innerId)
//  }
}

case class TargetVertexId(override val colId: Int,
                          override val innerId: InnerValLike)
  extends  VertexId(colId, innerId) {
  override val storeColId: Boolean = false
  override val storeHash: Boolean = false

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: TargetVertexId => innerId == other.innerId
      case _ => false
    }
  }
}
