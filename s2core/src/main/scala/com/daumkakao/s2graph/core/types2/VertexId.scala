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
    var pos = offset + GraphUtil.bytesForMurMurHash

    val innerId = InnerVal.fromBytes(bytes, pos, len, version)
    pos += innerId.bytes.length
    val colId = Bytes.toInt(bytes, pos, 4)
    new VertexId(colId, innerId)
  }
  def toVertexIdWithoutHash(vid: VertexId) = {
    VertexIdWithoutHash(vid.colId, vid.innerId)
  }
  def toVertexIdWithoutColId(vid: VertexId) = {
    VertexIdWithoutColId(vid.colId, vid.innerId)
  }
  def toVertexIdWithoutHashAndColId(vid: VertexId) = {
    VertexIdWithoutHashAndColId(vid.colId, vid.innerId)
  }
}
object VertexIdWithoutHash extends HBaseDeserializable {
  import VertexId._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): VertexId = {
    var pos = offset
    val innerId = InnerVal.fromBytes(bytes, pos, len, version)
    pos += innerId.bytes.length
    val colId = Bytes.toInt(bytes, pos, 4)
    VertexIdWithoutHash(colId, innerId)
  }
}
object VertexIdWithoutColId extends HBaseDeserializable {
  import VertexId._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): VertexId = {
    var pos = offset + GraphUtil.bytesForMurMurHash
    val innerId = InnerVal.fromBytes(bytes, pos, len, version)
    VertexIdWithoutColId(DEFAULT_COL_ID, innerId)
  }
  def toVertexId(vid: VertexId): VertexId = {
    new VertexId(vid.colId, vid.innerId)
  }
}
object VertexIdWithoutHashAndColId extends HBaseDeserializable {
  import VertexId._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): VertexId = {
    var pos = offset + GraphUtil.bytesForMurMurHash
    val innerId = InnerVal.fromBytes(bytes, pos, len, version)
    VertexIdWithoutHashAndColId(DEFAULT_COL_ID, innerId)
  }
  def toVertexId(vid: VertexId): VertexId = {
    new VertexId(vid.colId, vid.innerId)
  }
}
class VertexId(val colId: Int, val innerId: InnerValLike) extends HBaseSerializable {
  val storeHash: Boolean = true
  val storeColId: Boolean = true
  val hashBytes =
    if (storeHash) Bytes.toBytes(GraphUtil.murmur3(innerId.value.toString))
    else Array.empty[Byte]

  val colIdBytes: Array[Byte] =
    if (storeColId) Bytes.toBytes(colId)
    else Array.empty[Byte]

  val bytes: Array[Byte] = Bytes.add(hashBytes, innerId.bytes, colIdBytes)
}
case class VertexIdWithoutHash(override val colId: Int,
                               override val innerId: InnerValLike)
  extends VertexId(colId, innerId) {
  override val storeHash: Boolean = false
}
case class VertexIdWithoutColId(override val colId: Int,
                                override val innerId: InnerValLike)
  extends VertexId(colId, innerId) {
  override val storeColId: Boolean = false
}
case class VertexIdWithoutHashAndColId(override val colId: Int,
                                       override val innerId: InnerValLike)
  extends  VertexId(colId, innerId) {
  override val storeColId: Boolean = false
  override val storeHash: Boolean = false
}