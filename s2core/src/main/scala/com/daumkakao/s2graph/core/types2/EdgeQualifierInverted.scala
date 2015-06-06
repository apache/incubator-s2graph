package com.daumkakao.s2graph.core.types2

/**
 * Created by shon on 6/6/15.
 */
object EdgeQualifierInverted extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): EdgeQualifierInverted = {
    val tgtVertexId = CompositeId.fromBytes(bytes, offset, isEdge = true, useHash = false, version)
    EdgeQualifierInverted(tgtVertexId)
  }
}
case class EdgeQualifierInverted(tgtVertexId: CompositeId)
  extends HBaseSerializable {

  val bytes: Array[Byte] = {
    tgtVertexId.updateUseHash(false).bytes
  }
}
