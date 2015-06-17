package com.daumkakao.s2graph.core.types2.v1

import com.daumkakao.s2graph.core.types2.{VertexId, TargetVertexId, EdgeQualifierInvertedLike, HBaseDeserializable}

/**
 * Created by shon on 6/10/15.
 */

object EdgeQualifierInverted extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): EdgeQualifierInverted = {
    val tgtVertexId = TargetVertexId.fromBytes(bytes, offset, len, version)
    EdgeQualifierInverted(tgtVertexId)
  }
}
case class EdgeQualifierInverted(tgtVertexId: VertexId) extends EdgeQualifierInvertedLike {

  val bytes: Array[Byte] = {
    VertexId.toTargetVertexId(tgtVertexId).bytes
  }
}