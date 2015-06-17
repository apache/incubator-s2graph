package com.daumkakao.s2graph.core.types2

/**
 * Created by shon on 6/10/15.
 */
object EdgeQualifier extends HBaseDeserializable {
  val emptySeqByte = EMPTY_SEQ_BYTE
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): EdgeQualifierLike = {
    version match {
      case VERSION2 => v2.EdgeQualifier.fromBytes(bytes, offset, len, version)
      case VERSION1 => v1.EdgeQualifier.fromBytes(bytes, offset, len, version)
      case _ => throw notSupportedEx(version)
    }
  }
  def apply(idxProps: Seq[(Byte, InnerValLike)] = Seq.empty[(Byte, InnerValLike)],
                  tgtVertexId: VertexId = null,
                  op: Byte)(version: String = DEFAULT_VERSION): EdgeQualifierLike = {
    version match {
      case VERSION2 => v2.EdgeQualifier(idxProps, tgtVertexId, op)
      case VERSION1 => v1.EdgeQualifier(idxProps, tgtVertexId, op)
      case _ => throw notSupportedEx(version)
    }
  }
}
trait EdgeQualifierLike extends HBaseSerializable {
  val props: Seq[(Byte, InnerValLike)]
  val tgtVertexId: VertexId
  val op: Byte

  def propsKVs(propsKeys: List[Byte]): List[(Byte, InnerValLike)] = {
    import EdgeQualifier.emptySeqByte
    val filtered = props.filter(kv => kv._1 != emptySeqByte)
    if (filtered.isEmpty) {
      propsKeys.zip(props.map(_._2))
    } else {
      filtered.toList
    }
  }
}
