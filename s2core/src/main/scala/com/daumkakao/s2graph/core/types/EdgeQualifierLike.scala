package com.daumkakao.s2graph.core.types

/**
 * Created by shon on 6/10/15.
 */
object EdgeQualifier extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (EdgeQualifierLike, Int) = {
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
  import HBaseType._
  val props: Seq[(Byte, InnerValLike)]
  val tgtVertexId: VertexId
  val op: Byte

  def propsKVs(propsKeys: Seq[Byte]): Seq[(Byte, InnerValLike)] = {
    for {
      (propKey, (storedPropKey, storedPropVal)) <- propsKeys.zip(props)
    } yield {
      if (storedPropKey == EMPTY_SEQ_BYTE) (propKey -> storedPropVal)
      else (storedPropKey -> storedPropVal)
    }
  }
}
