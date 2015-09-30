package com.kakao.s2graph.core.types.v1

import com.kakao.s2graph.core.types._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/10/15.
 */
object EdgeQualifier extends HBaseDeserializable {
  import HBaseType._
  import HBaseDeserializable._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): (EdgeQualifier, Int) = {
    var pos = offset
    var numOfBytesUsedTotal = 0
    /** changed not to store op bytes on edge qualifier */
    val op = bytes(offset + len - 1)
    numOfBytesUsedTotal += 1
    val (props, tgtVertexId) = {
      val (decodedProps, endAt) = bytesToProps(bytes, pos, version)
      val (decodedVId, numOfBytesUsed) = TargetVertexId.fromBytes(bytes, endAt, len, version)
      numOfBytesUsedTotal += numOfBytesUsed + (endAt - offset)
      (decodedProps, decodedVId)
    }
    (EdgeQualifier(props, tgtVertexId, op), numOfBytesUsedTotal)
  }
}
case class EdgeQualifier(props: Seq[(Byte, InnerValLike)],
                         tgtVertexId: VertexId,
                         op: Byte) extends EdgeQualifierLike {
  import HBaseSerializable._
  lazy val innerTgtVertexId = VertexId.toTargetVertexId(tgtVertexId)
  lazy val propsMap = props.toMap
  lazy val propsBytes = propsToBytes(props)
  def bytes: Array[Byte] = {
        Bytes.add(propsBytes, innerTgtVertexId.bytes, Array[Byte](op))
  }

  override def equals(obj: Any) = {
    obj match {
      case other: EdgeQualifier =>
        props.map(_._2) == other.props.map(_._2) &&
          tgtVertexId == other.tgtVertexId &&
          op == other.op
      case _ => false
    }
  }
}
