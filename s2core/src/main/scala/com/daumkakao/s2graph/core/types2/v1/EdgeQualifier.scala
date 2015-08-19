package com.daumkakao.s2graph.core.types2.v1

import com.daumkakao.s2graph.core.GraphUtil
import com.daumkakao.s2graph.core.types2._
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

    val (props, tgtVertexId) = {
      val (props, endAt) = bytesToProps(bytes, pos, version)
      numOfBytesUsedTotal += endAt - offset


      val (tgtVertexId, numOfBytesUsed) = if (endAt == offset + len) {
        (defaultTgtVertexId, 0)
      } else {
        TargetVertexId.fromBytes(bytes, endAt, len, version)
      }
      numOfBytesUsedTotal += numOfBytesUsed
      (props, tgtVertexId)
    }
    val (op, opBytesUsed) =
      if (offset + numOfBytesUsedTotal == len) (GraphUtil.defaultOpByte, 0)
      else {
        (bytes(offset + numOfBytesUsedTotal), 1)
      }
    (EdgeQualifier(props, tgtVertexId, op), numOfBytesUsedTotal + opBytesUsed)
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
