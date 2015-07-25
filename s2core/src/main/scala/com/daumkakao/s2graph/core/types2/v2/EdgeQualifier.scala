package com.daumkakao.s2graph.core.types2.v2

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
                version: String = VERSION2): (EdgeQualifier, Int) = {
    var pos = offset
    var numOfBytesUsedTotal = 0
    /** changed not to store op bytes on edge qualifier */
    val op = GraphUtil.defaultOpByte
    val (props, tgtVertexId) = {
      val (props, endAt) = bytesToProps(bytes, pos, version)
      numOfBytesUsedTotal += endAt - offset
      //        val tgtVertexId = CompositeId(bytes, endAt, true, false)
      /** check if target vertex Id is included indexProps or seperate field */
      val (tgtVertexId, numOfBytesUsed) = if (endAt == offset + len) {
        (defaultTgtVertexId, 0)
      } else {
        TargetVertexId.fromBytes(bytes, endAt, len, version)
      }
      numOfBytesUsedTotal += numOfBytesUsed
      (props, tgtVertexId)
    }
    (EdgeQualifier(props, tgtVertexId, op), numOfBytesUsedTotal)
  }
}
case class EdgeQualifier(props: Seq[(Byte, InnerValLike)],
                         tgtVertexId: VertexId,
                         op: Byte) extends EdgeQualifierLike {
  import HBaseType._
  import HBaseSerializable._
  lazy val innerTgtVertexId = VertexId.toTargetVertexId(tgtVertexId)
  lazy val propsMap = props.toMap
  lazy val propsBytes = propsToBytes(props)
  def bytes: Array[Byte] = {
        propsMap.get(toSeqByte) match {
          case None => Bytes.add(propsBytes, innerTgtVertexId.bytes)
          case Some(vId) => propsBytes
        }
  }
  override def equals(obj: Any) = {
    obj match {
      case other: EdgeQualifier =>
        props.map(_._2) == other.props.map(_._2) &&
          tgtVertexId == other.tgtVertexId
//          op == other.op
      case _ => false
    }
  }
}
