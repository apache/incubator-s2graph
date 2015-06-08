package com.daumkakao.s2graph.core.types2

import com.daumkakao.s2graph.core.GraphUtil
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/6/15.
 */
object EdgeQualifier extends HBaseDeserializable {
  val toSeqByte = -5.toByte
  val defaultTgtVertexId = null

  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): EdgeQualifier = {
    var pos = offset
    /** changed not to store op bytes on edge qualifier */
    val op = version match {
      case VERSION2 => GraphUtil.defaultOpByte
      case VERSION1 => bytes(offset + len - 1)
      case _ => throw notSupportedEx(version)
    }
    val (props, tgtVertexId) = {
      val (props, endAt) = bytesToProps(bytes, pos, version)
      //        val tgtVertexId = CompositeId(bytes, endAt, true, false)
      /** check if target vertex Id is included indexProps or seperate field */
      val tgtVertexId = if (endAt == offset + len) {
        defaultTgtVertexId
      } else {
        CompositeId.fromBytes(bytes, endAt, true, false, version)
      }
      (props, tgtVertexId)
    }
    EdgeQualifier(props, tgtVertexId, op, version)
  }
}
case class EdgeQualifier(props: Seq[(Byte, InnerValLike)],
                         tgtVertexId: CompositeId,
                         op: Byte,
                         version: String) extends HBaseSerializable {
  import EdgeQualifier._
  import HBaseDeserializable._
  val innerTgtVertexId = tgtVertexId.updateUseHash(false)
  val propsMap = props.toMap
  val propsBytes = propsToBytes(props)
  val bytes: Array[Byte] = {
    if (version == InnerVal.VERSION2) {
      /** check if target vertex id is already included in indexProps. */
      propsMap.get(toSeqByte) match {
        case None => Bytes.add(propsBytes, innerTgtVertexId.bytes)
        case Some(vId) => propsBytes
      }
    } else {
      Bytes.add(propsBytes, innerTgtVertexId.bytes)
    }
  }
  def propsKVs(propsKeys: List[Byte]): List[(Byte, InnerValLike)] = {
    val filtered = props.filter(kv => kv._1 != EMPTY_SEQ_BYTE)
    if (filtered.isEmpty) {
      propsKeys.zip(props.map(_._2))
    } else {
      filtered.toList
    }
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
