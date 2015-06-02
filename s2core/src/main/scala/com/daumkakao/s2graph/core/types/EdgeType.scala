package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.models.{HLabelIndex, HLabelMeta}
import org.apache.hadoop.hbase.util.Bytes
import LabelWithDirection._
import play.api.Logger

/**
 * Created by shon on 5/29/15.
 */
object EdgeType {
  def propsToBytes(props: Seq[(Byte, InnerVal)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, v.bytes)

//        Logger.debug(s"propsToBytes: $props => ${bytes.toList}")
    bytes
  }
  def propsToKeyValues(props: Seq[(Byte, InnerVal)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
//        Logger.error(s"propsToBytes: $props => ${bytes.toList}")
    bytes
  }
  def propsToKeyValuesWithTs(props: Seq[(Byte, InnerValWithTs)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
//        Logger.error(s"propsToBytes: $props => ${bytes.toList}")
    bytes
  }
  def bytesToKeyValues(bytes: Array[Byte], offset: Int): (Seq[(Byte, InnerVal)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = for (i <- (0 until len)) yield {
      val k = bytes(pos)
      pos += 1
      val v = InnerVal(bytes, pos)
      pos += v.bytes.length
      (k -> v)
    }
    val ret = (kvs.toList, pos)
    //    Logger.debug(s"bytesToProps: $ret")
    ret
  }
  def bytesToKeyValuesWithTs(bytes: Array[Byte], offset: Int): (Seq[(Byte, InnerValWithTs)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = for (i <- (0 until len)) yield {
      val k = bytes(pos)
      pos += 1
      val v = InnerValWithTs(bytes, pos)
      pos += v.bytes.length
      (k -> v)
    }
    val ret = (kvs.toList, pos)
    //    Logger.debug(s"bytesToProps: $ret")
    ret
  }
  def bytesToProps(bytes: Array[Byte], offset: Int): (Seq[(Byte, InnerVal)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = for (i <- (0 until len)) yield {
      val k = HLabelMeta.emptyValue
      val v = InnerVal(bytes, pos)

      pos += v.bytes.length
      (k -> v)
    }
//    Logger.error(s"bytesToProps: $kvs")
    val ret = (kvs.toList, pos)

    ret
  }
  object EdgeRowKey {
    val propMode = 0
    val isEdge = true
    def apply(bytes: Array[Byte], offset: Int): EdgeRowKey = {
      var pos = offset
      val copmositeId = CompositeId(bytes, pos, isEdge, true)
      pos += copmositeId.bytesInUse
      val labelWithDir = LabelWithDirection(Bytes.toInt(bytes, pos, 4))
      pos += 4
      val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(bytes, pos)
      EdgeRowKey(copmositeId, labelWithDir, labelOrderSeq, isInverted)
    }
  }
  //TODO: split inverted table? cf?
  case class EdgeRowKey(srcVertexId: CompositeId,
                        labelWithDir: LabelWithDirection,
                        labelOrderSeq: Byte,
                        isInverted: Boolean) extends HBaseType {
    //    play.api.Logger.debug(s"$this")
    lazy val innerSrcVertexId = srcVertexId.updateUseHash(true)
    lazy val bytes = Bytes.add(innerSrcVertexId.bytes, labelWithDir.bytes, labelOrderSeqWithIsInverted(labelOrderSeq, isInverted))
  }

  object EdgeQualifier {
    val isEdge = true
    val degreeTgtId = Byte.MinValue
    val degreeOp = 0.toByte
    def apply(bytes: Array[Byte], offset: Int, len: Int): EdgeQualifier = {
      var pos = offset
      val op = bytes(offset + len - 1)
      val (props, tgtVertexId) = {
        val (props, endAt) = bytesToProps(bytes, pos)
        val tgtVertexId = CompositeId(bytes, endAt, true, false)
//        val tgtVertexId = propsMap.get(HLabelMeta.toSeq) match {
//          case None => CompositeId(bytes, endAt, true, false)
//          case Some(vId) => CompositeId(CompositeId.defaultColId, vId, true, false)
//        }
        (props, tgtVertexId)
      }
      EdgeQualifier(props, tgtVertexId, op)
    }
  }
  case class EdgeQualifier(props: Seq[(Byte, InnerVal)], tgtVertexId: CompositeId, op: Byte) extends HBaseType {

    val opBytes = Array.fill(1)(op)
    val innerTgtVertexId = tgtVertexId.updateUseHash(false)
    lazy val propsMap = props.toMap
    lazy val propsBytes = propsToBytes(props)
    lazy val bytes = {
      Bytes.add(propsBytes, innerTgtVertexId.bytes, opBytes)
//      propsMap.get(HLabelMeta.toSeq) match {
//        case None => Bytes.add(propsBytes, innerTgtVertexId.bytes, opBytes)
//        case Some(vId) => Bytes.add(propsBytes, opBytes)
//      }

    }
    //TODO:
    def propsKVs(labelId: Int, labelOrderSeq: Byte): List[(Byte, InnerVal)] = {
      val filtered = props.filter(kv => kv._1 != HLabelMeta.emptyValue)
      if (filtered.isEmpty) {
        val opt = for (index <- HLabelIndex.findByLabelIdAndSeq(labelId, labelOrderSeq)) yield {
          val v = index.metaSeqs.zip(props.map(_._2))
          v
        }
        opt.getOrElse(List.empty[(Byte, InnerVal)])
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

  object EdgeQualifierInverted {
    def apply(bytes: Array[Byte], offset: Int): EdgeQualifierInverted = {
      val tgtVertexId = CompositeId(bytes, offset, true, false)
      EdgeQualifierInverted(tgtVertexId)
    }
  }
  case class EdgeQualifierInverted(tgtVertexId: CompositeId) extends HBaseType {
    //    play.api.Logger.debug(s"$this")
    val innerTgtVertexId = tgtVertexId.updateUseHash(false)
    lazy val bytes = innerTgtVertexId.bytes
  }
  object EdgeValue {
    def apply(bytes: Array[Byte], offset: Int): EdgeValue = {
      val (props, endAt) = bytesToKeyValues(bytes, offset)
      EdgeValue(props)
    }
  }
  case class EdgeValue(props: Seq[(Byte, InnerVal)]) extends HBaseType {
    lazy val bytes = propsToKeyValues(props)
  }
  object EdgeValueInverted {
    def apply(bytes: Array[Byte], offset: Int): EdgeValueInverted = {
      var pos = offset
      val op = bytes(pos)
      pos += 1
      val (props, endAt) = bytesToKeyValuesWithTs(bytes, pos)
      EdgeValueInverted(op, props)
    }
  }
  case class EdgeValueInverted(op: Byte, props: Seq[(Byte, InnerValWithTs)]) extends HBaseType {
    lazy val bytes = Bytes.add(Array.fill(1)(op), propsToKeyValuesWithTs(props))
  }
}
