package com.daumkakao.s2graph.core.types2

import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/6/15.
 */
object HBaseDeserializable {
  def propsToBytes(props: Seq[(Byte, InnerValLike)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, v.bytes)
    bytes
  }
  def propsToKeyValues(props: Seq[(Byte, InnerValLike)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
    bytes
  }
  def propsToKeyValuesWithTs(props: Seq[(Byte, InnerValLikeWithTs)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
    bytes
  }
  def labelOrderSeqWithIsInverted(labelOrderSeq: Byte, isInverted: Boolean): Array[Byte] = {
    assert(labelOrderSeq < (1 << 6))
    val byte = labelOrderSeq << 1 | (if (isInverted) 1 else 0)
    Array.fill(1)(byte.toByte)
  }
  def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }
}
trait HBaseSerializable {
  val bytes: Array[Byte]
}
trait HBaseDeserializable {
  val VERSION2 = "v2"
  val VERSION1 = "v1"
  val DEFAULT_VERSION = VERSION2
  val EMPTY_SEQ_BYTE = Byte.MaxValue
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): HBaseSerializable
  def notSupportedEx(version: String) = new RuntimeException(s"not supported version, $version")

  def bytesToKeyValues(bytes: Array[Byte],
                       offset: Int,
                       len: Int,
                       version: String = DEFAULT_VERSION): (Seq[(Byte, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = for (i <- (0 until len)) yield {
      val k = bytes(pos)
      pos += 1
      val v = InnerVal.fromBytes(bytes, pos, 0, version)
      pos += v.bytes.length
      (k -> v)
    }
    val ret = (kvs.toList, pos)
    //    Logger.debug(s"bytesToProps: $ret")
    ret
  }

  def bytesToKeyValuesWithTs(bytes: Array[Byte],
                             offset: Int,
                             version: String = DEFAULT_VERSION): (Seq[(Byte, InnerValLikeWithTs)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = for (i <- (0 until len)) yield {
      val k = bytes(pos)
      pos += 1
      val v = InnerValLikeWithTs.fromBytes(bytes, pos, 0, version)
      pos += v.bytes.length
      (k -> v)
    }
    val ret = (kvs.toList, pos)
    //    Logger.debug(s"bytesToProps: $ret")
    ret
  }

  def bytesToProps(bytes: Array[Byte],
                   offset: Int,
                   version: String = DEFAULT_VERSION): (Seq[(Byte, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = for (i <- (0 until len)) yield {
      val k = EMPTY_SEQ_BYTE
      val v = InnerVal.fromBytes(bytes, pos, 0, version)
      pos += v.bytes.length
      (k -> v)
    }
    //    Logger.error(s"bytesToProps: $kvs")
    val ret = (kvs.toList, pos)

    ret
  }
}
