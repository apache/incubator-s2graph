package com.kakao.s2graph.core.types

import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/6/15.
 */
object HBaseType {
  val VERSION3 = "v3"
  val VERSION2 = "v2"
  val VERSION1 = "v1"
//  val DEFAULT_VERSION = VERSION2
  val DEFAULT_VERSION = VERSION3
  val EMPTY_SEQ_BYTE = Byte.MaxValue
  val DEFAULT_COL_ID = 0
  val bitsForDir = 2
  val maxBytes = Bytes.toBytes(Int.MaxValue)
  val toSeqByte = -5.toByte
  val defaultTgtVertexId = null
}

object HBaseDeserializable {

  import HBaseType._

  // 6 bits is used for index sequence so total index per label is limited to 2^6
  def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }

  def bytesToKeyValues(bytes: Array[Byte],
                       offset: Int,
                       length: Int,
                       version: String = DEFAULT_VERSION): (Array[(Byte, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = bytes(pos)
      pos += 1
      val (v, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, 0, version)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    val ret = (kvs, pos)
    //    logger.debug(s"bytesToProps: $ret")
    ret
  }

  def bytesToKeyValuesWithTs(bytes: Array[Byte],
                             offset: Int,
                             version: String = DEFAULT_VERSION): (Array[(Byte, InnerValLikeWithTs)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLikeWithTs)](len)
    var i = 0
    while (i < len) {
      val k = bytes(pos)
      pos += 1
      val (v, numOfBytesUsed) = InnerValLikeWithTs.fromBytes(bytes, pos, 0, version)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    val ret = (kvs, pos)
    //    logger.debug(s"bytesToProps: $ret")
    ret
  }

  def bytesToProps(bytes: Array[Byte],
                   offset: Int,
                   version: String = DEFAULT_VERSION): (Array[(Byte, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = EMPTY_SEQ_BYTE
      val (v, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, 0, version)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    //    logger.error(s"bytesToProps: $kvs")
    val ret = (kvs, pos)

    ret
  }
}

object HBaseSerializable {
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
}

trait HBaseSerializable {
  def bytes: Array[Byte]
}

trait HBaseDeserializable {

  import HBaseType._

  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (HBaseSerializable, Int)

  //  def fromBytesWithIndex(bytes: Array[Byte],
  //                offset: Int,
  //                len: Int,
  //                version: String = DEFAULT_VERSION): (HBaseSerializable, Int)
  def notSupportedEx(version: String) = new RuntimeException(s"not supported version, $version")
}

trait HBaseDeserializableWithIsVertexId {

  import HBaseType._

  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION,
                isVertexId: Boolean = false): (HBaseSerializable, Int)

  def notSupportedEx(version: String) = new RuntimeException(s"not supported version, $version")
}