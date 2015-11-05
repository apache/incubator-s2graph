package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core.QueryParam
import com.kakao.s2graph.core.types.{HBaseType, InnerVal, InnerValLike, InnerValLikeWithTs}

object StorageDeserializable {
  /** Deserializer */
  def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }

  def bytesToKeyValues(bytes: Array[Byte],
                       offset: Int,
                       length: Int,
                       version: String): (Array[(Byte, InnerValLike)], Int) = {
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
                             version: String): (Array[(Byte, InnerValLikeWithTs)], Int) = {
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
                   version: String): (Array[(Byte, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = HBaseType.EMPTY_SEQ_BYTE
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

trait StorageDeserializable[E] {
  def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam, kvs: Seq[T], version: String, cacheElementOpt: Option[E]): E
}
