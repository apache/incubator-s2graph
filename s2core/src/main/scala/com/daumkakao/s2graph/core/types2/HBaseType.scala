package com.daumkakao.s2graph.core.types2

import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/6/15.
 */
trait HBaseSerializable {
  val bytes: Array[Byte]
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
  val notSupportedEx = new RuntimeException("not supported version")

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
                             version: String = "v2"): (Seq[(Byte, InnerValLikeWithTs)], Int) = {
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
                   version: String = "v2"): (Seq[(Byte, InnerValLike)], Int) = {
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

