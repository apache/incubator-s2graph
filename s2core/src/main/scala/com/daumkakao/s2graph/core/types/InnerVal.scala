package com.daumkakao.s2graph.core.types

import org.apache.hadoop.hbase.util._
import play.api.Logger
import play.api.libs.json.{JsString, JsNumber, JsValue}

/**
 * Created by shon on 5/28/15.
 */
object InnerVal {
  val maxMetaByte = Byte.MaxValue
  val minMetaByte = 0.toByte
  val order = Order.DESCENDING

  /** supported data type */
  val BLOB = "blob"
  val STRING = "string"
  val DOUBLE = "double"
  val FLOAT = "float"
  val LONG = "long"
  val INT = "integer"
  val SHORT = "short"
  val BYTE = "byte"
  val NUMERICS = List(DOUBLE, FLOAT, LONG, INT, SHORT, BYTE)
  val BOOLEAN = "boolean"


  def apply(bytes: Array[Byte], offset: Int): InnerVal = {
    val pbr = new SimplePositionedByteRange(bytes)
    pbr.setOffset(offset)
    if (OrderedBytes.isEncodedValue(pbr)) {
      if (OrderedBytes.isNumeric(pbr)) {
        val numeric = OrderedBytes.decodeNumericAsBigDecimal(pbr)
        InnerVal(BigDecimal(numeric))
      } else if (OrderedBytes.isText(pbr)) {
        val str = OrderedBytes.decodeString(pbr)
        InnerVal(str)
      } else if (OrderedBytes.isBlobVar(pbr)) {
        val blobVar = OrderedBytes.decodeBlobVar(pbr)
        InnerVal(blobVar)
      } else {
        throw new RuntimeException("!!")
      }
    } else {
      /** simple boolean */
      InnerVal(Bytes.toBoolean(bytes))
    }
  }

  def numByteRange(num: BigDecimal) = {
    val byteLen =
      if (num.isValidByte | num.isValidChar) 1
      else if (num.isValidShort) 2
      else if (num.isValidInt) 4
      else if (num.isValidLong) 8
      else if (num.isValidFloat) 4
      else 11
    //      else throw new RuntimeException(s"wrong data $num")
    new SimplePositionedMutableByteRange(byteLen + 3)
  }

  def dataTypeOfNumber(num: BigDecimal) = {
    if (num.isValidByte | num.isValidChar) BYTE
    else if (num.isValidShort) SHORT
    else if (num.isValidInt) INT
    else if (num.isValidLong) LONG
    else if (num.isValidFloat) FLOAT
    else if (num.isValidDouble) DOUBLE
    else throw new RuntimeException("innerVal data type is numeric but can`t find type")
  }

  def withLong(l: Long): InnerVal = InnerVal(BigDecimal(l))

  def withStr(s: String): InnerVal = InnerVal(s)

  def withNumber(num: BigDecimal): InnerVal = InnerVal(num)

  def withBoolean(b: Boolean): InnerVal = InnerVal(b)

  def withBlob(blob: Array[Byte]): InnerVal = InnerVal(blob)
}

/**
 * expect Boolean,BigDecimal, String, Array[Byte] as parameter
 * @param value
 */
case class InnerVal(value: Any) {

  import InnerVal._

  lazy val bytes = {
    val ret = value match {
      case b: Boolean =>
        /** since OrderedBytes header start from 0x05, it is safe to use -1, 0 */
        Bytes.toBytes(b)
      case b: BigDecimal =>
        val pbr = numByteRange(b)
        val len = OrderedBytes.encodeNumeric(pbr, b.bigDecimal, order)
        pbr.getBytes().take(len)
      case s: String =>
        val pbr = new SimplePositionedMutableByteRange(s.getBytes.length + 3)
        val len = OrderedBytes.encodeString(pbr, s, order)
        pbr.getBytes().take(len)
      case blob: Array[Byte] =>
        val len = OrderedBytes.blobVarEncodedLength(blob.length)
        val pbr = new SimplePositionedMutableByteRange(len)
        val totalLen = OrderedBytes.encodeBlobVar(pbr, blob, order)
        pbr.getBytes().take(totalLen)
    }
    println(s"$value => ${ret.toList}, ${ret.length}")
    ret
  }

  def toVal[T] = {
    try {
      value.asInstanceOf[T]
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        throw e
    }
  }

  def compare(other: InnerVal) = Bytes.compareTo(bytes, other.bytes)

  def +(other: InnerVal) = {
    (value, other.value) match {
      case (v1: BigDecimal, v2: BigDecimal) => InnerVal(BigDecimal(v1.bigDecimal.add(v2.bigDecimal)))
      case _ => throw new RuntimeException("+ operation on inner val is for big decimal pair")
    }
  }

  def <(other: InnerVal) = this.compare(other) < 0

  def <=(other: InnerVal) = this.compare(other) <= 0

  def >(other: InnerVal) = this.compare(other) > 0

  def >=(other: InnerVal) = this.compare(other) >= 0

  override def toString(): String = value.toString

  override def equals(obj: Any) = {
    obj match {
      case other: InnerVal =>
        Bytes.compareTo(bytes, other.bytes) == 0
      case _ => false
    }
  }

  def toJsValue(): JsValue = {
    value match {
      case b: BigDecimal => JsNumber(b)
      case s: String => JsString(s)
      case _ => throw new RuntimeException(s"$this -> toJsValue failed.")
    }
  }
}

object InnerValWithTs {
  def apply(bytes: Array[Byte], offset: Int): InnerValWithTs = {
    val innerVal = InnerVal(bytes, offset)
    var pos = offset + innerVal.bytes.length
    val ts = Bytes.toLong(bytes, pos, 8)
    InnerValWithTs(innerVal, ts)
  }

  def withLong(value: Long, ts: Long) = InnerValWithTs(InnerVal.withLong(value), ts)

  def withStr(value: String, ts: Long) = InnerValWithTs(InnerVal.withStr(value), ts)
}

case class InnerValWithTs(innerVal: InnerVal, ts: Long) {
  lazy val bytes = Bytes.add(innerVal.bytes, Bytes.toBytes(ts))
}