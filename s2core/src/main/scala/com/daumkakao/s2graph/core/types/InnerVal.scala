package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.KGraphExceptions.IllegalDataTypeException
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

  def toInnerDataType(dataType: String): String = {
    dataType.toLowerCase() match {
      case "blob" => BLOB
      case "string" | "str" | "s" => STRING
      case "double" | "d" | "float64" => DOUBLE
      case "float" | "f" | "float32" => FLOAT
      case "long" | "l" | "int64" | "integer64" => LONG
      case "int" | "integer" | "i" | "int32" | "integer32" => INT
      case "short" | "int16" | "integer16" => SHORT
      case "byte" | "b" | "tinyint" | "int8" | "integer8" => BYTE
      case "boolean" | "bool" => BOOLEAN
      case _ => throw new RuntimeException(s"can`t convert $dataType into InnerDataType")
    }
  }

  def apply(bytes: Array[Byte], offset: Int): InnerVal = {
    val pbr = new SimplePositionedByteRange(bytes)
    pbr.setOffset(offset)
    if (bytes(offset) == -1 | bytes(offset) == 0) {
      /** simple boolean */
      val boolean = order match {
        case Order.DESCENDING => bytes(offset) == 0
        case _ => bytes(offset) == -1
      }
      new InnerVal(boolean)
    }
    else {
      if (OrderedBytes.isNumeric(pbr)) {
        val numeric = OrderedBytes.decodeNumericAsBigDecimal(pbr)
        new InnerVal(BigDecimal(numeric))
      } else if (OrderedBytes.isText(pbr)) {
        val str = OrderedBytes.decodeString(pbr)
        new InnerVal(str)
      } else if (OrderedBytes.isBlobVar(pbr)) {
        val blobVar = OrderedBytes.decodeBlobVar(pbr)
        new InnerVal(blobVar)
      } else {
        throw new RuntimeException("!!")
      }
    }
  }

  def numByteRange(num: BigDecimal) = {
    val byteLen =
      if (num.isValidByte | num.isValidChar) 1
      else if (num.isValidShort) 2
      else if (num.isValidInt) 4
      else if (num.isValidLong) 8
      else if (num.isValidFloat) 4
      else 12
    //      else throw new RuntimeException(s"wrong data $num")
    new SimplePositionedMutableByteRange(byteLen + 4)
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

  def defaultInnerVal(dataType: String): InnerVal = {
    val innerVal = dataType match {
      case STRING => ""
      case BYTE => 0.toByte
      case SHORT => 0.toShort
      case INT => 0
      case LONG => 0L
      case FLOAT => 0.0f
      case DOUBLE => 0.0
      case BOOLEAN => false
    }
    new InnerVal(innerVal)
  }

  /** this part could be unnecessary but can not figure out how to JsNumber not to
    * print out scientific string
    * @param num
    * @return
    */
  def scaleNumber(num: BigDecimal, dataType: String) = {
    dataType match {
      case BYTE => BigDecimal(num.toByte)
      case SHORT => BigDecimal(num.toShort)
      case INT => BigDecimal(num.toInt)
      case LONG => BigDecimal(num.toLong)
      case FLOAT => BigDecimal(num.toFloat)
      case DOUBLE => BigDecimal(num.toDouble)
      case _ => throw new RuntimeException(s"InnerVal.scaleNumber failed. $num, $dataType")
    }
  }

  def withInt(i: Int): InnerVal = new InnerVal(BigDecimal(i))

  def withLong(l: Long): InnerVal = new InnerVal(BigDecimal(l))

  def withFloat(f: Float): InnerVal = new InnerVal(BigDecimal(f))

  def withDouble(d: Double): InnerVal = new InnerVal(BigDecimal(d))

  def withStr(s: String): InnerVal = new InnerVal(s)

  def withNumber(num: BigDecimal): InnerVal = new InnerVal(num)

  def withBoolean(b: Boolean): InnerVal = new InnerVal(b)

  def withBlob(blob: Array[Byte]): InnerVal = new InnerVal(blob)
}

/**
 * expect Boolean,BigDecimal, String, Array[Byte] as parameter
 * @param value
 */
class InnerVal(val value: Any) {

  import InnerVal._
//  println(s"$this, $value")
  lazy val bytes = {
    val ret = value match {
      case b: Boolean =>
        /** since OrderedBytes header start from 0x05, it is safe to use -1, 0
          * for decreasing order (true, false) */
//        Bytes.toBytes(b)
        order match {
          case Order.DESCENDING => if (b) Array(0.toByte) else Array(-1.toByte)
          case _ => if (!b) Array(0.toByte) else Array(-1.toByte)
        }
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
//    println(s"$value => ${ret.toList}, ${ret.length}")
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

  def compare(other: InnerVal) = {
    (value, other.value) match {
      case (v1: BigDecimal, v2: BigDecimal) =>
        v1.compare(v2)
      case (v1: String, v2: String) =>
        v1.compareTo(v2)
      case (v1: Boolean, v2: Boolean) =>
        v1.compareTo(v2)
      case (v1: Array[Byte], v2: Array[Byte]) =>
        Bytes.compareTo(v1, v2)
      case _ =>
        throw new RuntimeException(s"compare between $this and $other failed.")
    }
  }

  def +(other: InnerVal) = {
    (value, other.value) match {
      case (v1: BigDecimal, v2: BigDecimal) => new InnerVal(BigDecimal(v1.bigDecimal.add(v2.bigDecimal)))
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

//  def toJsValue(): JsValue = {
//    value match {
//      case b: BigDecimal => JsNumber(scaleNumber(b))
//      case s: String => JsString(s)
//      case _ => throw new RuntimeException(s"$this -> toJsValue failed.")
//    }
//  }
}

object InnerValWithTs {
  def apply(bytes: Array[Byte], offset: Int, version: String = "v2"): InnerValWithTs = {
    val innerVal =
      if (version == "v1") InnerValV1(bytes, offset)
      else InnerVal(bytes, offset)
    var pos = offset + innerVal.bytes.length
    val ts = Bytes.toLong(bytes, pos, 8)
    InnerValWithTs(innerVal, ts)
  }

  def withLong(value: Long, ts: Long, version: String = "v2") = {
    if (version == "v1") {
      InnerValWithTs(InnerValV1.withLong(value), ts)
    } else {
      InnerValWithTs(InnerVal.withLong(value), ts)
    }
  }

  def withStr(value: String, ts: Long, version: String = "v2") = {
    if (version == "v1") {
      InnerValWithTs(InnerValV1.withStr(value), ts)
    } else {
      InnerValWithTs(InnerVal.withStr(value), ts)
    }
  }
}

case class InnerValWithTs(innerVal: InnerVal, ts: Long) {
  lazy val bytes = Bytes.add(innerVal.bytes, Bytes.toBytes(ts))
}
