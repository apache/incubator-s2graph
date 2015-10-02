package com.kakao.s2graph.core.types

import org.apache.hadoop.hbase.util._

/**
 * Created by shon on 6/6/15.
 */
object InnerVal extends HBaseDeserializable {
  import HBaseType._

  val order = Order.DESCENDING
  val stringLenOffset = 7.toByte
  val maxStringLen = Byte.MaxValue - stringLenOffset
  val maxMetaByte = Byte.MaxValue
  val minMetaByte = 0.toByte

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

  def isNumericType(dataType: String): Boolean = {
    dataType match {
      case InnerVal.BYTE | InnerVal.SHORT | InnerVal.INT | InnerVal.LONG | InnerVal.FLOAT | InnerVal.DOUBLE => true
      case _ => false
    }
  }
  def toInnerDataType(dataType: String): String = {
    dataType match {
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

  def numByteRange(num: BigDecimal) = {
//    val byteLen =
//      if (num.isValidByte | num.isValidChar) 1
//      else if (num.isValidShort) 2
//      else if (num.isValidInt) 4
//      else if (num.isValidLong) 8
//      else if (num.isValidFloat) 4
//      else 12
    val byteLen = 12
    //      else throw new RuntimeException(s"wrong data $num")
    new SimplePositionedMutableByteRange(byteLen + 4)
  }

  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (InnerValLike, Int) = {
    version match {
      case VERSION2 => v2.InnerVal.fromBytes(bytes, offset, len, version)
      case VERSION1 => v1.InnerVal.fromBytes(bytes, offset, len, version)
      case _ => throw notSupportedEx(version)
    }
  }

  def withLong(l: Long, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(BigDecimal(l))
      case VERSION1 => v1.InnerVal(Some(l), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withInt(i: Int, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(BigDecimal(i))
      case VERSION1 => v1.InnerVal(Some(i.toLong), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withFloat(f: Float, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(BigDecimal(f.toDouble))
      case VERSION1 => v1.InnerVal(Some(f.toLong), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withDouble(d: Double, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(BigDecimal(d))
      case VERSION1 => v1.InnerVal(Some(d.toLong), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withNumber(num: BigDecimal, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(num)
      case VERSION1 => v1.InnerVal(Some(num.toLong), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withBoolean(b: Boolean, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(b)
      case VERSION1 => v1.InnerVal(None, None, Some(b))
      case _ => throw notSupportedEx(version)
    }
  }

  def withBlob(blob: Array[Byte], version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(blob)
      case _ => throw notSupportedEx(version)
    }
  }

  def withStr(s: String, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(s)
      case VERSION1 => v1.InnerVal(None, Some(s), None)
      case _ => throw notSupportedEx(version)
    }
  }

//  def withInnerVal(innerVal: InnerValLike, version: String): InnerValLike = {
//    val bytes = innerVal.bytes
//    version match {
//      case VERSION2 => v2.InnerVal.fromBytes(bytes, 0, bytes.length, version)._1
//      case VERSION1 => v1.InnerVal.fromBytes(bytes, 0, bytes.length, version)._1
//      case _ => throw notSupportedEx(version)
//    }
//  }

  /** nasty implementation for backward compatability */
  def convertVersion(innerVal: InnerValLike, dataType: String, toVersion: String): InnerValLike = {
    val ret = toVersion match {
      case VERSION2 =>
        if (innerVal.isInstanceOf[v1.InnerVal]) {
          val obj = innerVal.asInstanceOf[v1.InnerVal]
          obj.valueType match {
            case "long" => InnerVal.withLong(obj.longV.get, toVersion)
            case "string" => InnerVal.withStr(obj.strV.get, toVersion)
            case "boolean" => InnerVal.withBoolean(obj.boolV.get, toVersion)
            case _ => throw new Exception(s"InnerVal should be [long/integeer/short/byte/string/boolean]")
          }
        } else {
          innerVal
        }
      case VERSION1 =>
        if (innerVal.isInstanceOf[v2.InnerVal]) {
          val obj = innerVal.asInstanceOf[v2.InnerVal]
          obj.value match {
            case str: String => InnerVal.withStr(str, toVersion)
            case b: Boolean => InnerVal.withBoolean(b, toVersion)
            case n: BigDecimal => InnerVal.withNumber(n, toVersion)
            case _ => throw notSupportedEx(s"v2 to v1: $obj -> $toVersion")
          }
        } else {
          innerVal
        }
      case _ => throw notSupportedEx(toVersion)
    }
//    logger.debug(s"convertVersion: $innerVal, $dataType, $toVersion, $ret, ${innerVal.bytes.toList}, ${ret.bytes.toList}")
    ret
  }

}

trait InnerValLike extends HBaseSerializable {

  val value: Any

  def compare(other: InnerValLike): Int

  def +(other: InnerValLike): InnerValLike

  def <(other: InnerValLike) = this.compare(other) < 0

  def <=(other: InnerValLike) = this.compare(other) <= 0

  def >(other: InnerValLike) = this.compare(other) > 0

  def >=(other: InnerValLike) = this.compare(other) >= 0

  override def toString(): String = value.toString

  override def hashCode(): Int = value.hashCode()

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: InnerValLike =>
        val ret = toString == obj.toString
//        logger.debug(s"InnerValLike.equals($this, $obj) => $ret")
        ret
      case _ => false
    }
  }
  def hashKey(dataType: String): Int

  def toIdString(): String

}

object InnerValLikeWithTs extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (InnerValLikeWithTs, Int) = {
    val (innerVal, numOfBytesUsed) = InnerVal.fromBytes(bytes, offset, len, version)
    val ts = Bytes.toLong(bytes, offset + numOfBytesUsed)
    (InnerValLikeWithTs(innerVal, ts), numOfBytesUsed + 8)
  }

  def withLong(l: Long, ts: Long, version: String): InnerValLikeWithTs = {
    InnerValLikeWithTs(InnerVal.withLong(l, version), ts)
  }

  def withStr(s: String, ts: Long, version: String): InnerValLikeWithTs = {
    InnerValLikeWithTs(InnerVal.withStr(s, version), ts)
  }
}

case class InnerValLikeWithTs(innerVal: InnerValLike, ts: Long)
  extends HBaseSerializable {

  def bytes: Array[Byte] = {
    Bytes.add(innerVal.bytes, Bytes.toBytes(ts))
  }
}
