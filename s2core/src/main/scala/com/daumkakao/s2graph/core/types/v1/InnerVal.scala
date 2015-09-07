package com.daumkakao.s2graph.core.types.v1

import com.daumkakao.s2graph.core.KGraphExceptions.IllegalDataTypeException
import com.daumkakao.s2graph.core.types._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/6/15.
 */
object InnerVal extends HBaseDeserializable {
  import HBaseType._
  //  val defaultVal = new InnerVal(None, None, None)
  val stringLenOffset = 7.toByte
  val maxStringLen = Byte.MaxValue - stringLenOffset
  val maxMetaByte = Byte.MaxValue
  val minMetaByte = 0.toByte
  /**
   * first byte encoding rule.
   * 0 => default
   * 1 => long
   * 2 => int
   * 3 => short
   * 4 => byte
   * 5 => true
   * 6 => false
   * 7 ~ 127 => string len + 7
   */
  val metaByte = Map("default" -> 0, "long" -> 1, "int" -> 2, "short" -> 3,
    "byte" -> 4, "true" -> 5, "false" -> 6).map {
    case (k, v) => (k, v.toByte)
  }
  val metaByteRev = metaByte.map { case (k, v) => (v.toByte, k) } ++ metaByte.map { case (k, v) => ((-v).toByte, k) }

  def maxIdVal(dataType: String) = {
    dataType match {
      case "string" => InnerVal.withStr((0 until (Byte.MaxValue - stringLenOffset)).map("~").mkString)
      case "long" => InnerVal.withLong(Long.MaxValue)
      case "bool" => InnerVal.withBoolean(true)
      case _ => throw IllegalDataTypeException(dataType)
    }
  }

  def minIdVal(dataType: String) = {
    dataType match {
      case "string" => InnerVal.withStr("")
      case "long" => InnerVal.withLong(1)
      case "bool" => InnerVal.withBoolean(false)
      case _ => throw IllegalDataTypeException(dataType)
    }
  }

  def fromBytes(bytes: Array[Byte], offset: Int, len: Int, version: String = DEFAULT_VERSION): (InnerVal, Int) = {
    var pos = offset
    //
    val header = bytes(pos)
    //      logger.debug(s"${bytes(offset)}: ${bytes.toList.slice(pos, bytes.length)}")
    pos += 1

    var numOfBytesUsed = 0
    val (longV, strV, boolV) = metaByteRev.get(header) match {
      case Some(s) =>
        s match {
          case "default" =>
            (None, None, None)
          case "true" =>
            numOfBytesUsed = 0
            (None, None, Some(true))
          case "false" =>
            numOfBytesUsed = 0
            (None, None, Some(false))
          case "byte" =>
            numOfBytesUsed = 1
            val b = bytes(pos)
            val value = if (b >= 0) Byte.MaxValue - b else Byte.MinValue - b - 1
            (Some(value.toLong), None, None)
          case "short" =>
            numOfBytesUsed = 2
            val b = Bytes.toShort(bytes, pos, 2)
            val value = if (b >= 0) Short.MaxValue - b else Short.MinValue - b - 1
            (Some(value.toLong), None, None)
          case "int" =>
            numOfBytesUsed = 4
            val b = Bytes.toInt(bytes, pos, 4)
            val value = if (b >= 0) Int.MaxValue - b else Int.MinValue - b - 1
            (Some(value.toLong), None, None)
          case "long" =>
            numOfBytesUsed = 8
            val b = Bytes.toLong(bytes, pos, 8)
            val value = if (b >= 0) Long.MaxValue - b else Long.MinValue - b - 1
            (Some(value.toLong), None, None)
        }
      case _ => // string
        val strLen = header - stringLenOffset
        numOfBytesUsed = strLen
        (None, Some(Bytes.toString(bytes, pos, strLen)), None)
    }

    (InnerVal(longV, strV, boolV), numOfBytesUsed + 1)
  }

  def withLong(l: Long): InnerVal = {
    //      if (l < 0) throw new IllegalDataRangeException("value shoudl be >= 0")
    InnerVal(Some(l), None, None)
  }

  def withStr(s: String): InnerVal = {
    InnerVal(None, Some(s), None)
  }

  def withBoolean(b: Boolean): InnerVal = {
    InnerVal(None, None, Some(b))
  }

  /**
   * In natural order
   * -129, -128 , -2, -1 < 0 < 1, 2, 127, 128
   *
   * In byte order
   * 0 < 1, 2, 127, 128 < -129, -128, -2, -1
   *
   */
  def transform(l: Long): (Byte, Array[Byte]) = {
    if (Byte.MinValue <= l && l <= Byte.MaxValue) {
      //        val value = if (l < 0) l - Byte.MinValue else l + Byte.MinValue
      val key = if (l >= 0) metaByte("byte") else -metaByte("byte")
      val value = if (l >= 0) Byte.MaxValue - l else Byte.MinValue - l - 1
      val valueBytes = Array.fill(1)(value.toByte)
      (key.toByte, valueBytes)
    } else if (Short.MinValue <= l && l <= Short.MaxValue) {
      val key = if (l >= 0) metaByte("short") else -metaByte("short")
      val value = if (l >= 0) Short.MaxValue - l else Short.MinValue - l - 1
      val valueBytes = Bytes.toBytes(value.toShort)
      (key.toByte, valueBytes)
    } else if (Int.MinValue <= l && l <= Int.MaxValue) {
      val key = if (l >= 0) metaByte("int") else -metaByte("int")
      val value = if (l >= 0) Int.MaxValue - l else Int.MinValue - l - 1
      val valueBytes = Bytes.toBytes(value.toInt)
      (key.toByte, valueBytes)
    } else if (Long.MinValue <= l && l <= Long.MaxValue) {
      val key = if (l >= 0) metaByte("long") else -metaByte("long")
      val value = if (l >= 0) Long.MaxValue - l else Long.MinValue - l - 1
      val valueBytes = Bytes.toBytes(value.toLong)
      (key.toByte, valueBytes)
    } else {
      throw new Exception(s"InnerVal range is out: $l")
    }
  }
}

case class InnerVal(longV: Option[Long], strV: Option[String], boolV: Option[Boolean])
  extends HBaseSerializable with InnerValLike {

  import InnerVal._

  lazy val isDefault = longV.isEmpty && strV.isEmpty && boolV.isEmpty
  val value = (longV, strV, boolV) match {
    case (Some(l), None, None) => l
    case (None, Some(s), None) => s
    case (None, None, Some(b)) => b
    case _ => throw new Exception(s"InnerVal should be [long/integeer/short/byte/string/boolean]")
  }
  def valueType = (longV, strV, boolV) match {
    case (Some(l), None, None) => "long"
    case (None, Some(s), None) => "string"
    case (None, None, Some(b)) => "boolean"
    case _ => throw new Exception(s"InnerVal should be [long/integeer/short/byte/string/boolean]")
  }

  def compare(other: InnerValLike): Int = {
    if (!other.isInstanceOf[InnerVal]) {
      throw new RuntimeException(s"compare between $this vs $other is not supported")
    } else {
//      (value, other.value) match {
//        case (v1: Long, v2: Long) => v1.compare(v2)
//        case (b1: Boolean, b2: Boolean) => b1.compare(b2)
//        case (s1: String, s2: String) => s1.compare(s2)
//        case _ => throw new Exception("Please check a type of the compare operands")
//      }
      Bytes.compareTo(bytes, other.bytes) * -1
    }
  }

  def +(other: InnerValLike) = {
    if (!other.isInstanceOf[InnerVal]) {
      throw new RuntimeException(s"+ between $this vs $other is not supported")
    } else {
      (value, other.value) match {
        case (v1: Long, v2: Long) => InnerVal.withLong(v1 + v2)
        case (b1: Boolean, b2: Boolean) => InnerVal.withBoolean(if (b2) !b1 else b1)
        case _ => throw new Exception("Please check a type of the incr operands")
      }
    }
  }

  def bytes = {
    val (meta, valBytes) = (longV, strV, boolV) match {
      case (None, None, None) =>
        (metaByte("default"), Array.empty[Byte])
      case (Some(l), None, None) =>
        transform(l)
      case (None, None, Some(b)) =>
        val meta = if (b) metaByte("true") else metaByte("false")
        (meta, Array.empty[Byte])
      case (None, Some(s), None) =>
        val sBytes = Bytes.toBytes(s)
        if (sBytes.length > maxStringLen) {
          throw new IllegalDataTypeException(s"string in innerVal maxSize is $maxStringLen, given ${sBytes.length}")
        }
        assert(sBytes.length <= maxStringLen)
        val meta = (stringLenOffset + sBytes.length).toByte
        (meta, sBytes)
      case _ => throw new IllegalDataTypeException("innerVal data type should be [long/string/bool]")
    }
    Bytes.add(Array.fill(1)(meta.toByte), valBytes)
  }

  override def toString(): String = {
    value.toString
  }
  override def hashKey(dataType: String): Int = {
    value.toString.hashCode()
  }
  override def toIdString(): String = {
    value.toString
  }
}
