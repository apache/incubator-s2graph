package com.daumkakao.s2graph.core.types2.v2

import com.daumkakao.s2graph.core.types2.{InnerValLike, HBaseSerializable, HBaseDeserializable, InnerValBase}
import org.apache.hadoop.hbase.util._

/**
 * Created by shon on 6/6/15.
 */
object InnerVal extends HBaseDeserializable {
  val order = Order.DESCENDING
  val stringLenOffset = 7.toByte
  val maxStringLen = Byte.MaxValue - stringLenOffset

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
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): InnerVal = {
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
}
case class InnerVal(value: Any) extends HBaseSerializable with InnerValLike {
  import InnerVal._

  val bytes: Array[Byte] = {
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
  override def equals(obj: Any) = {
    obj match {
      case other: InnerVal =>
        Bytes.compareTo(bytes, other.bytes) == 0
      case _ => false
    }
  }

  def compare(other: InnerValLike): Int = {
    if (!other.isInstanceOf[InnerVal])
      throw new RuntimeException(s"compare $this vs $other")

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

  def +(other: InnerValLike): InnerValLike = {
    if (!other.isInstanceOf[InnerVal])
      throw new RuntimeException(s"+ $this, $other")

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

}
