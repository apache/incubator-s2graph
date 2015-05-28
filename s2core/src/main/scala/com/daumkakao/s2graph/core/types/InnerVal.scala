package com.daumkakao.s2graph.core.types

import org.apache.hadoop.hbase.util._
/**
 * Created by shon on 5/28/15.
 */
object InnerVal {
  val order = Order.DESCENDING
  def apply(bytes: Array[Byte], offset: Int): InnerVal = {
    val pbr = new SimplePositionedByteRange(bytes)
    pbr.setOffset(offset)
    if (OrderedBytes.isNumeric(pbr)) {
      val numeric = OrderedBytes.decodeNumericAsBigDecimal(pbr)
      InnerVal(BigDecimal(numeric))
    } else if (OrderedBytes.isText(pbr)) {
      val str = OrderedBytes.decodeString(pbr)
      InnerVal(str)
    } else if (OrderedBytes.isBlobVar(pbr)) {
      val blobVar= OrderedBytes.decodeBlobVar(pbr)
      InnerVal(blobVar)
    } else {
      throw new RuntimeException("!!")
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
}

/**
 * expect BigDecimal, String, Array[Byte] as parameter
 * @param value
 */
case class InnerVal(value: Any) {
  import InnerVal._
  lazy val bytes = {
    val ret = value match {
      case b: BigDecimal =>
        val pbr = numByteRange(b)
        OrderedBytes.encodeNumeric(pbr, b.bigDecimal, order)
        pbr.getBytes()
      case s: String =>
        val pbr = new SimplePositionedMutableByteRange(s.getBytes.length + 3)
        OrderedBytes.encodeString(pbr, s, order)
        pbr.getBytes()
      case blob: Array[Byte] =>
        val len = OrderedBytes.blobVarEncodedLength(blob.length)
        val pbr = new SimplePositionedMutableByteRange(len)
        OrderedBytes.encodeBlobVar(pbr, blob, order)
        pbr.getBytes()
    }
    println(s"$value => ${ret.toList}")
    ret
  }

  override def equals(obj: Any) = {
    obj match {
      case other: InnerVal =>
        Bytes.compareTo(bytes, other.bytes) == 0
      case _ => false
    }
  }
}
