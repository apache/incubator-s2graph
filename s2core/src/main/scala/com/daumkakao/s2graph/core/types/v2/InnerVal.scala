package com.daumkakao.s2graph.core.types.v2

import com.daumkakao.s2graph.core.types._
import org.apache.hadoop.hbase.util._

/**
 * Created by shon on 6/6/15.
 */
object InnerVal extends HBaseDeserializable {

  import HBaseType._

  val order = Order.DESCENDING

  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (InnerVal, Int) = {
    val pbr = new SimplePositionedByteRange(bytes)
    pbr.setPosition(offset)
    val startPos = pbr.getPosition
    if (bytes(offset) == -1 | bytes(offset) == 0) {
      /** simple boolean */
      val boolean = order match {
        case Order.DESCENDING => bytes(offset) == 0
        case _ => bytes(offset) == -1
      }
      (InnerVal(boolean), 1)
    }
    else {
      if (OrderedBytes.isNumeric(pbr)) {
        val numeric = OrderedBytes.decodeNumericAsBigDecimal(pbr)
        (InnerVal(BigDecimal(numeric)), pbr.getPosition - startPos)
      } else if (OrderedBytes.isText(pbr)) {
        val str = OrderedBytes.decodeString(pbr)
        (InnerVal(str), pbr.getPosition - startPos)
      } else if (OrderedBytes.isBlobVar(pbr)) {
        val blobVar = OrderedBytes.decodeBlobVar(pbr)
        (InnerVal(blobVar), pbr.getPosition - startPos)
      } else {
        throw new RuntimeException("!!")
      }
    }
  }
}

case class InnerVal(value: Any) extends HBaseSerializable with InnerValLike {

  import com.daumkakao.s2graph.core.types.InnerVal._

  def bytes: Array[Byte] = {
    val ret = value match {
      case b: Boolean =>

        /** since OrderedBytes header start from 0x05, it is safe to use -1, 0
          * for decreasing order (true, false) */
        //        Bytes.toBytes(b)
        order match {
          case Order.DESCENDING => if (b) Array(0.toByte) else Array(-1.toByte)
          case _ => if (!b) Array(0.toByte) else Array(-1.toByte)
        }
      case l: Long =>
        val num = BigDecimal(l)
        val pbr = numByteRange(num)
        val len = OrderedBytes.encodeNumeric(pbr, num.bigDecimal, order)
        pbr.getBytes().take(len)
      case i: Int =>
        val num = BigDecimal(i)
        val pbr = numByteRange(num)
        val len = OrderedBytes.encodeNumeric(pbr, num.bigDecimal, order)
        pbr.getBytes().take(len)
      case sh: Short =>
        val num = BigDecimal(sh)
        val pbr = numByteRange(num)
        val len = OrderedBytes.encodeNumeric(pbr, num.bigDecimal, order)
        pbr.getBytes().take(len)
      case b: Byte =>
        val num = BigDecimal(b)
        val pbr = numByteRange(num)
        val len = OrderedBytes.encodeNumeric(pbr, num.bigDecimal, order)
        pbr.getBytes().take(len)


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


  override def hashKey(dataType: String): Int = {
    if (value.isInstanceOf[String]) {
      // since we use dummy stringn value for degree edge.
      value.toString.hashCode()
    } else {
      dataType match {
        case BYTE => value.asInstanceOf[BigDecimal].bigDecimal.byteValue().hashCode()
        case FLOAT => value.asInstanceOf[BigDecimal].bigDecimal.floatValue().hashCode()
        case DOUBLE => value.asInstanceOf[BigDecimal].bigDecimal.doubleValue().hashCode()
        case LONG => value.asInstanceOf[BigDecimal].bigDecimal.longValue().hashCode()
        case INT => value.asInstanceOf[BigDecimal].bigDecimal.intValue().hashCode()
        case SHORT => value.asInstanceOf[BigDecimal].bigDecimal.shortValue().hashCode()
        case STRING => value.toString.hashCode
        case _ => throw new RuntimeException(s"NotSupportede type: $dataType")
      }
    }
  }

  def compare(other: InnerValLike): Int = {
    if (!other.isInstanceOf[InnerVal])
      throw new RuntimeException(s"compare $this vs $other")
    Bytes.compareTo(bytes, other.bytes) * -1
  }

  def +(other: InnerValLike): InnerValLike = {
    if (!other.isInstanceOf[InnerVal])
      throw new RuntimeException(s"+ $this, $other")

    (value, other.value) match {
      case (v1: BigDecimal, v2: BigDecimal) => new InnerVal(BigDecimal(v1.bigDecimal.add(v2.bigDecimal)))
      case _ => throw new RuntimeException("+ operation on inner val is for big decimal pair")
    }
  }

  //need to be removed ??
  override def toString(): String = {
//    value.toString()
    value match {
      case n: BigDecimal => n.bigDecimal.toPlainString
      case _ => value.toString
    }
  }

  override def toIdString(): String = {
    value match {
      case n: BigDecimal => n.bigDecimal.longValue().toString()
      case _ => value.toString
    }
  }

}
