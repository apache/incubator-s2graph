package com.daumkakao.s2graph.core.types2

import org.apache.hadoop.hbase.util._

import scala.reflect.ClassTag

/**
 * Created by shon on 6/6/15.
 */
object InnerVal extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                 offset: Int,
                 len: Int,
                 version: String = DEFAULT_VERSION): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal.fromBytes(bytes, offset, len, version)
      case VERSION1 => v1.InnerVal.fromBytes(bytes, offset, len, version)
      case _ => throw notSupportedEx
    }
  }
  def withLong(l: Long, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(BigDecimal(l))
      case VERSION1 => v1.InnerVal(Some(l), None, None)
      case _ => throw notSupportedEx
    }
  }
  def withInt(i: Int, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(BigDecimal(i))
      case VERSION1 => v1.InnerVal(Some(i.toLong), None, None)
      case _ => throw notSupportedEx
    }
  }
  def withFloat(f: Float, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(BigDecimal(f))
      case _ => throw notSupportedEx
    }
  }
  def withDouble(d: Double, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(BigDecimal(d))
      case _ => throw notSupportedEx
    }
  }
  def withNumber(num: BigDecimal, version: String): InnerValLike =  {
    version match {
      case VERSION2 => v2.InnerVal(num)
      case _ => throw notSupportedEx
    }
  }
  def withBoolean(b: Boolean, version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(b)
      case VERSION1 => v1.InnerVal(None, None, Some(b))
      case _ => throw notSupportedEx
    }
  }
  def withBlob(blob: Array[Byte], version: String): InnerValLike = {
    version match {
      case VERSION2 => v2.InnerVal(blob)
      case _ => throw notSupportedEx
    }
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

}

object InnerValLikeWithTs extends HBaseDeserializable {
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): InnerValLikeWithTs = {
    val innerVal = InnerVal.fromBytes(bytes, offset, len, version)
    val ts = Bytes.toLong(bytes, offset + innerVal.bytes.length)
    InnerValLikeWithTs(innerVal, ts)
  }


//  def withLong(value: Long, ts: Long, version: String = "v2") = {
//    if (version == "v1") {
//      InnerValWithTs(InnerValV1.withLong(value), ts)
//    } else {
//      InnerValWithTs(InnerVal.withLong(value), ts)
//    }
//  }
//
//  def withStr(value: String, ts: Long, version: String = "v2") = {
//    if (version == "v1") {
//      InnerValWithTs(InnerValV1.withStr(value), ts)
//    } else {
//      InnerValWithTs(InnerVal.withStr(value), ts)
//    }
//  }
}
case class InnerValLikeWithTs(innerVal: InnerValLike, ts: Long)
  extends HBaseSerializable {

  val bytes: Array[Byte] = {
    Bytes.add(innerVal.bytes, Bytes.toBytes(ts))
  }
}
