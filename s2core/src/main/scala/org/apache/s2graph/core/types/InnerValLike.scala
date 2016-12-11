/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.types

import org.apache.hadoop.hbase.util._
import org.apache.s2graph.core.utils.logger

object InnerVal extends HBaseDeserializableWithIsVertexId {
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
  val NUMERICS = Set(DOUBLE, FLOAT, LONG, INT, SHORT, BYTE)
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
                version: String,
                isVertexId: Boolean): (InnerValLike, Int) = {
    version match {
      case VERSION2 | VERSION3 | VERSION4 => v2.InnerVal.fromBytes(bytes, offset, len, version, isVertexId)
//      case VERSION1 => v1.InnerVal.fromBytes(bytes, offset, len, version, isVertexId)
      case _ => throw notSupportedEx(version)
    }
  }

  def withLong(l: Long, version: String): InnerValLike = {
    version match {
      case VERSION2 | VERSION3 | VERSION4 => v2.InnerVal(BigDecimal(l))
//      case VERSION1 => v1.InnerVal(Some(l), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withInt(i: Int, version: String): InnerValLike = {
    version match {
      case VERSION2 | VERSION3 | VERSION4 => v2.InnerVal(BigDecimal(i))
//      case VERSION1 => v1.InnerVal(Some(i.toLong), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withFloat(f: Float, version: String): InnerValLike = {
    version match {
      case VERSION2 | VERSION3 | VERSION4 => v2.InnerVal(BigDecimal(f.toDouble))
//      case VERSION1 => v1.InnerVal(Some(f.toLong), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withDouble(d: Double, version: String): InnerValLike = {
    version match {
      case VERSION2 | VERSION3 | VERSION4 => v2.InnerVal(BigDecimal(d))
//      case VERSION1 => v1.InnerVal(Some(d.toLong), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withNumber(num: BigDecimal, version: String): InnerValLike = {
    version match {
      case VERSION2 | VERSION3 | VERSION4 => v2.InnerVal(num)
//      case VERSION1 => v1.InnerVal(Some(num.toLong), None, None)
      case _ => throw notSupportedEx(version)
    }
  }

  def withBoolean(b: Boolean, version: String): InnerValLike = {
    version match {
      case VERSION2 | VERSION3 | VERSION4 => v2.InnerVal(b)
//      case VERSION1 => v1.InnerVal(None, None, Some(b))
      case _ => throw notSupportedEx(version)
    }
  }

  def withBlob(blob: Array[Byte], version: String): InnerValLike = {
    version match {
      case VERSION2 | VERSION3 | VERSION4 => v2.InnerVal(blob)
      case _ => throw notSupportedEx(version)
    }
  }

  def withStr(s: String, version: String): InnerValLike = {
    version match {
      case VERSION2 | VERSION3 | VERSION4 => v2.InnerVal(s)
//      case VERSION1 => v1.InnerVal(None, Some(s), None)
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
//  def convertVersion(innerVal: InnerValLike, dataType: String, toVersion: String): InnerValLike = {
//    val ret = toVersion match {
//      case VERSION2 | VERSION3 | VERSION4 =>
//        if (innerVal.isInstanceOf[v1.InnerVal]) {
//          val obj = innerVal.asInstanceOf[v1.InnerVal]
//          obj.valueType match {
//            case "long" => InnerVal.withLong(obj.longV.get, toVersion)
//            case "string" => InnerVal.withStr(obj.strV.get, toVersion)
//            case "boolean" => InnerVal.withBoolean(obj.boolV.get, toVersion)
//            case _ => throw new Exception(s"InnerVal should be [long/integeer/short/byte/string/boolean]")
//          }
//        } else {
//          innerVal
//        }
////      case VERSION1 =>
////        if (innerVal.isInstanceOf[v2.InnerVal]) {
////          val obj = innerVal.asInstanceOf[v2.InnerVal]
////          obj.value match {
////            case str: String => InnerVal.withStr(str, toVersion)
////            case b: Boolean => InnerVal.withBoolean(b, toVersion)
////            case n: BigDecimal => InnerVal.withNumber(n, toVersion)
////            case n: Long => InnerVal.withNumber(n, toVersion)
////            case n: Double => InnerVal.withNumber(n, toVersion)
////            case n: Int => InnerVal.withNumber(n, toVersion)
////            case _ => throw notSupportedEx(s"v2 to v1: $obj -> $toVersion")
////          }
////        } else {
////          innerVal
////        }
//      case _ => throw notSupportedEx(toVersion)
//    }
////    logger.debug(s"convertVersion: $innerVal, $dataType, $toVersion, $ret, ${innerVal.bytes.toList}, ${ret.bytes.toList}")
//    ret
//  }

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

  override def equals(obj: Any): Boolean = obj match {
    case other: InnerValLike => value == other.value
    case _ => false
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

  def withDouble(d: Double, ts: Long, version: String): InnerValLikeWithTs = {
    InnerValLikeWithTs(InnerVal.withDouble(d, version), ts)
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

trait CanInnerValLike[A] {
  def toInnerVal(element: A)(implicit encodingVer: String): InnerValLike
}
object CanInnerValLike {
  implicit val encodingVer = "v2"

  def castValue(element: Any, classType: String): Any = {
    import InnerVal._
    element match {
      case bd: BigDecimal =>
        classType match {
          case DOUBLE => bd.doubleValue()
          case FLOAT => bd.floatValue()
          case LONG => bd.longValue()
          case INT | "int" => bd.intValue()
          case SHORT => bd.shortValue()
          case BYTE => bd.byteValue()
          case _ => throw new RuntimeException(s"not supported data type: $element, $classType")
        }
      case _ => element
//        throw new RuntimeException(s"not supported data type: $element, ${element.getClass.getCanonicalName}, $classType")
    }
  }
  def validate(element: Any, classType: String): Boolean = {
    import InnerVal._
    classType match {
      case BLOB => element.isInstanceOf[Array[Byte]]
      case STRING => element.isInstanceOf[String]
      case DOUBLE => element.isInstanceOf[Double] || element.isInstanceOf[BigDecimal]
      case FLOAT => element.isInstanceOf[Float] || element.isInstanceOf[BigDecimal]
      case LONG => element.isInstanceOf[Long] || element.isInstanceOf[BigDecimal]
      case INT => element.isInstanceOf[Int] || element.isInstanceOf[BigDecimal]
      case SHORT => element.isInstanceOf[Short]  || element.isInstanceOf[BigDecimal]
      case BYTE => element.isInstanceOf[Byte]  || element.isInstanceOf[BigDecimal]
      case BOOLEAN => element.isInstanceOf[Boolean]
      case _ => throw new RuntimeException(s"not supported data type: $element, $classType")
    }
  }
  implicit val anyToInnerValLike = new CanInnerValLike[Any] {
    override def toInnerVal(element: Any)(implicit encodingVer: String): InnerValLike = {
      element match {
        case i: InnerValLike => i
        case s: String => stringToInnerValLike.toInnerVal(s)
        case i: Int => intToInnerValLike.toInnerVal(i)
        case l: Long => longToInnerValLike.toInnerVal(l)
        case f: Float => floatToInnerValLike.toInnerVal(f)
        case d: Double => doubleToInnerValLike.toInnerVal(d)
        case b: BigDecimal => bigDecimalToInnerValLike.toInnerVal(b)
        case b: Boolean => booleanToInnerValLike.toInnerVal(b)
        case b: Array[Byte] => blobToInnerValLike.toInnerVal(b)
        case _ => throw new RuntimeException(s"not supported element type: $element, ${element.getClass}")
      }
    }
  }
  implicit val innerValLikeToInnerValLike = new CanInnerValLike[InnerValLike] {
    override def toInnerVal(element: InnerValLike)(implicit encodingVer: String): InnerValLike = element
  }
  implicit val objectToInnerValLike = new CanInnerValLike[Object] {
    override def toInnerVal(element: Object)(implicit encodingVer: String): InnerValLike = {
      anyToInnerValLike.toInnerVal(element.asInstanceOf[Any])
    }
  }

  implicit val stringToInnerValLike = new CanInnerValLike[String] {
    override def toInnerVal(element: String)(implicit encodingVer: String): InnerValLike = {
      InnerVal.withStr(element, encodingVer)
    }
  }
  implicit val longToInnerValLike = new CanInnerValLike[Long] {
    override def toInnerVal(element: Long)(implicit encodingVer: String): InnerValLike = {
      InnerVal.withLong(element, encodingVer)
    }
  }
  implicit val intToInnerValLike = new CanInnerValLike[Int] {
    override def toInnerVal(element: Int)(implicit encodingVer: String): InnerValLike = {
      InnerVal.withInt(element, encodingVer)
    }
  }
  implicit val floatToInnerValLike = new CanInnerValLike[Float] {
    override def toInnerVal(element: Float)(implicit encodingVer: String): InnerValLike = {
      InnerVal.withFloat(element, encodingVer)
    }
  }
  implicit val doubleToInnerValLike = new CanInnerValLike[Double] {
    override def toInnerVal(element: Double)(implicit encodingVer: String): InnerValLike = {
      InnerVal.withDouble(element, encodingVer)
    }
  }
  implicit val bigDecimalToInnerValLike = new CanInnerValLike[BigDecimal] {
    override def toInnerVal(element: BigDecimal)(implicit encodingVer: String): InnerValLike = {
      InnerVal.withNumber(element, encodingVer)
    }
  }
  implicit val booleanToInnerValLike = new CanInnerValLike[Boolean] {
    override def toInnerVal(element: Boolean)(implicit encodingVer: String): InnerValLike = {
      InnerVal.withBoolean(element, encodingVer)
    }
  }
  implicit val blobToInnerValLike = new CanInnerValLike[Array[Byte]] {
    override def toInnerVal(element: Array[Byte])(implicit encodingVer: String): InnerValLike = {
      InnerVal.withBlob(element, encodingVer)
    }
  }
}
