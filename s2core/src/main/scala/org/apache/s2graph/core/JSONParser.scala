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

package org.apache.s2graph.core

import org.apache.s2graph.core.GraphExceptions.IllegalDataTypeException
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.rest.TemplateHelper
import org.apache.s2graph.core.types.{InnerValLikeWithTs, InnerVal, InnerValLike}
import org.apache.s2graph.core.utils.logger
import play.api.libs.json._


object JSONParser {

  //TODO: check result notation on bigDecimal.
  def innerValToJsValue(innerVal: InnerValLike, dataType: String): Option[JsValue] = {
    try {
      val dType = InnerVal.toInnerDataType(dataType)
      val jsValue = dType match {
        case InnerVal.STRING => JsString(innerVal.value.asInstanceOf[String])
        case InnerVal.BOOLEAN => JsBoolean(innerVal.value.asInstanceOf[Boolean])
        case InnerVal.BYTE | InnerVal.SHORT | InnerVal.INT | InnerVal.LONG | InnerVal.FLOAT | InnerVal.DOUBLE =>
          //        case t if InnerVal.NUMERICS.contains(t) =>
          innerVal.value match {
            case l: Long => JsNumber(l)
            case i: Int => JsNumber(i)
            case s: Short => JsNumber(s.toLong)
            case b: Byte => JsNumber(b.toLong)
            case f: Float => JsNumber(f.toDouble)
            case d: Double =>
              //              JsNumber(d)
              dType match {
                case InnerVal.BYTE => JsNumber(d.toInt)
                case InnerVal.SHORT => JsNumber(d.toInt)
                case InnerVal.INT => JsNumber(d.toInt)
                case InnerVal.LONG => JsNumber(d.toLong)
                case InnerVal.FLOAT => JsNumber(d.toDouble)
                case InnerVal.DOUBLE => JsNumber(d.toDouble)
                case _ => throw new RuntimeException(s"$innerVal, $dType => $dataType")
              }
            case num: BigDecimal =>
              //              JsNumber(num)
              //              JsNumber(InnerVal.scaleNumber(num.asInstanceOf[BigDecimal], dType))
              dType match {
                case InnerVal.BYTE => JsNumber(num.toInt)
                case InnerVal.SHORT => JsNumber(num.toInt)
                case InnerVal.INT => JsNumber(num.toInt)
                case InnerVal.LONG => JsNumber(num.toLong)
                case InnerVal.FLOAT => JsNumber(num.toDouble)
                case InnerVal.DOUBLE => JsNumber(num.toDouble)
                case _ => throw new RuntimeException(s"$innerVal, $dType => $dataType")
              }
            //              JsNumber(num.toLong)
            case _ => throw new RuntimeException(s"$innerVal, Numeric Unknown => $dataType")
          }
        //          JsNumber(InnerVal.scaleNumber(innerVal.asInstanceOf[BigDecimal], dType))
        case _ => throw new RuntimeException(s"$innerVal, Unknown => $dataType")
      }
      Some(jsValue)
    } catch {
      case e: Exception =>
        logger.info(s"JSONParser.innerValToJsValue: $e")
        None
    }
  }

  //  def innerValToString(innerVal: InnerValLike, dataType: String): String = {
  //    val dType = InnerVal.toInnerDataType(dataType)
  //    InnerVal.toInnerDataType(dType) match {
  //      case InnerVal.STRING => innerVal.toString
  //      case InnerVal.BOOLEAN => innerVal.toString
  //      //      case t if InnerVal.NUMERICS.contains(t)  =>
  //      case InnerVal.BYTE | InnerVal.SHORT | InnerVal.INT | InnerVal.LONG | InnerVal.FLOAT | InnerVal.DOUBLE =>
  //        BigDecimal(innerVal.toString).bigDecimal.toPlainString
  //      case _ => innerVal.toString
  //      //        throw new RuntimeException("innerVal to jsValue failed.")
  //    }
  //  }

  //  def toInnerVal(str: String, dataType: String, version: String): InnerValLike = {
  //    //TODO:
  //    //        logger.error(s"toInnerVal: $str, $dataType, $version")
  //    val s =
  //      if (str.startsWith("\"") && str.endsWith("\"")) str.substring(1, str.length - 1)
  //      else str
  //    val dType = InnerVal.toInnerDataType(dataType)
  //
  //    dType match {
  //      case InnerVal.STRING => InnerVal.withStr(s, version)
  //      //      case t if InnerVal.NUMERICS.contains(t) => InnerVal.withNumber(BigDecimal(s), version)
  //      case InnerVal.BYTE | InnerVal.SHORT | InnerVal.INT | InnerVal.LONG | InnerVal.FLOAT | InnerVal.DOUBLE =>
  //        InnerVal.withNumber(BigDecimal(s), version)
  //      case InnerVal.BOOLEAN => InnerVal.withBoolean(s.toBoolean, version)
  //      case InnerVal.BLOB => InnerVal.withBlob(s.getBytes, version)
  //      case _ =>
  //        //        InnerVal.withStr("")
  //        throw new RuntimeException(s"illegal datatype for string: dataType is $dataType for $s")
  //    }
  //  }
  def isNumericType(dType: String): Boolean = {
    dType == InnerVal.LONG || dType == InnerVal.INT ||
      dType == InnerVal.SHORT || dType == InnerVal.BYTE ||
      dType == InnerVal.FLOAT || dType == InnerVal.DOUBLE
  }

  //TODO: fix this messy parts
  def innerValToAny(innerValLike: InnerValLike, dataType: String): Any = {
    val dType = InnerVal.toInnerDataType(dataType)
    dType match {
      case InnerVal.LONG =>
        innerValLike.value match {
          case b: BigDecimal => b.toLong
          case l: Long => l
          case i: Int => i.toLong
          case f: Float => f.toLong
          case d: Double => d.toLong
          case _ => throw new RuntimeException(s"not supported data type: $innerValLike, ${innerValLike.value.getClass}, $dataType")
        }
      case InnerVal.INT =>
        innerValLike.value match {
          case b: BigDecimal => b.toInt
          case l: Long => l.toInt
          case i: Int => i
          case f: Float => f.toInt
          case d: Double => d.toInt
          case _ => throw new RuntimeException(s"not supported data type: $innerValLike, ${innerValLike.value.getClass}, $dataType")
        }
      case InnerVal.SHORT =>
        innerValLike.value match {
          case b: BigDecimal => b.toShort
          case s: Short => s
          case _ => throw new RuntimeException(s"not supported data type: $innerValLike, ${innerValLike.value.getClass}, $dataType")
        }
      case InnerVal.BYTE =>
        innerValLike.value match {
          case b: BigDecimal => b.toByte
          case b: Byte => b
          case _ => throw new RuntimeException(s"not supported data type: $innerValLike, ${innerValLike.value.getClass}, $dataType")
        }
      case InnerVal.FLOAT =>
        innerValLike.value match {
          case b: BigDecimal => b.toFloat
          case d: Double => d.toFloat
          case f: Float => f
          case l: Long => l.toFloat
          case i: Int => i.toFloat
          case _ => throw new RuntimeException(s"not supported data type: $innerValLike, ${innerValLike.value.getClass}, $dataType")
        }
      case InnerVal.DOUBLE =>
        innerValLike.value match {
          case b: BigDecimal => b.toDouble
          case d: Double => d
          case l: Long => l.toDouble
          case i: Int => i.toDouble
          case f: Float => f.toDouble
          case _ => throw new RuntimeException(s"not supported data type: $innerValLike, ${innerValLike.value.getClass}, $dataType")
        }
      case _ => innerValLike.value
    }
  }

  def toInnerVal(any: Any, dataType: String, version: String): InnerValLike = {
    val dType = InnerVal.toInnerDataType(dataType)
    val isNumeric = isNumericType(dType)
    any match {
      case a: InnerValLike => a
      case n: BigDecimal =>
        if (isNumeric) InnerVal.withNumber(n, version)
        else if (dType == InnerVal.STRING) InnerVal.withStr(n.toString, version)
        else throw new IllegalDataTypeException(s"[ValueType] = BigDecimal, [DataType]: $dataType, [Input]: $any")
      case l: Long =>
        if (isNumeric) InnerVal.withLong(l, version)
        else if (dType == InnerVal.STRING) InnerVal.withStr(l.toString, version)
        else throw new IllegalDataTypeException(s"[ValueType] = Long, [DataType]: $dataType, [Input]: $any")
      case i: Int =>
        if (isNumeric) InnerVal.withInt(i, version)
        else if (dType == InnerVal.STRING) InnerVal.withStr(i.toString, version)
        else throw new IllegalDataTypeException(s"[ValueType] = Int, [DataType]: $dataType, [Input]: $any")
      case sh: Short =>
        if (isNumeric) InnerVal.withInt(sh.toInt, version)
        else if (dType == InnerVal.STRING) InnerVal.withStr(sh.toString, version)
        else throw new IllegalDataTypeException(s"[ValueType] = Short, [DataType]: $dataType, [Input]: $any")
      case b: Byte =>
        if (isNumeric) InnerVal.withInt(b.toInt, version)
        else if (dType == InnerVal.STRING) InnerVal.withStr(b.toString, version)
        else throw new IllegalDataTypeException(s"[ValueType] = Byte, [DataType]: $dataType, [Input]: $any")
      case f: Float =>
        if (isNumeric) InnerVal.withFloat(f, version)
        else if (dType == InnerVal.STRING) InnerVal.withStr(f.toString, version)
        else throw new IllegalDataTypeException(s"[ValueType] = Float, [DataType]: $dataType, [Input]: $any")
      case d: Double =>
        if (isNumeric) InnerVal.withDouble(d, version)
        else if (dType == InnerVal.STRING) InnerVal.withStr(d.toString, version)
        else throw new IllegalDataTypeException(s"[ValueType] = Double, [DataType]: $dataType, [Input]: $any")
      case bl: Boolean =>
        if (dType == InnerVal.BOOLEAN) InnerVal.withBoolean(bl, version)
        else if (dType == InnerVal.STRING) InnerVal.withStr(bl.toString, version)
        else throw new IllegalDataTypeException(s"[ValueType] = Boolean, [DataType]: $dataType, [Input]: $any")
      case _s: String =>
        if (isNumeric) {
          try {
            val s = TemplateHelper.replaceVariable(System.currentTimeMillis(), _s.toString)
            InnerVal.withNumber(BigDecimal(s), version)
          } catch {
            case e: Exception =>
              throw new IllegalDataTypeException(s"[ValueType] = String, [DataType]: $dataType, [Input]: $any")
          }
        } else {
          dType match {
            case InnerVal.BOOLEAN => try {
              InnerVal.withBoolean(_s.toBoolean, version)
            } catch {
              case e: Exception =>
                throw new IllegalDataTypeException(s"[ValueType] = String, [DataType]: boolean, [Input]: $any")
            }
            case InnerVal.STRING => InnerVal.withStr(_s, version)
          }
        }
    }
  }
  def jsValueToInnerVal(jsValue: JsValue, dataType: String, version: String): Option[InnerValLike] = {
    val ret = try {
      val dType = InnerVal.toInnerDataType(dataType.toLowerCase())
      jsValue match {
        case n: JsNumber =>
          dType match {
            case InnerVal.STRING => Some(InnerVal.withStr(jsValue.toString, version))
            //            case t if InnerVal.NUMERICS.contains(t) =>
            case InnerVal.BYTE | InnerVal.SHORT | InnerVal.INT | InnerVal.LONG | InnerVal.FLOAT | InnerVal.DOUBLE =>
              Some(InnerVal.withNumber(n.value, version))
            case _ => None
          }
        case _s: JsString =>
          val s = TemplateHelper.replaceVariable(System.currentTimeMillis(), _s.value)
          dType match {
            case InnerVal.STRING => Some(InnerVal.withStr(s, version))
            case InnerVal.BOOLEAN => Some(InnerVal.withBoolean(s.toBoolean, version))
            //            case t if InnerVal.NUMERICS.contains(t) =>
            case InnerVal.BYTE | InnerVal.SHORT | InnerVal.INT | InnerVal.LONG | InnerVal.FLOAT | InnerVal.DOUBLE =>
              Some(InnerVal.withNumber(BigDecimal(s), version))
            case _ => None
          }
        case b: JsBoolean =>
          dType match {
            case InnerVal.STRING => Some(InnerVal.withStr(b.toString, version))
            case InnerVal.BOOLEAN => Some(InnerVal.withBoolean(b.value, version))
            case _ => None
          }
        case _ =>
          None
      }
    } catch {
      case e: Exception =>
        logger.error(s"jsValueToInnerVal: jsValue = ${jsValue}, dataType = ${dataType}, version = ${version}", e)
        None
    }

    ret
  }

  def anyValToJsValue(value: Any): Option[JsValue] = {
    try {
      val v = value match {
        case null => JsNull
        case l: Long => JsNumber(l)
        case i: Int => JsNumber(i)
        case s: Short => JsNumber(s.toInt)
        case b: Byte => JsNumber(b.toInt)
        case f: Float => JsNumber(f.toDouble)
        case d: Double => JsNumber(d)
        case bd: BigDecimal => if (bd.isValidLong) JsNumber(bd.toLong) else JsNumber(bd)
        case s: String => JsString(s)
        case b: Boolean => JsBoolean(b)
        case _ => throw new RuntimeException(s"$value, ${value.getClass.getName} is not supported data type.")
      }
      Option(v)
    } catch {
      case e: Exception =>
        logger.error(s"anyValToJsValue: $value", e)
        None
    }
  }

  def jsValueToAny(value: JsValue): Option[AnyRef] = {
    try {
      val v = value match {
//        case JsNull =>
        case n: JsNumber => n.value
        case s: JsString => TemplateHelper.replaceVariable(System.currentTimeMillis(), s.value)
        case b: JsBoolean => Boolean.box(b.value)
      }
      Option(v)
    } catch {
      case e: Exception =>
        logger.error(s"jsValueToAny: $value", e)
        None
    }
  }

  def propertiesToJson(props: Map[String, Any],
                       selectColumns: Map[String, Boolean] = Map.empty): Map[String, JsValue] = {
    if (selectColumns.isEmpty) {
      for {
        (k, v) <- props
        jsValue <- anyValToJsValue(v)
      //      labelMeta <- label.metaPropsInvMap.get(k)
      //      innerVal = toInnerVal(v.toString, labelMeta.dataType, labelMeta.)
      } yield {
        k -> jsValue
      }
    } else {
      for {
        (k, _) <- selectColumns
        v <- props.get(k)
        jsValue <- anyValToJsValue(v)
      } yield k -> jsValue
    }
  }

  def jsValueToString(jsValue: JsValue): String = {
    jsValue match {
      case s: JsString => s.value
      case _ => jsValue.toString
    }
  }
  def fromJsonToProperties(jsObject: JsObject): Map[String, Any] = {
    val kvs = for {
      (k, v) <- jsObject.fieldSet
    } yield {
        k -> jsValueToString(v)
      }
    kvs.toMap
  }
}
