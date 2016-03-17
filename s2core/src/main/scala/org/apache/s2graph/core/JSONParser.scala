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

import org.apache.s2graph.core.types.{InnerVal, InnerValLike}
import org.apache.s2graph.core.utils.logger
import play.api.libs.json._


trait JSONParser {

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

  def toInnerVal(str: String, dataType: String, version: String): InnerValLike = {
    //TODO:
    //        logger.error(s"toInnerVal: $str, $dataType, $version")
    val s =
      if (str.startsWith("\"") && str.endsWith("\"")) str.substring(1, str.length - 1)
      else str
    val dType = InnerVal.toInnerDataType(dataType)

    dType match {
      case InnerVal.STRING => InnerVal.withStr(s, version)
      //      case t if InnerVal.NUMERICS.contains(t) => InnerVal.withNumber(BigDecimal(s), version)
      case InnerVal.BYTE | InnerVal.SHORT | InnerVal.INT | InnerVal.LONG | InnerVal.FLOAT | InnerVal.DOUBLE =>
        InnerVal.withNumber(BigDecimal(s), version)
      case InnerVal.BOOLEAN => InnerVal.withBoolean(s.toBoolean, version)
      case InnerVal.BLOB => InnerVal.withBlob(s.getBytes, version)
      case _ =>
        //        InnerVal.withStr("")
        throw new RuntimeException(s"illegal datatype for string: dataType is $dataType for $s")
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
        case s: JsString =>
          dType match {
            case InnerVal.STRING => Some(InnerVal.withStr(s.value, version))
            case InnerVal.BOOLEAN => Some(InnerVal.withBoolean(s.as[String].toBoolean, version))
            //            case t if InnerVal.NUMERICS.contains(t) =>
            case InnerVal.BYTE | InnerVal.SHORT | InnerVal.INT | InnerVal.LONG | InnerVal.FLOAT | InnerVal.DOUBLE =>
              Some(InnerVal.withNumber(BigDecimal(s.value), version))
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
        logger.error(e.getMessage)
        None
    }

    ret
  }
}
