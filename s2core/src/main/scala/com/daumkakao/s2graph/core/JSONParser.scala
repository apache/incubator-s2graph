package com.daumkakao.s2graph.core

import play.api.libs.json._
import HBaseElement.InnerVal

trait JSONParser {

  def innerValToJsValue(innerVal: InnerVal): JsValue = {
    (innerVal.longV, innerVal.strV, innerVal.boolV) match {
      case (Some(l), None, None) => new JsNumber(l)
      case (None, Some(s), None) => new JsString(s)
      case (None, None, Some(b)) => new JsBoolean(b)
      case _ => throw new Exception(s"InnerVal should be [long/integeer/short/byte/string/boolean]")
    }
  }

  def toInnerVal(s: String, dataType: String) = {
    dataType match {
      case "string" | "str" => InnerVal.withStr(s)
//      InnerVal.withStr(s.replaceAll ("[\"]", ""))
      case "long" | "integer" | "int" => InnerVal.withLong(s.toLong)
      case "boolean" | "bool" => InnerVal.withBoolean(s.toBoolean)
      case _ =>
        //        InnerVal.withStr("")
        throw new RuntimeException(s"illegal datatype for string: dataType is $dataType for $s")
    }
  }

  def toInnerVal(jsValue: JsValue) = {
    jsValue match {
      case n: JsNumber => (InnerVal.withLong(n.as[BigDecimal].toLong), "long")
      case s: JsString => (InnerVal.withStr(s.as[String]), "string")
      case b: JsBoolean => (InnerVal.withBoolean(b.as[Boolean]), "boolean")
      case _ => throw new Exception("JsonValue should be in [long/string/boolean].")
    }
  }
  def jsValueToInnerVal(jsValue: JsValue, dataType: String): Option[InnerVal] = {
    val ret = try {
      jsValue match {
        case n: JsNumber =>
          val dType = dataType.toLowerCase()
          dType match {
            case "string" | "str" => Some(InnerVal.withStr(jsValue.toString))
            case "boolean" | "bool" => None
            case "long" | "integer" | "int" => Some(InnerVal.withLong(n.as[Long]))
            case _ => None
          }
        case s: JsString =>
          dataType.toLowerCase() match {
            case "string" => Some(InnerVal.withStr(s.as[String]))
            case "boolean" => Some(InnerVal.withBoolean(s.as[String].toBoolean))
            case "long" | "integer" | "int" => Some(InnerVal.withLong(s.as[String].toLong))
            case _ => None
          }
        case b: JsBoolean =>
          dataType.toLowerCase() match {
            case "string" => Some(InnerVal.withStr(b.toString))
            case "boolean" => Some(InnerVal.withBoolean(b.as[Boolean]))
            case "long" | "integer" | "int" => None
            case _ => None
          }
        case _ =>
          None
      }
    } catch {
      case e: Throwable =>
        None
    }

    ret
  }
  def innerValToString(innerVal: InnerVal, dataType: String): String = {
    val value = innerVal.value
    dataType.toLowerCase() match {
      case "string" | "str" => JsString(value.toString).toString
      case _ => value.toString
    }
  }
}