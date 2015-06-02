package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.models.{HLabelMeta, HLabel}
import com.daumkakao.s2graph.core.types.InnerVal
import play.api.libs.json._

import scala.util.parsing.combinator.JavaTokenParsers

trait JSONParser {
  /** */
//  def toInnerVal(jsValue: JsValue) = {
//    jsValue match {
//      case n: JsNumber => (InnerVal.withNumber(n.value), InnerVal.dataTypeOfNumber(n.value))
//      case s: JsString => (InnerVal.withStr(s.value), InnerVal.STRING)
//      case b: JsBoolean => (InnerVal.withBoolean(b.value), InnerVal.BOOLEAN)
//      // ?? blob??
//      case _ => throw new Exception("JsonValue should be in [long/string/boolean].")
//    }
//  }
//  def innerValToJsValue(innerVal: InnerVal): JsValue = {
//    innerVal.toJsValue()
//  }
  def innerValToJsValue(innerVal: InnerVal, dataType: String): Option[JsValue] = {
    try {
      val jsValue = dataType match {
        case InnerVal.STRING => JsString(innerVal.value.toString)
        case InnerVal.BOOLEAN => JsBoolean(innerVal.toVal[Boolean])
        case t if InnerVal.NUMERICS.contains(t) =>
          JsNumber(InnerVal.scaleNumber(innerVal.toVal[BigDecimal], dataType))
        case _ =>
          throw new RuntimeException(s"innerVal $innerVal to JsValue with type $dataType")
      }
      Some(jsValue)
    } catch {
      case e: Throwable =>
        None
    }
  }
  def innerValToString(innerVal: InnerVal, dataType: String): String = {
    dataType match {
      case InnerVal.STRING => innerVal.value.toString
      case InnerVal.BOOLEAN => innerVal.value.toString
      case t if InnerVal.NUMERICS.contains(t) => innerVal.toVal[BigDecimal].bigDecimal.toPlainString
      case _ => throw new RuntimeException("innerVal to jsValue failed.")
    }
  }

  def toInnerVal(s: String, dataType: String) = {
    dataType match {
      case InnerVal.STRING => InnerVal.withStr(s)
      case t if InnerVal.NUMERICS.contains(t) => InnerVal.withNumber(BigDecimal(s))
      case InnerVal.BOOLEAN => InnerVal.withBoolean(s.toBoolean)
      case InnerVal.BLOB => InnerVal.withBlob(s.getBytes)
      case _ =>
        //        InnerVal.withStr("")
        throw new RuntimeException(s"illegal datatype for string: dataType is $dataType for $s")
    }
  }


  def jsValueToInnerVal(jsValue: JsValue, dataType: String): Option[InnerVal] = {
    val ret = try {
      val dType = dataType.toLowerCase()
      jsValue match {
        case n: JsNumber =>
          dType match {
            case InnerVal.STRING => Some(InnerVal.withStr(jsValue.toString))
            case t if InnerVal.NUMERICS.contains(t) => Some(InnerVal.withNumber(n.value))
            case _ => None
          }
        case s: JsString =>
          dType match {
            case InnerVal.STRING => Some(InnerVal.withStr(s.value))
            case InnerVal.BOOLEAN => Some(InnerVal.withBoolean(s.as[String].toBoolean))
            case t if InnerVal.NUMERICS.contains(t) => Some(InnerVal.withNumber(BigDecimal(s.value)))
            case _ => None
          }
        case b: JsBoolean =>
          dType match {
            case InnerVal.STRING => Some(InnerVal.withStr(b.toString))
            case InnerVal.BOOLEAN => Some(InnerVal.withBoolean(b.value))
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
//  def innerValToString(innerVal: InnerVal, dataType: String): String = {
//    val value = innerVal.value
//    dataType.toLowerCase() match {
//      case InnerVal.STRING => JsString(value.toString).toString
//      case _ => value.toString
//    }
//  }

}