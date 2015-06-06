package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.types2.{InnerValLike, InnerVal}
import play.api.libs.json._


trait JSONParser {

  def innerValToJsValue(innerVal: InnerValLike, dataType: String): Option[JsValue] = {
    try {
      val jsValue = dataType match {
        case InnerVal.STRING => JsString(innerVal.value.toString)
        case InnerVal.BOOLEAN => JsBoolean(innerVal.value.asInstanceOf[Boolean])
        case t if InnerVal.NUMERICS.contains(t) =>
          JsNumber(InnerVal.scaleNumber(innerVal.value.asInstanceOf[BigDecimal], dataType))
        case _ =>
          throw new RuntimeException(s"innerVal $innerVal to JsValue with type $dataType")
      }
      Some(jsValue)
    } catch {
      case e: Throwable =>
        None
    }
  }
  def innerValToString(innerVal: InnerValLike, dataType: String): String = {
    dataType match {
      case InnerVal.STRING => innerVal.toString
      case InnerVal.BOOLEAN => innerVal.toString
      case t if InnerVal.NUMERICS.contains(t)  => innerVal.value.asInstanceOf[BigDecimal].bigDecimal.toPlainString
      case _ =>  innerVal.toString
//        throw new RuntimeException("innerVal to jsValue failed.")
    }
  }

  def toInnerVal(s: String, dataType: String, version: String) = {
    dataType match {
      case InnerVal.STRING => InnerVal.withStr(s, version)
      case t if InnerVal.NUMERICS.contains(t) => InnerVal.withNumber(BigDecimal(s), version)
      case InnerVal.BOOLEAN => InnerVal.withBoolean(s.toBoolean, version)
      case InnerVal.BLOB => InnerVal.withBlob(s.getBytes, version)
      case _ =>
        //        InnerVal.withStr("")
        throw new RuntimeException(s"illegal datatype for string: dataType is $dataType for $s")
    }
  }


  def jsValueToInnerVal(jsValue: JsValue, dataType: String, version: String): Option[InnerValLike] = {
    val ret = try {
      val dType = dataType.toLowerCase()
      jsValue match {
        case n: JsNumber =>
          dType match {
            case InnerVal.STRING => Some(InnerVal.withStr(jsValue.toString, version))
            case t if InnerVal.NUMERICS.contains(t) => Some(InnerVal.withNumber(n.value, version))
            case _ => None
          }
        case s: JsString =>
          dType match {
            case InnerVal.STRING => Some(InnerVal.withStr(s.value, version))
            case InnerVal.BOOLEAN => Some(InnerVal.withBoolean(s.as[String].toBoolean, version))
            case t if InnerVal.NUMERICS.contains(t) => Some(InnerVal.withNumber(BigDecimal(s.value), version))
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
      case e: Throwable =>
        None
    }

    ret
  }
}