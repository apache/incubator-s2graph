package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.types.InnerVal
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/30/15.
 */
class JsonParserTest extends FunSuite with Matchers with TestCommon with JSONParser {
  val innerValWithDataTypes = List(
    (InnerVal.withStr("ABC123"), InnerVal.STRING),
    (InnerVal.withNumber(23), InnerVal.BYTE),
    (InnerVal.withNumber(23), InnerVal.INT),
    (InnerVal.withNumber(Int.MaxValue), InnerVal.INT),
    (InnerVal.withNumber(Int.MinValue), InnerVal.INT),
    (InnerVal.withNumber(Long.MaxValue), InnerVal.LONG),
    (InnerVal.withNumber(Long.MinValue), InnerVal.LONG),
    (InnerVal.withBoolean(true), InnerVal.BOOLEAN)
  )

  def testInnerValToJsValue(innerValWithDataTypes: Seq[(InnerVal, String)]) = {
    val rets = for {
      (innerVal, dataType) <- innerValWithDataTypes
    } yield {
        val jsValueOpt = innerValToJsValue(innerVal, dataType)
        val decodedOpt = jsValueOpt.map { jsValue =>
          jsValueToInnerVal(jsValue, dataType)
        }
        jsValueOpt.isDefined && decodedOpt.isDefined &&
          innerVal == decodedOpt.get
      }
    rets.foreach(x => x)
  }

  test("JsValue <-> InnerVal with dataType") {
    testInnerValToJsValue(innerValWithDataTypes)
  }
}
