package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.types2.{InnerValLike, InnerVal}
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/30/15.
 */
class JsonParserTest extends FunSuite with Matchers with TestCommon with JSONParser {

  import types2.HBaseType._
  import InnerVal._

  val innerValsPerVersion = for {
    version <- List(VERSION2, VERSION1)
  } yield {
      val innerVals = List(
        (InnerVal.withStr("ABC123", version), STRING),
        (InnerVal.withNumber(23, version), BYTE),
        (InnerVal.withNumber(23, version), INT),
        (InnerVal.withNumber(Int.MaxValue, version), INT),
        (InnerVal.withNumber(Int.MinValue, version), INT),
        (InnerVal.withNumber(Long.MaxValue, version), LONG),
        (InnerVal.withNumber(Long.MinValue, version), LONG),
        (InnerVal.withBoolean(true, version), BOOLEAN)
      )
      val doubleVals = if (version == VERSION2) {
        List(
          (InnerVal.withDouble(Double.MaxValue, version), DOUBLE),
          (InnerVal.withDouble(Double.MinValue, version), DOUBLE),
          (InnerVal.withDouble(0.1, version), DOUBLE),
          (InnerVal.withFloat(Float.MinValue, version), FLOAT),
          (InnerVal.withFloat(Float.MaxValue, version), FLOAT),
          (InnerVal.withFloat(0.9f, version), FLOAT)
        )
      } else {
        List.empty[(InnerValLike, String)]
      }
      (innerVals ++ doubleVals, version)
    }

  def testInnerValToJsValue(innerValWithDataTypes: Seq[(InnerValLike, String)],
                            version: String) = {
    for {
      (innerVal, dataType) <- innerValWithDataTypes
    } {
      val jsValueOpt = innerValToJsValue(innerVal, dataType)
      val decodedOpt = jsValueOpt.flatMap { jsValue =>
        jsValueToInnerVal(jsValue, dataType, version)
      }
      println(s"jsValue: $jsValueOpt")
      println(s"innerVal: $decodedOpt")
      (decodedOpt.isDefined && innerVal == decodedOpt.get) shouldBe true
    }
  }

  test("aa") {
    val innerVal = InnerVal.withStr("abc", VERSION2)
    val tmp = innerValToJsValue(innerVal, "string")
    println(tmp)
  }
  test("JsValue <-> InnerVal with dataType") {
    for {
      (innerValWithDataTypes, version) <- innerValsPerVersion
    } {
      testInnerValToJsValue(innerValWithDataTypes, version)
    }
  }
}
