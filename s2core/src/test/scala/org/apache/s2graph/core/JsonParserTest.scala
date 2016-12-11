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

import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.types.{InnerVal, InnerValLike}
import org.scalatest.{FunSuite, Matchers}


class JsonParserTest extends FunSuite with Matchers with TestCommon {

  import InnerVal._
  import types.HBaseType._

  val innerValsPerVersion = for {
    version <- ValidVersions
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
