///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.s2graph.graphql.types
//
//import sangria.ast._
//import sangria.validation.ValueCoercionViolation
//
//// https://gist.github.com/OlegIlyenko/5b96f4b54f656aac226d3c4bc33fd2a6
//
//object PlayJsonPolyType {
//
//  import play.api.libs.json._
//  import sangria.ast
//  import sangria.schema._
//
//  case object JsonCoercionViolation extends ValueCoercionViolation("Not valid JSON")
//
//  def scalarTypeToJsValue(v: sangria.ast.Value): JsValue = v match {
//    case v: IntValue => JsNumber(v.value)
//    case v: BigIntValue => JsNumber(BigDecimal(v.value.bigInteger))
//    case v: FloatValue => JsNumber(v.value)
//    case v: BigDecimalValue => JsNumber(v.value)
//    case v: StringValue => JsString(v.value)
//    case v: BooleanValue => JsBoolean(v.value)
//    case v: ListValue => JsNull
//    case v: VariableValue => JsNull
//    case v: NullValue => JsNull
//    case v: ObjectValue => JsNull
//    case _ => throw new RuntimeException("Error!")
//  }
//
//  implicit val PolyType = ScalarType[JsValue]("Poly",
//    description = Some("Type Poly = String | Number | Boolean"),
//    coerceOutput = (value, _) ⇒ value match {
//      case JsString(s) => s
//      case JsNumber(n) => n
//      case JsBoolean(b) => b
//      case JsNull => null
//      case _ => value
//    },
//    coerceUserInput = {
//      case v: String => Right(JsString(v))
//      case v: Boolean => Right(JsBoolean(v))
//      case v: Int => Right(JsNumber(v))
//      case v: Long => Right(JsNumber(v))
//      case v: Float => Right(JsNumber(v.toDouble))
//      case v: Double => Right(JsNumber(v))
//      case v: BigInt => Right(JsNumber(BigDecimal(v)))
//      case v: BigDecimal => Right(JsNumber(v))
//      case _ => Left(JsonCoercionViolation)
//    },
//    coerceInput = {
//      case value: ast.StringValue => Right(JsString(value.value))
//      case value: ast.IntValue => Right(JsNumber(value.value))
//      case value: ast.FloatValue => Right(JsNumber(value.value))
//      case value: ast.BigIntValue => Right(JsNumber(BigDecimal(value.value.bigInteger)))
//      case _ => Left(JsonCoercionViolation)
//    })
//}
