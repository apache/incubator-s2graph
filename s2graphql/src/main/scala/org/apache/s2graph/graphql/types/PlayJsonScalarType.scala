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

package org.apache.s2graph.graphql.types

import play.api.libs.json._
import sangria.ast
import sangria.execution.Executor
import sangria.marshalling.{ArrayMapBuilder, InputUnmarshaller, ResultMarshaller, ScalarValueInfo}
import sangria.schema._
import sangria.validation.{BigIntCoercionViolation, IntCoercionViolation, ValueCoercionViolation}
import sangria.macros._

import scala.concurrent.ExecutionContext.Implicits.global

object PlayJsonScalarType {

  implicit object CustomPlayJsonResultMarshaller extends ResultMarshaller {
    type Node = JsValue
    type MapBuilder = ArrayMapBuilder[Node]

    def emptyMapNode(keys: Seq[String]) = new ArrayMapBuilder[Node](keys)

    def addMapNodeElem(builder: MapBuilder, key: String, value: Node, optional: Boolean) = builder.add(key, value)

    def mapNode(builder: MapBuilder) = JsObject(builder.toMap)

    def mapNode(keyValues: Seq[(String, JsValue)]) = Json.toJson(keyValues.toMap)

    def arrayNode(values: Vector[JsValue]) = JsArray(values)

    def optionalArrayNodeValue(value: Option[JsValue]) = value match {
      case Some(v) ⇒ v
      case None ⇒ nullNode
    }

    def scalarNode(value: Any, typeName: String, info: Set[ScalarValueInfo]) = value match {
      case v: String ⇒ JsString(v)
      case v: Boolean ⇒ JsBoolean(v)
      case v: Int ⇒ JsNumber(v)
      case v: Long ⇒ JsNumber(v)
      case v: Float ⇒ JsNumber(BigDecimal(v))
      case v: Double ⇒ JsNumber(v)
      case v: BigInt ⇒ JsNumber(BigDecimal(v))
      case v: BigDecimal ⇒ JsNumber(v)
      case v: JsValue ⇒ v
      case v ⇒ throw new IllegalArgumentException("Unsupported scalar value: " + v)
    }

    def enumNode(value: String, typeName: String) = JsString(value)

    def nullNode = JsNull

    def renderCompact(node: JsValue) = Json.stringify(node)

    def renderPretty(node: JsValue) = Json.prettyPrint(node)
  }

  implicit object PlayJsonInputUnmarshaller extends InputUnmarshaller[JsValue] {
    def getRootMapValue(node: JsValue, key: String) = node.asInstanceOf[JsObject].value get key

    def isListNode(node: JsValue) = node.isInstanceOf[JsArray]

    def getListValue(node: JsValue) = node.asInstanceOf[JsArray].value

    def isMapNode(node: JsValue) = node.isInstanceOf[JsObject]

    def getMapValue(node: JsValue, key: String) = node.asInstanceOf[JsObject].value get key

    def getMapKeys(node: JsValue) = node.asInstanceOf[JsObject].fields.map(_._1)

    def isDefined(node: JsValue) = node != JsNull

    def getScalarValue(node: JsValue) = node match {
      case JsBoolean(b) ⇒ b
      case JsNumber(d) ⇒ d.toBigIntExact getOrElse d
      case JsString(s) ⇒ s
      case n ⇒ n
    }

    def getScalaScalarValue(node: JsValue) = getScalarValue(node)

    def isEnumNode(node: JsValue) = node.isInstanceOf[JsString]

    def isScalarNode(node: JsValue) = true

    def isVariableNode(node: JsValue) = false

    def getVariableName(node: JsValue) = throw new IllegalArgumentException("variables are not supported")

    def render(node: JsValue) = Json.stringify(node)
  }

  case object JsonCoercionViolation extends ValueCoercionViolation("Not valid JSON")

  implicit val JsonType = ScalarType[JsValue]("Json",
    description = Some("Raw PlayJson value"),
    coerceOutput = (value, _) ⇒ value,
    coerceUserInput = {
      case v: String ⇒ Right(JsString(v))
      case v: Boolean ⇒ Right(JsBoolean(v))
      case v: Int ⇒ Right(JsNumber(v))
      case v: Long ⇒ Right(JsNumber(v))
      case v: Float ⇒ Right(JsNumber(BigDecimal(v)))
      case v: Double ⇒ Right(JsNumber(v))
      case v: BigInt ⇒ Right(JsNumber(BigDecimal(v)))
      case v: BigDecimal ⇒ Right(JsNumber(v))
      case v: JsValue ⇒ Right(v)
    },
    coerceInput = {
      case sv: ast.StringValue => Right(Json.parse(sv.value))
      case _ ⇒
        Left(JsonCoercionViolation)
    })
}
