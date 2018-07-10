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

package org.apache.s2graph.graphql

import org.apache.s2graph.graphql.repository.GraphRepository
import sangria.schema._

package object types {

  def wrapField(objectName: String, fieldName: String, fields: Seq[Field[GraphRepository, Any]]): Field[GraphRepository, Any] = {
    val tpe = ObjectType(objectName, fields = fields.toList)
    Field(fieldName, tpe, resolve = c => c.value): Field[GraphRepository, Any]
  }

  def toScalarType(from: String): ScalarType[_] = from match {
    case "string" => StringType
    case "int" | "integer" => IntType
    case "long" => LongType
    case "float" | "double" => FloatType
    case "bool" | "boolean" => BooleanType
  }

  val validateRegEx = "/^[_a-zA-Z][_a-zA-Z0-9]*$/.".r

  val ints = (0 to 9).map(_.toString).toSet

  implicit class StringOps(val s: String) extends AnyVal {
    def toValidName: String = {
      val newS = s
        .replaceAll("-", "_HYPHEN_")
        .replaceAll("-", "_DASH_")
        .replaceAll(":", "_COLON_")
        .replaceAll(" ", "_SPACE_")
        .trim

      if (ints.contains(newS.head.toString)) s"NUMBER_${newS}"
      else newS
    }
  }

}
