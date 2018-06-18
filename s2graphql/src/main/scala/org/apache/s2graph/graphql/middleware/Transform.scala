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

package org.apache.s2graph.graphql.middleware

import org.apache.s2graph.graphql.types.S2Directive
import sangria.ast.StringValue
import sangria.execution.{Middleware, MiddlewareAfterField, MiddlewareQueryContext}
import sangria.schema.Context

case class Transform() extends Middleware[Any] with MiddlewareAfterField[Any] {
  type QueryVal = Unit
  type FieldVal = Unit

  def beforeQuery(context: MiddlewareQueryContext[Any, _, _]) = ()

  def afterQuery(queryVal: QueryVal, context: MiddlewareQueryContext[Any, _, _]) = ()

  def beforeField(cache: QueryVal, mctx: MiddlewareQueryContext[Any, _, _], ctx: Context[Any, _]) = continue

  def afterField(cache: QueryVal, fromCache: FieldVal, value: Any, mctx: MiddlewareQueryContext[Any, _, _], ctx: Context[Any, _]) = {

    value match {
      case s: String => {
        val transformFuncOpt = ctx.astFields.headOption.flatMap(_.directives.find(_.name == S2Directive.Transform.name)).map { directive =>
          directive.arguments.head.value.asInstanceOf[StringValue].value
        }

        transformFuncOpt.map(funcString => S2Directive.resolveTransform(funcString, s)).orElse(Option(s))
      }

      case _ ⇒ None
    }
  }
}
