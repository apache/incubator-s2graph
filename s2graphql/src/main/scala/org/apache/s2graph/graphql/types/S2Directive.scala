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

import sangria.schema.{Argument, Directive, DirectiveLocation, StringType}

object S2Directive {

  object Eval {

    import scala.collection._

    val codeMap = mutable.Map.empty[String, () => Any]

    def compileCode(code: String): () => Any = {
      import scala.tools.reflect.ToolBox
      val toolbox = reflect.runtime.currentMirror.mkToolBox()

      toolbox.compile(toolbox.parse(code))
    }

    def getCompiledCode[T](code: String): T = {
      val compiledCode = Eval.codeMap.getOrElseUpdate(code, Eval.compileCode(code))

      compiledCode
        .asInstanceOf[() => Any]
        .apply()
        .asInstanceOf[T]
    }
  }

  type TransformFunc = String => String

  val funcArg = Argument("func", StringType)

  val Transform =
    Directive("transform",
      arguments = List(funcArg),
      locations = Set(DirectiveLocation.Field),
      shouldInclude = _ => true)

  def resolveTransform(code: String, input: String): String = {
    try {
      val fn = Eval.getCompiledCode[TransformFunc](code)

      fn.apply(input)
    } catch {
      case e: Exception => e.toString
    }
  }
}
