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
