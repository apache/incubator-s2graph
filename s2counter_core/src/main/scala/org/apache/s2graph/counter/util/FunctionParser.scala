package org.apache.s2graph.counter.util

object FunctionParser {
  val funcRe = """([a-zA-Z_]+)(\((\d+)?\))?""".r

  def apply(str: String): Option[(String, String)] = {
    str match {
      case funcRe(funcName, funcParam, funcArg) =>
        funcName match {
          case x: String =>
            Some((funcName, funcArg match {
              case x: String => funcArg
              case null => ""
            }))
          case null => None
        }
      case _ =>
        None
    }
  }
}
