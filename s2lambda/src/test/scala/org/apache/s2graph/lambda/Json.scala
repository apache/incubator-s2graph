package org.apache.s2graph.lambda

object Json {
  def get(name: String) = scala.io.Source.fromInputStream(getClass.getResourceAsStream(s"/$name")).getLines.mkString("")
}
