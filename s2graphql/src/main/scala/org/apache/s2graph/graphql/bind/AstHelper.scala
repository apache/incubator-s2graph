package org.apache.s2graph.graphql.bind

object AstHelper {
  def selectedFields(astFields: Seq[sangria.ast.Field]): Vector[String] = {
    astFields.flatMap { f =>
      f.selections.map(s => s.asInstanceOf[sangria.ast.Field].name)
    }.toVector
  }
}
