package org.apache.s2graph.core.tinkerpop

import java.lang.annotation.Annotation

import org.apache.tinkerpop.gremlin.LoadGraphWith
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData

class S2GraphData extends LoadGraphWith {
  override def value(): GraphData = ???

  override def annotationType(): Class[_ <: Annotation] = classOf[LoadGraphWith]
}
