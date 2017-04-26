package org.apache.s2graph.core.features

import java.util
import java.util.Optional
import scala.collection.JavaConversions._
import org.apache.tinkerpop.gremlin.structure.Graph

class S2GraphVariables extends Graph.Variables {
  import scala.collection.mutable
  private val variables = mutable.Map.empty[String, Any]

  override def set(key: String, value: scala.Any): Unit = {
    if (key == null) throw Graph.Variables.Exceptions.variableKeyCanNotBeNull()
    if (key.isEmpty) throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty()
    if (value == null) throw Graph.Variables.Exceptions.variableValueCanNotBeNull()

    variables.put(key, value)
  }

  override def keys(): util.Set[String] = variables.keySet

  override def remove(key: String): Unit = {
    if (key == null) throw Graph.Variables.Exceptions.variableKeyCanNotBeNull()
    if (key.isEmpty) throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty()

    variables.remove(key)
  }

  override def get[R](key: String): Optional[R] = {
    if (key == null) throw Graph.Variables.Exceptions.variableKeyCanNotBeNull()
    if (key.isEmpty) throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty()
    variables.get(key) match {
      case None => Optional.empty()
      case Some(value) => if (value == null) Optional.empty() else Optional.of(value.asInstanceOf[R])
    }
  }

  override def toString: String = {
    s"variables[size:${variables.keys.size()}]"
  }
}