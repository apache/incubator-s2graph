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
