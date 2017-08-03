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

package org.apache.s2graph.core.tinkerpop.groovy.plugin

import java.util
import org.apache.tinkerpop.gremlin.groovy.plugin.{AbstractGremlinPlugin, PluginAcceptor}

object S2GraphGremlinPlugin{
  lazy val IMPORTS = {
    val hashSet = new util.HashSet[String]()
    hashSet.add("import org.apache.s2graph.core.*")
    hashSet.add("import org.apache.s2graph.core.S2Graph.*")
    hashSet.add("import org.apache.s2graph.core.Management.JsonModel.*")
    hashSet.add("import org.apache.s2graph.core.S2GraphFactory")
    hashSet.add("import org.apache.s2graph.core.S2GraphFactory.*")
    hashSet.add("import org.apache.s2graph.core.mysqls.*")
    hashSet.add("import org.apache.s2graph.core.index.*")
    hashSet.add("import org.apache.s2graph.core.features.*")
    hashSet.add("import org.apache.s2graph.core.io.*")
    hashSet.add("import org.apache.s2graph.core.parsers.*")
    hashSet.add("import org.apache.s2graph.core.rest.*")
    hashSet.add("import org.apache.s2graph.core.utils.*")
    hashSet.add("import org.apache.s2graph.core.types.*")

    hashSet
  }
}
class S2GraphGremlinPlugin extends AbstractGremlinPlugin{

  override def pluginTo(pluginAcceptor: PluginAcceptor): Unit = {
    pluginAcceptor.addImports(S2GraphGremlinPlugin.IMPORTS)
  }


  override def afterPluginTo(pluginAcceptor: PluginAcceptor): Unit = {

  }

  override def getName() : String = "tinkerpop.s2graph"

  override def requireRestart() : Boolean = true
}
