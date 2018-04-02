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

package org.apache.s2graph.s2jobs

import org.apache.s2graph.core.{GraphElement, S2Edge, S2Graph, S2Vertex}
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.hadoop.hbase.{KeyValue => HKeyValue}


object DegreeKey {
  def fromGraphElement(s2: S2Graph,
                       element: GraphElement,
                       labelMapping: Map[String, String] = Map.empty): Option[DegreeKey] = {
    element match {
      case v: S2Vertex => None
      case e: S2Edge =>
        val newLabel = labelMapping.getOrElse(e.innerLabel.label, e.innerLabel.label)
        val degreeKey = DegreeKey(e.srcVertex.innerIdVal.toString, newLabel, e.getDirection())
        Option(degreeKey)
      case _ => None
    }
  }

  def toSKeyValue(s2: S2Graph,
                  degreeKey: DegreeKey,
                  count: Long): Seq[SKeyValue] = {
    S2GraphHelper.buildDegreePutRequests(s2, degreeKey.vertexIdStr, degreeKey.labelName, degreeKey.direction, count)
  }

  def toKeyValue(s2: S2Graph, degreeKey: DegreeKey, count: Long): Seq[HKeyValue] = {
    toSKeyValue(s2, degreeKey, count).map(skv => new HKeyValue(skv.row, skv.cf, skv.qualifier, skv.timestamp, skv.value))
  }
}

case class DegreeKey(vertexIdStr: String, labelName: String, direction: String)
