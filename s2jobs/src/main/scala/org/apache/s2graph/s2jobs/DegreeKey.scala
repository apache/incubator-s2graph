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

import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.hadoop.hbase.{KeyValue => HKeyValue}
import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.types.{InnerValLikeWithTs, SourceVertexId}
import play.api.libs.json.Json


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

  def toEdge(s2: S2Graph, degreeKey: DegreeKey, count: Long): S2EdgeLike = {
    val labelName = degreeKey.labelName
    val direction = degreeKey.direction
    val vertexId = degreeKey.vertexIdStr
    val label = Label.findByName(labelName).getOrElse(throw new RuntimeException(s"$labelName is not found in DB."))
    val dir = GraphUtil.directions(direction)
    val innerVal = JSONParser.jsValueToInnerVal(Json.toJson(vertexId), label.srcColumnWithDir(dir).columnType, label.schemaVersion).getOrElse {
      throw new RuntimeException(s"$vertexId can not be converted into innerval")
    }
    val vertex = s2.elementBuilder.newVertex(SourceVertexId(label.srcColumn, innerVal))

    val ts = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timestamp -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion),
      LabelMeta.degree -> InnerValLikeWithTs.withLong(count, ts, label.schemaVersion))

    s2.elementBuilder.newEdge(vertex, vertex, label, dir, propsWithTs = propsWithTs)
  }

  def toSKeyValue(s2: S2Graph,
                  degreeKey: DegreeKey,
                  count: Long): Seq[SKeyValue] = {
    try {
      val edge = toEdge(s2, degreeKey, count)
      edge.edgesWithIndex.flatMap { indexEdge =>
        s2.getStorage(indexEdge.label).serDe.indexEdgeSerializer(indexEdge).toKeyValues
      }
    } catch {
      case e: Exception => Nil
    }
  }

  def toKeyValue(s2: S2Graph, degreeKey: DegreeKey, count: Long): Seq[HKeyValue] = {
    toSKeyValue(s2, degreeKey, count).map(skv => new HKeyValue(skv.row, skv.cf, skv.qualifier, skv.timestamp, skv.value))
  }
}

case class DegreeKey(vertexIdStr: String, labelName: String, direction: String)
