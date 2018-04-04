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

package org.apache.s2graph.s2jobs.serde.writer

import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.DegreeKey
import org.apache.s2graph.s2jobs.serde.GraphElementWritable

object GraphElementDataFrameWriter {
  type GraphElementTuple = (Long, String, String, String, String, String, String, String)
  val Fields = Seq("timestamp", "operation", "element", "from", "to", "label", "props", "direction")
}

class GraphElementDataFrameWriter extends GraphElementWritable[GraphElementDataFrameWriter.GraphElementTuple] {
  import GraphElementDataFrameWriter._
  private def toGraphElementTuple(tokens: Array[String]): GraphElementTuple = {
    tokens match {
      case Array(ts, op, elem, from, to, label, props, dir) => (ts.toLong, op, elem, from, to, label, props, dir)
      case Array(ts, op, elem, from, to, label, props) => (ts.toLong, op, elem, from, to, label, props, "out")
      case _ => throw new IllegalStateException(s"${tokens.toList} is malformed.")
    }
  }
  override def write(s2: S2Graph)(element: GraphElement): GraphElementTuple = {
    toGraphElementTuple(element.toLogString().split("\t"))
  }

  override def writeDegree(s2: S2Graph)(degreeKey: DegreeKey, count: Long): GraphElementTuple = {
    val element = DegreeKey.toEdge(s2, degreeKey, count)

    toGraphElementTuple(element.toLogString().split("\t"))
  }
}
