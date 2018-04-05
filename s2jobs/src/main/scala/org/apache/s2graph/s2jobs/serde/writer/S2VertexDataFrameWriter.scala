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

import org.apache.s2graph.core.{GraphElement, S2Graph, S2VertexLike}
import org.apache.s2graph.s2jobs.DegreeKey
import org.apache.s2graph.s2jobs.serde.GraphElementWritable
import org.apache.s2graph.s2jobs.serde.writer.S2VertexDataFrameWriter.S2VertexTuple

object S2VertexDataFrameWriter {
  type S2VertexTuple = (Long, String, String, String, String, String, String)
  val EmptyS2VertexTuple = (0L, "", "", "", "", "", "")
  val Fields = Seq("timestamp", "operation", "elem", "id", "service", "column", "props")
}

class S2VertexDataFrameWriter extends GraphElementWritable[S2VertexTuple] {
  import S2VertexDataFrameWriter._
  private def toVertexTuple(tokens: Array[String]): S2VertexTuple = {
    tokens match {
      case Array(ts, op, elem, id, service, column, props) => (ts.toLong, op, elem, id, service, column, props)
      case _ => throw new IllegalStateException(s"${tokens.toList} is malformed.")
    }
  }
  override def write(s2: S2Graph)(element: GraphElement): S2VertexTuple = {
    element match {
      case v: S2VertexLike => toVertexTuple(v.toLogString().split("\t"))
      case _ => throw new IllegalArgumentException(s"Vertex expected, $element is not vertex.")
    }

  }

  override def writeDegree(s2: S2Graph)(degreeKey: DegreeKey, count: Long): S2VertexTuple =
    EmptyS2VertexTuple
}
