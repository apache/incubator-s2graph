package org.apache.s2graph.s2jobs.serde.writer

import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.DegreeKey
import org.apache.s2graph.s2jobs.serde.GraphElementWritable

class IdentityWriter extends GraphElementWritable[GraphElement]{
  override def write(s2: S2Graph)(element: GraphElement): GraphElement = element

  override def writeDegree(s2: S2Graph)(degreeKey: DegreeKey, count: Long): GraphElement =
    DegreeKey.toEdge(s2, degreeKey, count)

}
