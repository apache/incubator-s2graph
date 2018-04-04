package org.apache.s2graph.s2jobs.serde.writer

import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.DegreeKey
import org.apache.s2graph.s2jobs.serde.GraphElementWritable

object DataFrameWriter {
  type GraphElementTuple = (String, String, String, String, String, String, String, String)
  val Fields = Seq("timestamp", "operation", "element", "from", "to", "label", "props", "direction")
}

class DataFrameWriter extends GraphElementWritable[DataFrameWriter.GraphElementTuple]{
  override def write(s2: S2Graph)(element: GraphElement): (String, String, String, String, String, String, String, String) = {
    val Array(ts, op, elem, from, to, label, props, dir) = element.toLogString().split("\t")

    (ts, op, elem, from, to, label, props, dir)
  }

  override def writeDegree(s2: S2Graph)(degreeKey: DegreeKey, count: Long): (String, String, String, String, String, String, String, String) = {
    val element = DegreeKey.toEdge(s2, degreeKey, count)
    val Array(ts, op, elem, from, to, label, props, dir) = element.toLogString().split("\t")

    (ts, op, elem, from, to, label, props, dir)
  }
}
