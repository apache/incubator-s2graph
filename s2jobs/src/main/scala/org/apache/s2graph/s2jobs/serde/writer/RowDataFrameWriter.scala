package org.apache.s2graph.s2jobs.serde.writer

import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.{DegreeKey, S2GraphHelper}
import org.apache.s2graph.s2jobs.serde.GraphElementWritable
import org.apache.spark.sql.Row

class RowDataFrameWriter extends GraphElementWritable[Row]{
  override def write(s2: S2Graph)(element: GraphElement): Row = {
    S2GraphHelper.graphElementToSparkSqlRow(s2, element)
  }

  override def writeDegree(s2: S2Graph)(degreeKey: DegreeKey, count: Long): Row = {
    val element = DegreeKey.toEdge(s2, degreeKey, count)
    S2GraphHelper.graphElementToSparkSqlRow(s2, element)
  }
}
