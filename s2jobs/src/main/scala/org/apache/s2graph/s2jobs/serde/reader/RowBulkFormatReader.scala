package org.apache.s2graph.s2jobs.serde.reader

import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.S2GraphHelper
import org.apache.s2graph.s2jobs.serde.GraphElementReadable
import org.apache.spark.sql.Row

class RowBulkFormatReader extends GraphElementReadable[Row] {
  private val RESERVED_COLUMN = Set("timestamp", "from", "to", "label", "operation", "elem", "direction")

  override def read(s2: S2Graph)(row: Row): Option[GraphElement] =
    S2GraphHelper.sparkSqlRowToGraphElement(s2, row, row.schema, RESERVED_COLUMN)

}
