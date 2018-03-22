package org.apache.s2graph.s2jobs

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Schema {
  val BulkLoadSchema = StructType(Seq(
    StructField("timestamp", LongType, false),
    StructField("operation", StringType, false),
    StructField("elem", StringType, false),
    StructField("from", StringType, false),
    StructField("to", StringType, false),
    StructField("label", StringType, false),
    StructField("props", StringType, false),
    StructField("direction", StringType, true)
  ))
}
