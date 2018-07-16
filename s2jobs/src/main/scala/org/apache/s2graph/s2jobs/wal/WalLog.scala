package org.apache.s2graph.s2jobs.wal

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

case class WalLog(timestamp:Long,
                  operation:String,
                  elem:String,
                  from:String,
                  to:String,
                  service:String,
                  label:String,
                  props:String) {
  val id = from
  val columnName = label
  val serviceName = to
}

object WalLog {
  val WalLogSchema = StructType(Seq(
    StructField("timestamp", LongType, false),
    StructField("operation", StringType, false),
    StructField("elem", StringType, false),
    StructField("from", StringType, false),
    StructField("to", StringType, false),
    StructField("service", StringType, true),
    StructField("label", StringType, false),
    StructField("props", StringType, false)
    //    StructField("direction", StringType, true)
  ))
}

