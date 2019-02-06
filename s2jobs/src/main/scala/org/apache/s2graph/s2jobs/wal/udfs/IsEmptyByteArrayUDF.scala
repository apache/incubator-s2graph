package org.apache.s2graph.s2jobs.wal.udfs

import org.apache.s2graph.s2jobs.udfs.Udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


class IsEmptyByteArrayUDF extends Udf {
  override def register(ss: SparkSession, name: String, options: Map[String, String]): Unit = {
    val f = udf((bytes: Array[Byte]) => {
      bytes.isEmpty
    })

    ss.udf.register(name, f)
  }
}
