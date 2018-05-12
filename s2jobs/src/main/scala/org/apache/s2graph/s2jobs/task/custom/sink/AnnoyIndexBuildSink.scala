package org.apache.s2graph.s2jobs.task.custom.sink

import org.apache.s2graph.s2jobs.task.{Sink, TaskConf}
import org.apache.s2graph.s2jobs.task.custom.process.ALSModelProcess
import org.apache.spark.sql.DataFrame


class AnnoyIndexBuildSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  override val FORMAT: String = "parquet"

  override def mandatoryOptions: Set[String] = Set("path", "itemFactors")

  override def write(inputDF: DataFrame): Unit = {
    val df = repartition(preprocess(inputDF), inputDF.sparkSession.sparkContext.defaultParallelism)

    if (inputDF.isStreaming) throw new IllegalStateException("AnnoyIndexBuildSink can not be run as streaming.")
    else {
      ALSModelProcess.buildAnnoyIndex(conf, inputDF)
    }
  }
}
