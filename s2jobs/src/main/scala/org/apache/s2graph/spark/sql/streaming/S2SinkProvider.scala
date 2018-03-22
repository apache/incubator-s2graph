package org.apache.s2graph.spark.sql.streaming

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

import scala.collection.JavaConversions._

class S2SinkProvider extends StreamSinkProvider with DataSourceRegister with Logger {
  override def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {

    logger.info(s"S2SinkProvider options : ${parameters}")
    val jobConf:Config = ConfigFactory.parseMap(parameters).withFallback(ConfigFactory.load())
    logger.info(s"S2SinkProvider Configuration : ${jobConf.root().render(ConfigRenderOptions.concise())}")

    new S2SparkSqlStreamingSink(sqlContext.sparkSession, jobConf)
  }

  override def shortName(): String = "s2graph"
}
