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

package org.apache.s2graph.s2jobs.task

import com.typesafe.config.Config
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.util.ToolRunner
import org.apache.s2graph.core.Management
import org.apache.s2graph.s2jobs.S2GraphHelper
import org.apache.s2graph.s2jobs.loader.{GraphFileOptions, HFileGenerator, SparkBulkLoaderTransformer}
import org.apache.s2graph.s2jobs.serde.reader.RowBulkFormatReader
import org.apache.s2graph.s2jobs.serde.writer.KeyValueWriter
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Sink
  *
  * @param queryName
  * @param conf
  */
abstract class Sink(queryName: String, override val conf: TaskConf) extends Task {
  val DEFAULT_CHECKPOINT_LOCATION = s"/tmp/streamingjob/${queryName}/${conf.name}"
  val DEFAULT_TRIGGER_INTERVAL = "10 seconds"

  val FORMAT: String

  def preprocess(df: DataFrame): DataFrame = df

  def write(inputDF: DataFrame): Unit = {
    val df = repartition(preprocess(inputDF), inputDF.sparkSession.sparkContext.defaultParallelism)

    if (inputDF.isStreaming) writeStream(df.writeStream)
    else writeBatch(df.write)
  }

  protected def writeStream(writer: DataStreamWriter[Row]): Unit = {
    val partitionsOpt = conf.options.get("partitions")
    val mode = conf.options.getOrElse("mode", "append") match {
      case "append" => OutputMode.Append()
      case "update" => OutputMode.Update()
      case "complete" => OutputMode.Complete()
      case _ => logger.warn(s"${LOG_PREFIX} unsupported output mode. use default output mode 'append'")
        OutputMode.Append()
    }
    val interval = conf.options.getOrElse("interval", DEFAULT_TRIGGER_INTERVAL)
    val checkpointLocation = conf.options.getOrElse("checkpointLocation", DEFAULT_CHECKPOINT_LOCATION)

    val cfg = conf.options ++ Map("checkpointLocation" -> checkpointLocation)

    val partitionedWriter = if (partitionsOpt.isDefined) writer.partitionBy(partitionsOpt.get.split(","): _*) else writer

    partitionedWriter
      .queryName(s"${queryName}_${conf.name}")
      .format(FORMAT)
      .options(cfg)
      .trigger(Trigger.ProcessingTime(interval))
      .outputMode(mode)
      .start()
  }

  protected def writeBatch(writer: DataFrameWriter[Row]): Unit = {
    val partitionsOpt = conf.options.get("partitions")
    val mode = conf.options.getOrElse("mode", "overwrite") match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "errorIfExists" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
      case _ => SaveMode.Overwrite
    }

    val partitionedWriter = if (partitionsOpt.isDefined) writer.partitionBy(partitionsOpt.get) else writer

    writeBatchInner(partitionedWriter.format(FORMAT).mode(mode))
  }

  protected def writeBatchInner(writer: DataFrameWriter[Row]): Unit = {
    val outputPath = conf.options("path")
    writer.save(outputPath)
  }

  protected def repartition(df: DataFrame, defaultParallelism: Int) = {
    conf.options.get("numPartitions").map(n => Integer.parseInt(n)) match {
      case Some(numOfPartitions: Int) =>
        if (numOfPartitions > defaultParallelism) df.repartition(numOfPartitions)
        else df.coalesce(numOfPartitions)
      case None => df
    }
  }
}

/**
  * KafkaSink
  *
  * @param queryName
  * @param conf
  */
class KafkaSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  override def mandatoryOptions: Set[String] = Set("kafka.bootstrap.servers", "topic")

  override val FORMAT: String = "kafka"

  override def preprocess(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    logger.debug(s"${LOG_PREFIX} schema: ${df.schema}")

    conf.options.getOrElse("format", "json") match {
      case "tsv" =>
        val delimiter = conf.options.getOrElse("delimiter", "\t")

        val columns = df.columns
        df.select(concat_ws(delimiter, columns.map(c => col(c)): _*).alias("value"))
      case format: String =>
        if (format != "json") logger.warn(s"${LOG_PREFIX} unsupported format '$format'. use default json format")
        df.selectExpr("to_json(struct(*)) AS value")
    }
  }

  override protected def writeBatch(writer: DataFrameWriter[Row]): Unit =
    throw new RuntimeException(s"unsupported source type for ${this.getClass.getSimpleName} : ${conf.name}")
}

/**
  * FileSink
  *
  * @param queryName
  * @param conf
  */
class FileSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  override def mandatoryOptions: Set[String] = Set("path", "format")

  override val FORMAT: String = conf.options.getOrElse("format", "parquet")
}

/**
  * HiveSink
  *
  * @param queryName
  * @param conf
  */
class HiveSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  override def mandatoryOptions: Set[String] = Set("database", "table")

  override val FORMAT: String = "hive"

  override protected def writeBatchInner(writer: DataFrameWriter[Row]): Unit = {
    val database = conf.options("database")
    val table = conf.options("table")

    writer.insertInto(s"${database}.${table}")
  }

  override protected def writeStream(writer: DataStreamWriter[Row]): Unit =
    throw new RuntimeException(s"unsupported source type for ${this.getClass.getSimpleName} : ${conf.name}")

}

/**
  * ESSink
  *
  * @param queryName
  * @param conf
  */
class ESSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  override def mandatoryOptions: Set[String] = Set("es.nodes", "path", "es.port")

  override val FORMAT: String = "es"

  override def write(inputDF: DataFrame): Unit = {
    val df = repartition(inputDF, inputDF.sparkSession.sparkContext.defaultParallelism)

    if (inputDF.isStreaming) writeStream(df.writeStream)
    else {

      val resource = conf.options("path")
      EsSparkSQL.saveToEs(df, resource, conf.options)
    }
  }
}

/**
  * S2graphSink
  *
  * @param queryName
  * @param conf
  */
class S2graphSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  override def mandatoryOptions: Set[String] = Set()

  override val FORMAT: String = "org.apache.s2graph.spark.sql.streaming.S2SinkProvider"

  override def write(inputDF: DataFrame): Unit = {
    val df = repartition(preprocess(inputDF), inputDF.sparkSession.sparkContext.defaultParallelism)

    if (inputDF.isStreaming) writeStream(df.writeStream)
    else {
      val options = S2GraphHelper.toGraphFileOptions(conf)
      val config = Management.toConfig(options.toConfigParams)
      val input = df.rdd

      val transformer = new SparkBulkLoaderTransformer(config, options)

      implicit val reader = new RowBulkFormatReader
      implicit val writer = new KeyValueWriter

      val kvs = transformer.transform(input)

      HFileGenerator.generateHFile(df.sparkSession.sparkContext, config, kvs.flatMap(ls => ls), options)

      // finish bulk load by execute LoadIncrementHFile.
      HFileGenerator.loadIncrementHFile(options)
    }
  }
}

