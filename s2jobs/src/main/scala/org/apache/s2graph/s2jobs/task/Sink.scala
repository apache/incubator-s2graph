package org.apache.s2graph.s2jobs.task

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Sink
  * @param queryName
  * @param conf
  */
abstract class Sink(queryName:String, override val conf:TaskConf) extends Task {
  val DEFAULT_CHECKPOINT_LOCATION = s"/tmp/streamingjob/${queryName}"
  val DEFAULT_TRIGGER_INTERVAL = "10 seconds"

  val FORMAT:String

  def preprocess(df:DataFrame):DataFrame = df

  def write(inputDF: DataFrame):Unit = {
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

    val partitionedWriter = if (partitionsOpt.isDefined) writer.partitionBy(partitionsOpt.get) else writer

    val query = partitionedWriter
      .queryName(queryName)
      .format(FORMAT)
      .options(cfg)
      .trigger(Trigger.ProcessingTime(interval))
      .outputMode(mode)
      .start()

    query.awaitTermination()
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

  protected def repartition(df:DataFrame, defaultParallelism:Int) = {
    conf.options.get("numPartitions").map(n => Integer.parseInt(n)) match {
      case Some(numOfPartitions:Int) =>
        if (numOfPartitions > defaultParallelism) df.repartition(numOfPartitions)
        else df.coalesce(numOfPartitions)
      case None => df
    }
  }
}

/**
  * KafkaSink
  * @param queryName
  * @param conf
  */
class KafkaSink(queryName:String, conf:TaskConf) extends Sink(queryName, conf) {
  override def mandatoryOptions: Set[String] = Set("kafka.bootstrap.servers", "topic")
  override val FORMAT: String = "kafka"

  override def preprocess(df:DataFrame):DataFrame = {
    import org.apache.spark.sql.functions._

    logger.debug(s"${LOG_PREFIX} schema: ${df.schema}")

    conf.options.getOrElse("format", "json") match {
      case "tsv" =>
        val delimiter = conf.options.getOrElse("delimiter", "\t")

        val columns = df.columns
        df.select(concat_ws(delimiter, columns.map(c => col(c)): _*).alias("value"))
      case format:String =>
        if (format != "json") logger.warn(s"${LOG_PREFIX} unsupported format '$format'. use default json format")
        df.selectExpr("to_json(struct(*)) AS value")
    }
  }

  override protected def writeBatch(writer: DataFrameWriter[Row]): Unit =
    throw new RuntimeException(s"unsupported source type for ${this.getClass.getSimpleName} : ${conf.name}")
}

/**
  * FileSink
  * @param queryName
  * @param conf
  */
class FileSink(queryName:String, conf:TaskConf) extends Sink(queryName, conf) {
  override def mandatoryOptions: Set[String] = Set("path", "format")
  override val FORMAT: String = conf.options.getOrElse("format", "parquet")
}

/**
  * HiveSink
  * @param queryName
  * @param conf
  */
class HiveSink(queryName:String, conf:TaskConf) extends Sink(queryName, conf) {
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
  * @param queryName
  * @param conf
  */
class ESSink(queryName:String, conf:TaskConf) extends Sink(queryName, conf) {
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

