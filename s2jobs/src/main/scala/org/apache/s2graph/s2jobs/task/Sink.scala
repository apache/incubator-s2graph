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

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.hadoop.hbase.KeyValue
import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.core.{GraphUtil, Management}
import org.apache.s2graph.s2jobs.S2GraphHelper
import org.apache.s2graph.s2jobs.loader.{HFileGenerator, SparkBulkLoaderTransformer}
import org.apache.s2graph.s2jobs.serde.reader.RowBulkFormatReader
import org.apache.s2graph.s2jobs.serde.writer.KeyValueWriter
import org.apache.s2graph.s2jobs.wal.WalLog
import org.apache.s2graph.s2jobs.wal.utils.{SchemaUtil, SerializeUtil}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
    val isContinuous = conf.options.getOrElse("isContinuous", "false").toBoolean
    val trigger = if (isContinuous) Trigger.Continuous(interval) else Trigger.ProcessingTime(interval)

    val cfg = conf.options ++ Map("checkpointLocation" -> checkpointLocation)

    val partitionedWriter = if (partitionsOpt.isDefined) writer.partitionBy(partitionsOpt.get.split(","): _*) else writer

    partitionedWriter
      .queryName(s"${queryName}_${conf.name}")
      .format(FORMAT)
      .options(cfg)
      .trigger(trigger)
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

    val partitionedWriter = if (partitionsOpt.isDefined) writer.partitionBy(partitionsOpt.get.split(","): _*) else writer

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
      case "raw" =>
        df
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
  * JdbcSink
  * @param queryName
  * @param conf
  */
class JdbcSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  override def mandatoryOptions: Set[String] = Set("url", "dbtable")
  override val FORMAT: String = "jdbc"

  override protected def writeBatchInner(writer: DataFrameWriter[Row]): Unit = {
    writer.format("jdbc").options(conf.options).save()
  }
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
class S2GraphSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  import org.apache.s2graph.core.S2GraphConfigs._
  import org.apache.s2graph.spark.sql.streaming.S2SinkConfigs._

  import scala.collection.JavaConversions._

  override def mandatoryOptions: Set[String] = Set()

  override val FORMAT: String = "org.apache.s2graph.spark.sql.streaming.S2SinkProvider"

  private def writeBatchBulkload(df: DataFrame, runLoadIncrementalHFiles: Boolean = true): Unit = {
    /*
    * overwrite HBASE + MetaStorage + LocalCache configuration from given option.
    */
    val mergedConf = TaskConf.parseHBaseConfigs(conf) ++ TaskConf.parseMetaStoreConfigs(conf) ++
      TaskConf.parseLocalCacheConfigs(conf)
    val config = Management.toConfig(mergedConf)

    /*
      * initialize meta storage connection.
      * note that we only connect meta storage once at spark driver,
      * then build schema manager which is serializable and broadcast it to executors.
      *
      * schema manager responsible for translation between logical logical representation and physical representation.
      *
      */
    SchemaUtil.init(config)
    val ss = df.sparkSession
    val sc = ss.sparkContext

    // required for bulkload
    val labelMapping = getConfigStringOpt(config, S2_SINK_BULKLOAD_LABEL_MAPPING).map(GraphUtil.toLabelMapping).getOrElse(Map.empty)
    val buildDegree = getConfigStringOpt(config, S2_SINK_BULKLOAD_BUILD_DEGREE).map(_.toBoolean).getOrElse(false)
    val autoEdgeCreate = getConfigStringOpt(config, S2_SINK_BULKLOAD_AUTO_EDGE_CREATE).map(_.toBoolean).getOrElse(false)
    val skipError = getConfigStringOpt(config, S2_SINK_SKIP_ERROR).map(_.toBoolean).getOrElse(false)

    val zkQuorum = config.getString(HBaseConfigs.HBASE_ZOOKEEPER_QUORUM)
    val tableName = config.getString(S2_SINK_BULKLOAD_HBASE_TABLE_NAME)

    val numRegions = config.getString(S2_SINK_BULKLOAD_HBASE_NUM_REGIONS).toInt
    val outputPath = config.getString(S2_SINK_BULKLOAD_HBASE_TEMP_DIR)

    // optional.
    val incrementalLoad = getConfigStringOpt(config, S2_SINK_BULKLOAD_HBASE_INCREMENTAL_LOAD).map(_.toBoolean).getOrElse(false)
    val compressionAlgorithm = getConfigStringOpt(config, S2_SINK_BULKLOAD_HBASE_COMPRESSION).getOrElse("lz4")

    val hbaseConfig = HFileGenerator.toHBaseConfig(zkQuorum, tableName)

    val schema = SchemaUtil.buildSchemaManager(Map.empty, Nil)
    val schemaBCast = sc.broadcast(schema)
    val tallSchemaVersions = Set(HBaseType.VERSION4)

    implicit val enc = Encoders.kryo[KeyValue]

    val kvs = df.mapPartitions { iter =>
      val schema = schemaBCast.value

      iter.flatMap { row =>
        val walLog = WalLog.fromRow(row)

        SerializeUtil.walToSKeyValues(walLog, schema, tallSchemaVersions)
          .map(SerializeUtil.sKeyValueToKeyValue)
      }
    }

//    val input = df
//    val transformer = new SparkBulkLoaderTransformer(graphConfig, labelMapping, buildDegree)
//
//    implicit val reader = new RowBulkFormatReader
//    implicit val writer = new KeyValueWriter(autoEdgeCreate, skipError)
//
//    val kvs = transformer.transform(input).flatMap(ls => ls)

    HFileGenerator.generateHFile(df.sparkSession.sparkContext, config,
      kvs.rdd, hbaseConfig, tableName,
      numRegions, outputPath, incrementalLoad, compressionAlgorithm
    )

    // finish bulk load by execute LoadIncrementHFile.
    if (runLoadIncrementalHFiles) HFileGenerator.loadIncrementalHFiles(outputPath, tableName)
  }

  private def writeBatchWithMutate(df:DataFrame):Unit = {
    import org.apache.s2graph.spark.sql.streaming.S2SinkConfigs._

    import scala.collection.JavaConversions._

    // TODO: FIX THIS. overwrite local cache config.
    val mergedOptions = conf.options ++ TaskConf.parseLocalCacheConfigs(conf)
    val graphConfig: Config = ConfigFactory.parseMap(mergedOptions).withFallback(ConfigFactory.load())
    val serializedConfig = graphConfig.root().render(ConfigRenderOptions.concise())

    val reader = new RowBulkFormatReader

    val groupedSize = getConfigString(graphConfig, S2_SINK_GROUPED_SIZE, DEFAULT_GROUPED_SIZE).toInt
    val waitTime = getConfigString(graphConfig, S2_SINK_WAIT_TIME, DEFAULT_WAIT_TIME_SECONDS).toInt
    val skipError = getConfigStringOpt(graphConfig, S2_SINK_SKIP_ERROR).map(_.toBoolean).getOrElse(false)

    df.foreachPartition { iters =>
      val config = ConfigFactory.parseString(serializedConfig)
      val s2Graph = S2GraphHelper.getS2Graph(config)

      val responses = iters.grouped(groupedSize).flatMap { rows =>
        val elements = rows.flatMap(row => reader.read(s2Graph)(row))

        val mutateF = if (skipError) {
          try {
            s2Graph.mutateElements(elements, true)
          } catch {
            case e: Throwable => Future.successful(Nil)
          }
        } else {
          s2Graph.mutateElements(elements, true)
        }
        Await.result(mutateF, Duration(waitTime, "seconds"))
      }

      val (success, fail) = responses.toSeq.partition(r => r.isSuccess)
      logger.info(s"success : ${success.size}, fail : ${fail.size}")
    }
  }

  override def write(inputDF: DataFrame): Unit = {
    val df = repartition(preprocess(inputDF), inputDF.sparkSession.sparkContext.defaultParallelism)

    if (inputDF.isStreaming) writeStream(df.writeStream)
    else {
      conf.options.getOrElse(S2_SINK_WRITE_METHOD, "mutate") match {
        case "mutate" => writeBatchWithMutate(df)
        case "bulk" =>
          val runLoadIncrementalHFiles = conf.options.getOrElse("runLoadIncrementalHFiles", "true").toBoolean
          writeBatchBulkload(df, runLoadIncrementalHFiles)
        case writeMethod:String => throw new IllegalArgumentException(s"unsupported write method '$writeMethod' (valid method: mutate, bulk)")
      }
    }
  }
}

