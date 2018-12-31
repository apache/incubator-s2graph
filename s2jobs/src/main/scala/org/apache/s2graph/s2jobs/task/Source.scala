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

import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.core.{JSONParser, Management}
import org.apache.s2graph.s2jobs.loader.HFileGenerator
import org.apache.s2graph.s2jobs.wal.utils.{DeserializeUtil, SchemaUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{JsObject, Json}

/**
  * Source
  *
  * @param conf
  */
abstract class Source(override val conf: TaskConf) extends Task {
  def toDF(ss: SparkSession): DataFrame
}

class KafkaSource(conf: TaskConf) extends Source(conf) {
  val DEFAULT_FORMAT = "raw"

  override def mandatoryOptions: Set[String] = Set("kafka.bootstrap.servers", "subscribe")

  def repartition(df: DataFrame, defaultParallelism: Int) = {
    conf.options.get("numPartitions").map(n => Integer.parseInt(n)) match {
      case Some(numOfPartitions: Int) =>
        logger.info(s"[repartitition] $numOfPartitions ($defaultParallelism)")
        if (numOfPartitions >= defaultParallelism) df.repartition(numOfPartitions)
        else df.coalesce(numOfPartitions)
      case None => df
    }
  }

  override def toDF(ss: SparkSession): DataFrame = {
    logger.info(s"${LOG_PREFIX} options: ${conf.options}")

    val format = conf.options.getOrElse("format", "raw")
    val df = ss.readStream.format("kafka").options(conf.options).load()

    val partitionedDF = repartition(df, df.sparkSession.sparkContext.defaultParallelism)
    format match {
      case "raw" => partitionedDF
      case "json" => parseJsonSchema(ss, partitionedDF)
      //      case "custom" => parseCustomSchema(df)
      case _ =>
        logger.warn(s"${LOG_PREFIX} unsupported format '$format'.. use default schema ")
        partitionedDF
    }
  }

  def parseJsonSchema(ss: SparkSession, df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.from_json
    import org.apache.spark.sql.types.DataType
    import ss.implicits._

    val schemaOpt = conf.options.get("schema")
    schemaOpt match {
      case Some(schemaAsJson: String) =>
        val dataType: DataType = DataType.fromJson(schemaAsJson)
        logger.debug(s"${LOG_PREFIX} schema : ${dataType.sql}")

        df.selectExpr("CAST(value AS STRING)")
          .select(from_json('value, dataType) as 'struct)
          .select("struct.*")

      case None =>
        logger.warn(s"${LOG_PREFIX} json format does not have schema.. use default schema ")
        df
    }
  }
}

class FileSource(conf: TaskConf) extends Source(conf) {
  val DEFAULT_FORMAT = "parquet"

  override def mandatoryOptions: Set[String] = Set("paths")

  override def toDF(ss: SparkSession): DataFrame = {
    import org.apache.s2graph.s2jobs.Schema._
    val paths = conf.options("paths").split(",")
    val format = conf.options.getOrElse("format", DEFAULT_FORMAT)
    val columnsOpt = conf.options.get("columns")
    val readOptions = conf.options.get("read").map { s =>
      Json.parse(s).as[JsObject].fields.map { case (k, jsValue) =>
        k -> JSONParser.jsValueToString(jsValue)
      }.toMap
    }.getOrElse(Map.empty)

    format match {
      case "edgeLog" =>
        ss.read.format("com.databricks.spark.csv").option("delimiter", "\t")
          .schema(BulkLoadSchema).load(paths: _*)
      case _ =>
        val df =
          if (readOptions.isEmpty) ss.read.format(format).load(paths: _*)
          else ss.read.options(readOptions).format(format).load(paths: _*)

        if (columnsOpt.isDefined) df.toDF(columnsOpt.get.split(",").map(_.trim): _*) else df
    }
  }
}


class HiveSource(conf: TaskConf) extends Source(conf) {
  override def mandatoryOptions: Set[String] = Set("database", "table")

  override def toDF(ss: SparkSession): DataFrame = {
    val database = conf.options("database")
    val table = conf.options("table")

    val sql = conf.options.getOrElse("sql", s"SELECT * FROM ${database}.${table}")
    ss.sql(sql)
  }
}

class S2GraphSource(conf: TaskConf) extends Source(conf) {

  import org.apache.s2graph.spark.sql.streaming.S2SourceConfigs._

  override def mandatoryOptions: Set[String] = Set(
    S2_SOURCE_BULKLOAD_HBASE_ROOT_DIR,
    S2_SOURCE_BULKLOAD_RESTORE_PATH,
    S2_SOURCE_BULKLOAD_HBASE_TABLE_NAMES,
    S2_SOURCE_BULKLOAD_LABEL_NAMES
  )

  override def toDF(ss: SparkSession): DataFrame = {
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

    val sc = ss.sparkContext

    val snapshotPath = conf.options(S2_SOURCE_BULKLOAD_HBASE_ROOT_DIR)
    val restorePath = conf.options(S2_SOURCE_BULKLOAD_RESTORE_PATH)
    val tableNames = conf.options(S2_SOURCE_BULKLOAD_HBASE_TABLE_NAMES).split(",")
    val columnFamily = conf.options.getOrElse(S2_SOURCE_BULKLOAD_HBASE_TABLE_CF, "e")
    val batchSize = conf.options.getOrElse(S2_SOURCE_BULKLOAD_SCAN_BATCH_SIZE, "1000").toInt
    val labelNames = conf.options(S2_SOURCE_BULKLOAD_LABEL_NAMES).split(",").toSeq

    val labelMapping = Map.empty[String, String]
    val buildDegree =
      if (columnFamily == "v") false
      else conf.options.getOrElse(S2_SOURCE_BULKLOAD_BUILD_DEGREE, "false").toBoolean
    //    val elementType = conf.options.getOrElse(S2_SOURCE_ELEMENT_TYPE, "IndexEdge")
    //    val schema = if (columnFamily == "v") Schema.VertexSchema else Schema.EdgeSchema

    val cells = HFileGenerator.tableSnapshotDump(ss, config, snapshotPath,
      restorePath, tableNames, columnFamily, batchSize, labelMapping, buildDegree)

    val schema = SchemaUtil.buildEdgeDeserializeSchema(labelNames)
    val schemaBCast = sc.broadcast(schema)
    val tallSchemaVersions = Set(HBaseType.VERSION4)
    val tgtDirection = 0

    val results = cells.mapPartitions { iter =>
      val schema = schemaBCast.value

      iter.flatMap { case (_, result) =>
        DeserializeUtil.indexEdgeResultToWals(result, schema, tallSchemaVersions, tgtDirection)
      }

    }

    ss.createDataFrame(results)
  }
}