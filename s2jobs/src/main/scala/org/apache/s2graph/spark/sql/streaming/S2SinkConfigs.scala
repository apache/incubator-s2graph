package org.apache.s2graph.spark.sql.streaming

import com.typesafe.config.Config

import scala.util.Try

object S2SinkConfigs {
  val DB_DEFAULT_URL = "db.default.url"
  val HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"


  val DEFAULT_GROUPED_SIZE = "100"
  val DEFAULT_WAIT_TIME_SECONDS = "5"

  val S2_SINK_PREFIX = "s2.spark.sql.streaming.sink"
  val S2_SINK_QUERY_NAME = s"$S2_SINK_PREFIX.queryname"
  val S2_SINK_LOG_PATH = s"$S2_SINK_PREFIX.logpath"
  val S2_SINK_CHECKPOINT_LOCATION = "checkpointlocation"
  val S2_SINK_FILE_CLEANUP_DELAY = s"$S2_SINK_PREFIX.file.cleanup.delay"
  val S2_SINK_DELETE_EXPIRED_LOG = s"$S2_SINK_PREFIX.delete.expired.log"
  val S2_SINK_COMPACT_INTERVAL = s"$S2_SINK_PREFIX.compact.interval"
  val S2_SINK_GROUPED_SIZE = s"$S2_SINK_PREFIX.grouped.size"
  val S2_SINK_WAIT_TIME = s"$S2_SINK_PREFIX.wait.time"

  def getConfigStringOpt(config:Config, path:String): Option[String] = Try(config.getString(path)).toOption

  def getConfigString(config:Config, path:String, default:String): String = getConfigStringOpt(config, path).getOrElse(default)
}
