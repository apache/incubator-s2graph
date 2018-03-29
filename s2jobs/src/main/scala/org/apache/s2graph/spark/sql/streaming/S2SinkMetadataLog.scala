package org.apache.s2graph.spark.sql.streaming

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.CompactibleFileStreamLog

class S2SinkMetadataLog(sparkSession: SparkSession, config:Config, logPath:String)
  extends CompactibleFileStreamLog[S2SinkStatus](S2SinkMetadataLog.VERSION, sparkSession, logPath) {
  import S2SinkConfigs._

  override protected def fileCleanupDelayMs: Long = getConfigStringOpt(config, S2_SINK_FILE_CLEANUP_DELAY)
                                                      .getOrElse("60").toLong

  override protected def isDeletingExpiredLog: Boolean = getConfigStringOpt(config, S2_SINK_DELETE_EXPIRED_LOG)
                                                            .getOrElse("false").toBoolean

  override protected def defaultCompactInterval: Int = getConfigStringOpt(config, S2_SINK_COMPACT_INTERVAL)
                                                          .getOrElse("60").toInt

  override def compactLogs(logs: Seq[S2SinkStatus]): Seq[S2SinkStatus] = logs
}

object S2SinkMetadataLog {
  private val VERSION = 1
}