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
