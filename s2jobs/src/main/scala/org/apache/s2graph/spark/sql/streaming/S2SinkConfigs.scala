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
