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

import java.util.UUID

import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.streaming.{MetadataLog, Sink}
import org.apache.spark.sql.{DataFrame, SparkSession}

class S2SparkSqlStreamingSink(
                               sparkSession: SparkSession,
                               config:Config
                             ) extends Sink with Logger {
  import S2SinkConfigs._

  private val APP_NAME = "s2graph"

  private val writeLog: MetadataLog[Array[S2SinkStatus]] = {
    val logPath = getCommitLogPath(config)
    logger.info(s"MetaDataLogPath: $logPath")

    new S2SinkMetadataLog(sparkSession, config, logPath)
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    logger.debug(s"addBatch : $batchId, getLatest : ${writeLog.getLatest()}")

    if (batchId <= writeLog.getLatest().map(_._1).getOrElse(-1L)) {
      logger.info(s"Skipping already committed batch [$batchId]")
    } else {
      val queryName = getConfigStringOpt(config, "queryname").getOrElse(UUID.randomUUID().toString)
      val commitProtocol = new S2CommitProtocol(writeLog)
      val jobState = JobState(queryName, batchId)
      val serializedConfig = config.root().render(ConfigRenderOptions.concise())
      val queryExecution = data.queryExecution
      val schema = data.schema

      SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
        try {
          val taskCommits = sparkSession.sparkContext.runJob(queryExecution.toRdd,
            (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
              new S2StreamQueryWriter(serializedConfig, schema, commitProtocol).run(taskContext, iter)
            }
          )
          commitProtocol.commitJob(jobState, taskCommits)
        } catch {
          case t: Throwable =>
            commitProtocol.abortJob(jobState)
            throw t;
        }

      }
    }
  }

  private def getCommitLogPath(config:Config): String = {
    val logPathOpt = getConfigStringOpt(config, S2_SINK_LOG_PATH)
    val userCheckpointLocationOpt = getConfigStringOpt(config, S2_SINK_CHECKPOINT_LOCATION)

    (logPathOpt, userCheckpointLocationOpt) match {
      case (Some(logPath), _) => logPath
      case (None, Some(userCheckpoint)) => s"$userCheckpoint/sinks/$APP_NAME"
      case _ => throw new IllegalArgumentException(s"failed to get commit log path")
    }
  }

  override def toString(): String = "S2GraphSink"
}
