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

import org.apache.spark.sql.execution.streaming.MetadataLog

class S2CommitProtocol(@transient val commitLog: MetadataLog[Array[S2SinkStatus]])
  extends Serializable with Logger {

  def initJob(jobState: JobState): Unit = {
    logger.debug(s"[InitJob] ${jobState}")
  }

  def commitJob(jobState: JobState, taskCommits: Seq[TaskCommit]): Unit = {
    val commits = taskCommits.flatMap(_.statuses).toArray[S2SinkStatus]
    if (commitLog.add(jobState.batchId, commits)) {
      logger.debug(s"[Committed batch] ${jobState.batchId}  (${taskCommits})")
    } else {
      throw new IllegalStateException(s"Batch Id [${jobState.batchId}] is already committed")
    }
  }

  def abortJob(jobState: JobState): Unit = {
    logger.info(s"[AbortJob] ${jobState}")
  }

  @transient var executionStart: Long = _

  def initTask(): Unit = {
    executionStart = System.currentTimeMillis()
  }

  def commitTask(taskState: TaskState): TaskCommit = {
    TaskCommit(Some(S2SinkStatus(taskState.taskId, executionStart, taskState.successCnt, taskState.failCnt, taskState.counter)))
  }

  def abortTask(taskState: TaskState): Unit = {
    logger.info(s"[AbortTask] ${taskState}")
    TaskCommit(None)
  }
}

case class JobState(jobId: String, batchId: Long, dataCnt: Long = -1L)
case class TaskState(
                      taskId: Int,
                      successCnt:Long = -1L,
                      failCnt:Long = -1L,
                      counter:Map[String, Int] = Map.empty
                    )
case class TaskCommit(statuses: Option[S2SinkStatus])
