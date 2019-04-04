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

package org.apache.s2graph.s2jobs.task.custom.sink

import org.apache.s2graph.s2jobs.task.{Sink, TaskConf}
import org.apache.s2graph.s2jobs.task.custom.process.ALSModelProcess
import org.apache.spark.sql.DataFrame


class AnnoyIndexBuildSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  override val FORMAT: String = "parquet"

  override def mandatoryOptions: Set[String] = Set("path", "itemFactors")

  override def write(inputDF: DataFrame): Unit = {
    val df = repartition(preprocess(inputDF), inputDF.sparkSession.sparkContext.defaultParallelism)

    if (df.isStreaming) throw new IllegalStateException("AnnoyIndexBuildSink can not be run as streaming.")
    else {
      ALSModelProcess.buildAnnoyIndex(conf, df)
    }
  }
}
