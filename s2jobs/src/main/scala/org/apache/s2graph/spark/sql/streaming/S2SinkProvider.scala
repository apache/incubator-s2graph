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

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

import scala.collection.JavaConversions._

class S2SinkProvider extends StreamSinkProvider with DataSourceRegister with Logger {
  override def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {

    logger.info(s"S2SinkProvider options : ${parameters}")
    val jobConf:Config = ConfigFactory.parseMap(parameters).withFallback(ConfigFactory.load())
    logger.info(s"S2SinkProvider Configuration : ${jobConf.root().render(ConfigRenderOptions.concise())}")

    new S2SparkSqlStreamingSink(sqlContext.sparkSession, jobConf)
  }

  override def shortName(): String = "s2graph"
}
