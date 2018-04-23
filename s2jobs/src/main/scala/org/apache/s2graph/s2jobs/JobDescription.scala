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

package org.apache.s2graph.s2jobs

import play.api.libs.json.{JsValue, Json}
import org.apache.s2graph.s2jobs.task._

case class JobDescription(
                           name:String,
                           sources:Seq[Source],
                           processes:Seq[task.Process],
                           sinks:Seq[Sink]
                         )

object JobDescription extends Logger {
  val dummy = JobDescription("dummy", Nil, Nil, Nil)

  def apply(jsVal:JsValue):JobDescription = {
    implicit val TaskConfReader = Json.reads[TaskConf]

    logger.debug(s"JobDescription: ${jsVal}")

    val jobName = (jsVal \ "name").as[String]
    val sources = (jsVal \ "source").asOpt[Seq[TaskConf]].getOrElse(Nil).map(conf => getSource(conf))
    val processes = (jsVal \ "process").asOpt[Seq[TaskConf]].getOrElse(Nil).map(conf => getProcess(conf))
    val sinks = (jsVal \ "sink").asOpt[Seq[TaskConf]].getOrElse(Nil).map(conf => getSink(jobName, conf))

    JobDescription(jobName, sources, processes, sinks)
  }

  def getSource(conf:TaskConf):Source = {
    conf.`type` match {
      case "kafka" => new KafkaSource(conf)
      case "file"  => new FileSource(conf)
      case "hive" => new HiveSource(conf)
      case "s2graph" => new S2GraphSource(conf)
      case _ => throw new IllegalArgumentException(s"unsupported source type : ${conf.`type`}")
    }
  }

  def getProcess(conf:TaskConf): task.Process = {
    conf.`type` match {
      case "sql" => new SqlProcess(conf)
      case "custom" =>
        val customClassOpt = conf.options.get("class")
        customClassOpt match {
          case Some(customClass:String) =>
            logger.debug(s"custom class init.. $customClass")

            Class.forName(customClass)
              .getConstructor(TaskConf.getClass)
              .newInstance(conf)
              .asInstanceOf[task.Process]

          case None => throw new IllegalArgumentException(s"custom class name is not exist.. ${conf}")
        }

      case _ => throw new IllegalArgumentException(s"unsupported process type : ${conf.`type`}")
    }
  }

  def getSink(jobName:String, conf:TaskConf):Sink = {
    conf.`type` match {
      case "kafka" => new KafkaSink(jobName, conf)
      case "file" => new FileSink(jobName, conf)
      case "es" => new ESSink(jobName, conf)
      case "s2graph" => new S2GraphSink(jobName, conf)
      case _ => throw new IllegalArgumentException(s"unsupported sink type : ${conf.`type`}")
    }

  }
}
