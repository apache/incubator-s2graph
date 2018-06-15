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

import org.apache.s2graph.s2jobs.Logger
import org.apache.s2graph.s2jobs.loader.GraphFileOptions

object TaskConf {
  def toGraphFileOptions(taskConf: TaskConf): GraphFileOptions = {
    val args = taskConf.options.filterKeys(GraphFileOptions.OptionKeys)
      .flatMap(kv => Seq(kv._1, kv._2)).toSeq.toArray

    GraphFileOptions.toOption(args)
  }

  def parseHBaseConfigs(taskConf: TaskConf): Map[String, Any] = {
    taskConf.options.filterKeys(_.startsWith("hbase."))
  }

  def parseMetaStoreConfigs(taskConf: TaskConf): Map[String, Any] = {
    taskConf.options.filterKeys(_.startsWith("db."))
  }

  def parseLocalCacheConfigs(taskConf: TaskConf): Map[String, Any] = {
    taskConf.options.filterKeys(_.startsWith("cache.")).mapValues(_.toInt)
  }
}
case class TaskConf(name:String, `type`:String, inputs:Seq[String] = Nil, options:Map[String, String] = Map.empty, cache:Option[Boolean]=None)

trait Task extends Serializable with Logger {
  val conf: TaskConf
  val LOG_PREFIX = s"[${this.getClass.getSimpleName}]"

  def mandatoryOptions: Set[String]

  def isValidate: Boolean = mandatoryOptions.subsetOf(conf.options.keySet)

  require(isValidate,
    s"""${LOG_PREFIX} not exists mandatory options '${mandatoryOptions.mkString(",")}'
                          in task options (${conf.options.keySet.mkString(",")})
                      """.stripMargin)

  println(s"${LOG_PREFIX} init : $conf")
}
