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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.s2graph.s2jobs.task._

import scala.collection.mutable

class Job(ss:SparkSession, jobDesc:JobDescription) extends Serializable with Logger {
  private val dfMap = mutable.Map[String, DataFrame]()

  def run() = {
    // source
    jobDesc.sources.foreach{ source =>
      val df = source.toDF(ss)
      if (source.conf.cache.getOrElse(false) && !df.isStreaming) df.cache()

      dfMap.put(source.conf.name, df)
    }
    logger.debug(s"valid source DF set : ${dfMap.keySet}")

    // process
    var processRst:Seq[(String, DataFrame)] = Nil
    do {
      processRst = getValidProcess(jobDesc.processes)
      processRst.foreach { case (name, df) => dfMap.put(name, df)}

    } while(processRst.nonEmpty)

    logger.debug(s"valid named DF set : ${dfMap.keySet}")

    // sinks
    jobDesc.sinks.foreach { s =>
      val inputDFs = s.conf.inputs.flatMap{ input => dfMap.get(input)}
      if (inputDFs.isEmpty) throw new IllegalArgumentException(s"sink has not valid inputs (${s.conf.name})")

      // use only first input
      s.write(inputDFs.head)
    }
    // if stream query exist
    if (ss.streams.active.length > 0) ss.streams.awaitAnyTermination()
  }

  private def getValidProcess(processes:Seq[Process]):Seq[(String, DataFrame)] = {
    val dfKeys = dfMap.keySet

    processes.filter{ p =>
        var existAllInput = true
        p.conf.inputs.foreach { input => existAllInput = dfKeys(input) }
        !dfKeys(p.conf.name) && existAllInput
    }
    .map { p =>
      val inputMap = p.conf.inputs.map{ input => (input,  dfMap(input)) }.toMap
      val df = p.execute(ss, inputMap)
      if (p.conf.cache.getOrElse(false) && !df.isStreaming) df.cache()
      p.conf.name -> df
    }
  }
}
