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

import org.apache.s2graph.s2jobs.udfs.Udf
import org.apache.spark.sql.SparkSession
import play.api.libs.json.{JsValue, Json}

import scala.io.Source

case class JobOption(
                      name:String = "S2BatchJob",
                      confType:String = "db",
                      jobId:Int = -1,
                      confFile:String = ""
                    )

object JobLauncher extends Logger {

  def parseArguments(args: Array[String]): JobOption = {
    val parser = new scopt.OptionParser[JobOption]("run") {
      opt[String]('n', "name").required().action((x, c) =>
        c.copy(name = x)).text("job display name")

      cmd("file").action((_, c) => c.copy(confType = "file"))
        .text("get config from file")
        .children(
          opt[String]('f', "confFile").required().valueName("<file>").action((x, c) =>
            c.copy(confFile = x)).text("configuration file")
        )

      cmd("db").action((_, c) => c.copy(confType = "db"))
        .text("get config from db")
        .children(
          opt[String]('i', "jobId").required().valueName("<jobId>").action((x, c) =>
            c.copy(jobId = x.toInt)).text("configuration file")
        )
    }

    parser.parse(args, JobOption()) match {
      case Some(o) => o
      case None =>
        parser.showUsage()
        throw new IllegalArgumentException(s"failed to parse options... (${args.mkString(",")}")
    }
  }

  def getConfig(options: JobOption):JsValue = options.confType match {
    case "file" =>
      Json.parse(Source.fromFile(options.confFile).mkString)
    case "db" =>
      throw new IllegalArgumentException(s"'db' option that read config file from database is not supported yet.. ")
  }

  def main(args: Array[String]): Unit = {

    val options = parseArguments(args)
    logger.info(s"Job Options : ${options}")

    val jobDescription = JobDescription(getConfig(options))

    val ss = SparkSession
      .builder()
      .appName(s"${jobDescription.name}")
      .config("spark.driver.maxResultSize", "20g")
      .enableHiveSupport()
      .getOrCreate()

    // register udfs
    jobDescription.udfs.foreach{ udfOption =>
      val udf = Class.forName(udfOption.`class`).newInstance().asInstanceOf[Udf]
      logger.info((s"[udf register] ${udfOption}"))
      udf.register(ss, udfOption.name, udfOption.params.getOrElse(Map.empty))
    }

    val job = new Job(ss, jobDescription)
    job.run()
  }
}
