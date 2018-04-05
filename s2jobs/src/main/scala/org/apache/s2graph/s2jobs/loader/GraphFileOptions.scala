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

package org.apache.s2graph.s2jobs.loader

object GraphFileOptions {
  val OptionKeys = Set(
    "--input", "--tempDir", "--output", "--zkQuorum", "--table", "--dbUrl", "--dbUser", "--dbPassword", "--dbDriver",
    "--maxHFilePerRegionServer", "--numRegions", "--labelMapping", "--autoEdgeCreate", "--buildDegree",
    "--skipError", "--incrementalLoad", "--method"
  )

  val parser = new scopt.OptionParser[GraphFileOptions]("run") {

    opt[String]('i', "input").required().action( (x, c) =>
      c.copy(input = x) ).text("hdfs path for tsv file(bulk format)")

    opt[String]('t', "tempDir").required().action( (x, c) =>
      c.copy(tempDir = x) ).text("temp hdfs path for staging HFiles")

    opt[String]('o', "output").required().action( (x, c) =>
      c.copy(output = x) ).text("output hdfs path for storing HFiles")

    opt[String]('z', "zkQuorum").required().action( (x, c) =>
      c.copy(zkQuorum = x) ).text("zookeeper config for hbase")

    opt[String]('t', "table").required().action( (x, c) =>
      c.copy(tableName = x) ).text("table name for this bulk upload.")

    opt[String]('c', "dbUrl").required().action( (x, c) =>
      c.copy(dbUrl = x)).text("jdbc connection url.")

    opt[String]('u', "dbUser").required().action( (x, c) =>
      c.copy(dbUser = x)).text("database user name.")

    opt[String]('p', "dbPassword").required().action( (x, c) =>
      c.copy(dbPassword = x)).text("database password.")

    opt[String]('r', "dbDriver").action( (x, c) =>
      c.copy(dbDriver = x)).text("jdbc driver class.")

    opt[Int]('h', "maxHFilePerRegionServer").action ( (x, c) =>
      c.copy(maxHFilePerRegionServer = x)).text("maximum number of HFile per RegionServer.")

    opt[Int]('n', "numRegions").action ( (x, c) =>
      c.copy(numRegions = x)).text("total numRegions(pre-split size) on table.")

    opt[String]('l', "labelMapping").action( (x, c) =>
      c.copy(labelMapping = toLabelMapping(x)) ).text("mapping info to change the label from source (originalLabel:newLabel)")

    opt[Boolean]('d', "buildDegree").action( (x, c) =>
      c.copy(buildDegree = x)).text("generate degree values")

    opt[Boolean]('a', "autoEdgeCreate").action( (x, c) =>
      c.copy(autoEdgeCreate = x)).text("generate reverse edge automatically")

    opt[Boolean]('c', "incrementalLoad").action( (x, c) =>
      c.copy(incrementalLoad = x)).text("whether incremental bulkload which append data on existing table or not.")

    opt[Boolean]('s', "skipError").action ((x, c) =>
      c.copy(skipError = x)).text("whether skip error row.")

    opt[String]('m', "method").action( (x, c) =>
      c.copy(method = x)).text("run method. currently MR(default)/SPARK supported."
    )
  }

  def toOption(args: Array[String]): GraphFileOptions = {
    parser.parse(args, GraphFileOptions()) match {
      case Some(o) => o
      case None =>
        parser.showUsage()
        throw new IllegalArgumentException("failed to parse options...")
    }
  }

  private def toLabelMapping(lableMapping: String): Map[String, String] = {
    (for {
      token <- lableMapping.split(",")
      inner = token.split(":") if inner.length == 2
    } yield {
      (inner.head, inner.last)
    }).toMap
  }

  def toLabelMappingString(labelMapping: Map[String, String]): String =
    labelMapping.map { case (k, v) => Seq(k, v).mkString(":") }.mkString(",")

}
/**
  * Option case class for TransferToHFile.
  * @param input
  * @param output
  * @param zkQuorum
  * @param tableName
  * @param dbUrl
  * @param dbUser
  * @param dbPassword
  * @param maxHFilePerRegionServer
  * @param numRegions
  * @param labelMapping
  * @param autoEdgeCreate
  * @param buildDegree
  * @param incrementalLoad
  * @param compressionAlgorithm
  */
case class GraphFileOptions(input: String = "",
                            tempDir: String = "",
                            output: String = s"/tmp/bulkload",
                            zkQuorum: String = "",
                            tableName: String = "",
                            dbUrl: String = "",
                            dbUser: String = "",
                            dbPassword: String = "",
                            dbDriver: String = "org.h2.Driver",
                            maxHFilePerRegionServer: Int = 1,
                            numRegions: Int = 3,
                            labelMapping: Map[String, String] = Map.empty[String, String],
                            autoEdgeCreate: Boolean = false,
                            buildDegree: Boolean = false,
                            incrementalLoad: Boolean = false,
                            skipError: Boolean = false,
                            compressionAlgorithm: String = "NONE",
                            method: String = "SPARK") {
  def toConfigParams = {
    Map(
      "hbase.zookeeper.quorum" -> zkQuorum,
      "db.default.url" -> dbUrl,
      "db.default.user" -> dbUser,
      "db.default.password" -> dbPassword,
      "db.default.driver" -> dbDriver
    )
  }

  def toCommand: Array[String] =
    Array(
      "--input", input,
      "--tempDir", tempDir,
      "--output", output,
      "--zkQuorum", zkQuorum,
      "--table", tableName,
      "--dbUrl", dbUrl,
      "--dbUser", dbUser,
      "--dbPassword", dbPassword,
      "--dbDriver", dbDriver,
      "--maxHFilePerRegionServer", maxHFilePerRegionServer.toString,
      "--numRegions", numRegions.toString,
      "--labelMapping", GraphFileOptions.toLabelMappingString(labelMapping),
      "--autoEdgeCreate", autoEdgeCreate.toString,
      "--buildDegree", buildDegree.toString,
      "--skipError", skipError.toString,
      "--incrementalLoad", incrementalLoad.toString,
      "--method", method
    )
}