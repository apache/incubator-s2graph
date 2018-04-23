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

import org.apache.s2graph.s2jobs.BaseSparkTest
import play.api.libs.json.Json

class TaskConfTest extends BaseSparkTest {
  test("parse dump loader TaskConf") {
    val s =
      """
        |{
        |        "name": "s2graph_sink",
        |        "inputs": [
        |          "filter"
        |        ],
        |        "type": "s2graph",
        |        "options": {
        |          "writeMethod": "bulk",
        |          "hbase.zookeeper.quorum": "localhost",
        |          "db.default.driver": "com.mysql.jdbc.Driver",
        |          "db.default.url": "jdbc:mysql://localhost:3306/graph_dev",
        |          "db.default.user": "graph",
        |          "db.default.password": "graph",
        |          "--input": "dummy",
        |          "--tempDir": "dummy",
        |          "--output": "/tmp/HTableMigrate",
        |          "--zkQuorum": "localhost",
        |          "--table": "CopyRated",
        |          "--dbUrl": "jdbc:mysql://localhost:3306/graph_dev",
        |          "--dbUser": "graph",
        |          "--dbPassword": "graph",
        |          "--dbDriver": "com.mysql.jdbc.Driver",
        |          "--autoEdgeCreate": "true",
        |          "--buildDegree": "true"
        |        }
        |      }
      """.stripMargin

    implicit val TaskConfReader = Json.reads[TaskConf]
    val taskConf = Json.parse(s).as[TaskConf]

  }
}
