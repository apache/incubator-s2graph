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
