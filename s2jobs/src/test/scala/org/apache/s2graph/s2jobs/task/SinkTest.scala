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

import org.apache.s2graph.core.S2EdgeLike
import org.apache.s2graph.s2jobs.BaseSparkTest
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class SinkTest extends BaseSparkTest {
  def toDataFrame(edges: Seq[String]): DataFrame = {
    import spark.sqlContext.implicits._
    val elements = edges.flatMap(s2.elementBuilder.toEdge(_))

    elements.map { e =>
      (e.getTs(),
        e.getOperation(),
        "e",
        e.srcVertex.innerIdVal.toString,
        e.tgtVertex.innerIdVal.toString,
        e.label(),
        "{}",
        e.getDirection())
    }.toDF("timestamp", "operation", "element", "from", "to", "label", "props", "direction")
  }

  test("S2graphSink writeBatch.") {
    val label = initTestEdgeSchema(s2, tableName, schemaVersion, compressionAlgorithm)

    val bulkEdgeString = "1416236400000\tinsert\tedge\ta\tb\tfriends\t{\"since\":1316236400000,\"score\":10}"
    val df = toDataFrame(Seq(bulkEdgeString))
    val args = options.toCommand.grouped(2).map { kv =>
      kv.head -> kv.last
    }.toMap

    val conf = TaskConf("test", "sql", Seq("input"), args)

    val sink = new S2graphSink("testQuery", conf)
    sink.write(df)

    val s2Edges = s2.edges().asScala.toSeq.map(_.asInstanceOf[S2EdgeLike])
    s2Edges.foreach { edge => println(edge) }
  }
}
