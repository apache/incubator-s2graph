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

package org.apache.s2graph.core.Integrate

import org.apache.s2graph.core.{CommonTest, PostProcess}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.Await


class VertexTest extends IntegrateCommon {

  import TestUtil._

  test("vertex", CommonTest) {
    val ids = (7 until 20).map(tcNum => tcNum * 1000 + 0)
    val (serviceName, columnName) = (testServiceName, testColumnName)

    val data = vertexInsertsPayload(serviceName, columnName, ids)
    val payload = Json.parse(Json.toJson(data).toString)
    println(s">>> insert vertices: ${payload}")

    val vertices = parser.toVertices(payload, "insert", Option(serviceName), Option(columnName))
    Await.result(graph.mutateVertices(vertices, withWait = true), HttpRequestWaitingTime)

    val res = graph.getVertices(vertices).map { vertices =>
      PostProcess.verticesToJson(vertices)
    }

    val ret = Await.result(res, HttpRequestWaitingTime)
    val fetched = ret.as[Seq[JsValue]]
    println(s">>> fetched vertices: ${fetched}")
    for {
      (d, f) <- data.zip(fetched)
    } yield {
      (d \ "id") should be(f \ "id")
      ((d \ "props") \ "age") should be((f \ "props") \ "age")
    }
  }
}


