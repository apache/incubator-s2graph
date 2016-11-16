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

import org.apache.s2graph.core.PostProcess
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.Await
import scala.util.Random


class VertexTestHelper extends IntegrateCommon {

  import TestUtil._
  import VertexTestHelper._



  test("vertex") {
    val ids = (7 until 20).map(tcNum => tcNum * 1000 + 0)
    val (serviceName, columnName) = (testServiceName, testColumnName)

    val data = vertexInsertsPayload(serviceName, columnName, ids)
    val payload = Json.parse(Json.toJson(data).toString)
    println(payload)

    val vertices = parser.toVertices(payload, "insert", Option(serviceName), Option(columnName))
    val srcVertices = vertices
    Await.result(graph.mutateVertices(srcVertices, withWait = true), HttpRequestWaitingTime)

    val res = graph.getVertices(srcVertices).map { vertices =>
      PostProcess.verticesToJson(vertices)
    }

    val ret = Await.result(res, HttpRequestWaitingTime)
    val fetched = ret.as[Seq[JsValue]]
    for {
      (d, f) <- data.zip(fetched)
    } yield {
      (d \ "id") should be(f \ "id")
      ((d \ "props") \ "age") should be((f \ "props") \ "age")
    }
  }

  object VertexTestHelper {
    def vertexQueryJson(serviceName: String, columnName: String, ids: Seq[Int]) = {
      Json.parse(
        s"""
           |[
           |{"serviceName": "$serviceName", "columnName": "$columnName", "ids": [${ids.mkString(",")}
         ]}
           |]
       """.stripMargin)
    }

    def vertexInsertsPayload(serviceName: String, columnName: String, ids: Seq[Int]): Seq[JsValue] = {
      ids.map { id =>
        Json.obj("id" -> id, "props" -> randomProps, "timestamp" -> System.currentTimeMillis())
      }
    }

    val vertexPropsKeys = List(
      ("age", "int")
    )

    def randomProps() = {
      (for {
        (propKey, propType) <- vertexPropsKeys
      } yield {
        propKey -> Random.nextInt(100)
      }).toMap
    }
  }
}


