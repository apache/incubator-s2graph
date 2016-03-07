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

package s2.counter.core.v2

import com.typesafe.config.Config
import org.apache.http.HttpStatus
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsObject, JsValue, Json}
import s2.config.S2CounterConfig

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 10..
  */
class GraphOperation(config: Config) {
  // using play-ws without play app
  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val wsClient = new play.api.libs.ws.ning.NingWSClient(builder.build)
  private val s2config = new S2CounterConfig(config)
  val s2graphUrl = s2config.GRAPH_URL
  private[counter] val log = LoggerFactory.getLogger(this.getClass)

  import scala.concurrent.ExecutionContext.Implicits.global

  def createLabel(json: JsValue): Boolean = {
    // fix counter label's schemaVersion
    val newJson = json.as[JsObject] ++ Json.obj("schemaVersion" -> "v2")
    val future = wsClient.url(s"$s2graphUrl/graphs/createLabel").post(newJson).map { resp =>
      resp.status match {
        case HttpStatus.SC_OK =>
          true
        case _ =>
          throw new RuntimeException(s"failed createLabel. errCode: ${resp.status} body: ${resp.body} query: $json")
      }
    }

    Await.result(future, 10 second)
  }

  def deleteLabel(label: String): Boolean = {
    val future = wsClient.url(s"$s2graphUrl/graphs/deleteLabel/$label").put("").map { resp =>
      resp.status match {
        case HttpStatus.SC_OK =>
          true
        case _ =>
          throw new RuntimeException(s"failed deleteLabel. errCode: ${resp.status} body: ${resp.body}")
      }
    }

    Await.result(future, 10 second)
  }
}
