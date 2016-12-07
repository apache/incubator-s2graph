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

package org.apache.s2graph.core.benchmark

import play.api.libs.json.JsNumber
import play.libs.Json

class JsonBenchmarkSpec extends BenchmarkCommon {

  "JsonBenchSpec" should {

    "Json Append" >> {
      import play.api.libs.json.{Json, _}
      val numberJson = Json
        .toJson((0 to 1000).map { i =>
          s"$i" -> JsNumber(i * i)
        }.toMap)
        .as[JsObject]

      /** dummy warm-up **/
      (0 to 10000) foreach { n =>
        Json.obj(s"$n" -> "dummy") ++ numberJson
      }
      (0 to 10000) foreach { n =>
        Json.obj(s"$n" -> numberJson)
      }

      duration("Append by JsObj ++ JsObj ") {
        (0 to 100000) foreach { n =>
          numberJson ++ Json.obj(s"$n" -> "dummy")
        }
      }

      duration("Append by Json.obj(newJson -> JsObj)") {
        (0 to 100000) foreach { n =>
          Json.obj(s"$n" -> numberJson)
        }
      }
      true
    }
  }

  "Make Json" >> {
    duration("map to json") {
      (0 to 10000) foreach { n =>
        val numberMaps = (0 to 100).map { n =>
          n.toString -> JsNumber(n * n)
        }.toMap

        Json.toJson(numberMaps)
      }
    }

    duration("direct") {
      (0 to 10000) foreach { n =>
        var jsObj = play.api.libs.json.Json.obj()

        (0 to 100).foreach { n =>
          jsObj += (n.toString -> JsNumber(n * n))
        }
      }
    }
    true
  }
}
