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

package models

import play.api.libs.json.{Json, Writes}

/**
 * Created by alec on 15. 4. 15..
 */
case class ExactCounterItem(ts: Long, count: Long, score: Double)

case class ExactCounterIntervalItem(interval: String, dimension: Map[String, String], counter: Seq[ExactCounterItem])

case class ExactCounterResultMeta(service: String, action: String, item: String)

case class ExactCounterResult(meta: ExactCounterResultMeta, data: Seq[ExactCounterIntervalItem])

object ExactCounterItem {
  implicit val writes = new Writes[ExactCounterItem] {
    def writes(item: ExactCounterItem) = Json.obj(
      "ts" -> item.ts,
      "time" -> tsFormat.format(item.ts),
      "count" -> item.count,
      "score" -> item.score
    )
  }
  implicit val reads = Json.reads[ExactCounterItem]
}

object ExactCounterIntervalItem {
  implicit val format = Json.format[ExactCounterIntervalItem]
}

object ExactCounterResultMeta {
  implicit val format = Json.format[ExactCounterResultMeta]
}

object ExactCounterResult {
  implicit val formats = Json.format[ExactCounterResult]
}
