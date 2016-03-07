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
case class RankCounterItem(rank: Int, id: String, score: Double)

case class RankCounterDimensionItem(interval: String, ts: Long, dimension: String, total: Double, ranks: Seq[RankCounterItem])

case class RankCounterResultMeta(service: String, action: String)

case class RankCounterResult(meta: RankCounterResultMeta, data: Seq[RankCounterDimensionItem])

object RankCounterItem {
  implicit val format = Json.format[RankCounterItem]
}

object RankCounterDimensionItem {
  implicit val writes = new Writes[RankCounterDimensionItem] {
    def writes(item: RankCounterDimensionItem) = Json.obj(
      "interval" -> item.interval,
      "ts" -> item.ts,
      "time" -> tsFormat.format(item.ts),
      "dimension" -> item.dimension,
      "total" -> item.total,
      "ranks" -> item.ranks
    )
  }
  implicit val reads = Json.reads[RankCounterDimensionItem]
}

object RankCounterResultMeta {
  implicit val format = Json.format[RankCounterResultMeta]
}

object RankCounterResult {
  implicit val format = Json.format[RankCounterResult]
}
