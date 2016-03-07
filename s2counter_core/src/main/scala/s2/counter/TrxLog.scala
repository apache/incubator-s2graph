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

package s2.counter

import play.api.libs.json.Json

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 4. 7..
 */
// item1 -> likedCount -> month:2015-10, 1
// edge
  // policyId = Label.findByName(likedCount).id.get
  // item = edge.srcVertexId
  // results =
case class TrxLog(success: Boolean, policyId: Int, item: String, results: Iterable[TrxLogResult])

// interval = m, ts = 2015-10, "age.gender.20.M", 1, 2
case class TrxLogResult(interval: String, ts: Long, dimension: String, value: Long, result: Long = -1)

object TrxLogResult {
  implicit val writes = Json.writes[TrxLogResult]
  implicit val reads = Json.reads[TrxLogResult]
  implicit val formats = Json.format[TrxLogResult]
}

object TrxLog {
  implicit val writes = Json.writes[TrxLog]
  implicit val reads = Json.reads[TrxLog]
  implicit val formats = Json.format[TrxLog]
}
