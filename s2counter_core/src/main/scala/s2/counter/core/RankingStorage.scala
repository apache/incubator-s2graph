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

package s2.counter.core

import s2.counter.core.RankingCounter.RankingValueMap
import s2.models.Counter

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 22..
 */
trait RankingStorage {
  def getTopK(key: RankingKey, k: Int): Option[RankingResult]
  def getTopK(keys: Seq[RankingKey], k: Int): Seq[(RankingKey, RankingResult)]
  def update(key: RankingKey, value: RankingValueMap, k: Int): Unit
  def update(values: Seq[(RankingKey, RankingValueMap)], k: Int): Unit
  def delete(key: RankingKey)

  def prepare(policy: Counter): Unit
  def destroy(policy: Counter): Unit
  def ready(policy: Counter): Boolean
}
