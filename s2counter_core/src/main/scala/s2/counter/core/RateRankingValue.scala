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

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 7. 2..
 */
case class RateRankingValue(actionScore: Double, baseScore: Double) {
  // increment score do not use.
  lazy val rankingValue: RankingValue = {
    RankingValue(actionScore / math.max(1d, baseScore), 0)
  }
}

object RateRankingValue {
  def reduce(r1: RateRankingValue, r2: RateRankingValue): RateRankingValue = {
    // maximum score and sum of increment
    RateRankingValue(math.max(r1.actionScore, r2.actionScore), math.max(r1.baseScore, r2.baseScore))
  }
}
