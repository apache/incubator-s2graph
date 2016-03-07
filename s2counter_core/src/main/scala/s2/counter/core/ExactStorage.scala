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

import s2.counter.core.ExactCounter.ExactValueMap
import s2.models.Counter

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by shon on 8/12/15.
 */
trait ExactStorage {
  // for range query and check dimension
  def get(policy: Counter,
          items: Seq[String],
          timeRange: Seq[(TimedQualifier, TimedQualifier)],
          dimQuery: Map[String, Set[String]])
         (implicit ec: ExecutionContext): Future[Seq[FetchedCountsGrouped]]
  // for range query
  def get(policy: Counter,
          items: Seq[String],
          timeRange: Seq[(TimedQualifier, TimedQualifier)])
         (implicit ec: ExecutionContext): Future[Seq[FetchedCounts]]
  // for query exact qualifier
  def get(policy: Counter,
          queries: Seq[(ExactKeyTrait, Seq[ExactQualifier])])
         (implicit ec: ExecutionContext): Future[Seq[FetchedCounts]]
  def update(policy: Counter, counts: Seq[(ExactKeyTrait, ExactValueMap)]): Map[ExactKeyTrait, ExactValueMap]
  def delete(policy: Counter, keys: Seq[ExactKeyTrait]): Unit
  def insertBlobValue(policy: Counter, keys: Seq[BlobExactKey]): Seq[Boolean]
  def getBlobValue(policy: Counter, blobId: String): Option[String]

  def prepare(policy: Counter): Unit
  def destroy(policy: Counter): Unit
  def ready(policy: Counter): Boolean
}
