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

package org.apache.s2graph.core.storage.hbase

import java.util

import com.stumbleupon.async.Deferred
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.{StorageIO, StorageSerDe}
import org.apache.s2graph.core.types.{HBaseType, VertexId}
import org.apache.s2graph.core.utils.{CanDefer, DeferCache, Extensions, logger}
import org.hbase.async._

import scala.concurrent.ExecutionContext

class AsynchbaseEdgeFetcher(val graph: S2GraphLike,
                            val config: Config,
                            val client: HBaseClient,
                            val serDe: StorageSerDe,
                            val io: StorageIO) extends EdgeFetcher {

  import AsynchbaseStorage._
  import CanDefer._
  import Extensions.DeferOps

  import scala.collection.JavaConverters._

  /** Future Cache to squash request */
  lazy private val futureCache = new DeferCache[StepResult, Deferred, Deferred](config, StepResult.Empty, "AsyncHbaseFutureCache", useMetric = true)

  override def fetches(queryRequests: Seq[QueryRequest], prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext) = {
    val defers: Seq[Deferred[StepResult]] = for {
      queryRequest <- queryRequests
    } yield {
      val queryOption = queryRequest.query.queryOption
      val queryParam = queryRequest.queryParam
      val shouldBuildParents = queryOption.returnTree || queryParam.whereHasParent
      val parentEdges = if (shouldBuildParents) prevStepEdges.getOrElse(queryRequest.vertex.id, Nil) else Nil
      fetch(queryRequest, isInnerCall = false, parentEdges)
    }

    val grouped: Deferred[util.ArrayList[StepResult]] = Deferred.groupInOrder(defers.asJava)
    grouped.map(emptyStepResult) { queryResults: util.ArrayList[StepResult] =>
      queryResults
    }.toFuture(emptyStepResult).map(_.asScala)
  }

  /**
    * we are using future cache to squash requests into same key on storage.
    *
    * @param queryRequest
    * @param isInnerCall
    * @param parentEdges
    * @return we use Deferred here since it has much better performrance compared to scala.concurrent.Future.
    *         seems like map, flatMap on scala.concurrent.Future is slower than Deferred's addCallback
    */
  private def fetch(queryRequest: QueryRequest,
                    isInnerCall: Boolean,
                    parentEdges: Seq[EdgeWithScore])(implicit ec: ExecutionContext): Deferred[StepResult] = {

    def fetchInner(hbaseRpc: AsyncRPC): Deferred[StepResult] = {
      val prevStepScore = queryRequest.prevStepScore
      val fallbackFn: (Exception => StepResult) = { ex =>
        logger.error(s"fetchInner failed. fallback return. $hbaseRpc}", ex)
        StepResult.Failure
      }

      val queryParam = queryRequest.queryParam
      AsynchbaseStorage.fetchKeyValuesInner(client, hbaseRpc).mapWithFallback(emptyKeyValues)(fallbackFn) { _kvs =>
        val kvs = _kvs.asScala
        val (startOffset, len) = queryParam.label.schemaVersion match {
          case HBaseType.VERSION4 =>
            val offset = if (queryParam.cursorOpt.isDefined) 0 else queryParam.offset
            (offset, queryParam.limit)
          case _ => (0, kvs.length)
        }

        io.toEdges(kvs, queryRequest, prevStepScore, isInnerCall, parentEdges, startOffset, len)
      }
    }

    val queryParam = queryRequest.queryParam
    val cacheTTL = queryParam.cacheTTLInMillis
    /* with version 4, request's type is (Scanner, (Int, Int)). otherwise GetRequest. */

    val edge = graph.elementBuilder.toRequestEdge(queryRequest, parentEdges)
    val request = buildRequest(client, serDe, queryRequest, edge)

    val (intervalMaxBytes, intervalMinBytes) = queryParam.buildInterval(Option(edge))
    val requestCacheKey = Bytes.add(toCacheKeyBytes(request), intervalMaxBytes, intervalMinBytes)

    if (cacheTTL <= 0) fetchInner(request)
    else {
      val cacheKeyBytes = Bytes.add(queryRequest.query.queryOption.cacheKeyBytes, requestCacheKey)

      //      val cacheKeyBytes = toCacheKeyBytes(request)
      val cacheKey = queryParam.toCacheKey(cacheKeyBytes)
      futureCache.getOrElseUpdate(cacheKey, cacheTTL)(fetchInner(request))
    }
  }
}
