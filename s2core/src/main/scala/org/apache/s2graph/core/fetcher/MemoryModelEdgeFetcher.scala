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

package org.apache.s2graph.core.fetcher

import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.VertexId

import scala.concurrent.{ExecutionContext, Future}

/**
  * Reference implementation for Fetcher interface.
  * it only produce constant edges.
  */
class MemoryModelEdgeFetcher(val graph: S2GraphLike) extends EdgeFetcher {
  val builder = graph.elementBuilder
  val ranges = (0 until 10)


  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val stepResultLs = queryRequests.map { queryRequest =>
      toEdges(queryRequest)
    }

    Future.successful(stepResultLs)
  }

  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] = {
    Future.successful(Nil)
  }

  private def toEdges(queryRequest: QueryRequest) = {
    val queryParam = queryRequest.queryParam
    val edges = ranges.map { ith =>
      val tgtVertexId = builder.newVertexId(queryParam.label.service, queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir), ith.toString)

      graph.toEdge(queryRequest.vertex.innerIdVal,
        tgtVertexId.innerId.value, queryParam.label.label, queryParam.direction)
    }

    val edgeWithScores = edges.map(e => EdgeWithScore(e, 1.0, queryParam.label))
    StepResult(edgeWithScores, Nil, Nil)
  }
}
