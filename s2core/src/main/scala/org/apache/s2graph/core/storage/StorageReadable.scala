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

package org.apache.s2graph.core.storage

import org.apache.s2graph.core.GraphExceptions.FetchTimeoutException
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.utils.logger

import scala.concurrent.{ExecutionContext, Future}

trait StorageReadable {
  val io: StorageIO
 /**
    * responsible to fire parallel fetch call into storage and create future that will return merged result.
    *
    * @param queryRequests
    * @param prevStepEdges
    * @return
    */
  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]]

// private def fetchKeyValues(rpc: Q)(implicit ec: ExecutionContext): Future[Seq[SKeyValue]]
  def fetchVertices(vertices: Seq[S2Vertex])(implicit ec: ExecutionContext): Future[Seq[S2Vertex]]

  def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2Edge]]

  def fetchVerticesAll()(implicit ec: ExecutionContext): Future[Seq[S2Vertex]]

  protected def fetchKeyValues(queryRequest: QueryRequest, edge: S2Edge)(implicit ec: ExecutionContext): Future[Seq[SKeyValue]]

  protected def fetchKeyValues(queryRequest: QueryRequest, vertex: S2Vertex)(implicit ec: ExecutionContext): Future[Seq[SKeyValue]]

  def fetchSnapshotEdgeInner(edge: S2Edge)(implicit ec: ExecutionContext): Future[(Option[S2Edge], Option[SKeyValue])] = {
    val queryParam = QueryParam(labelName = edge.innerLabel.label,
      direction = GraphUtil.fromDirection(edge.labelWithDir.dir),
      tgtVertexIdOpt = Option(edge.tgtVertex.innerIdVal),
      cacheTTLInMillis = -1)
    val q = Query.toQuery(Seq(edge.srcVertex), Seq(queryParam))
    val queryRequest = QueryRequest(q, 0, edge.srcVertex, queryParam)

    fetchKeyValues(queryRequest, edge).map { kvs =>
      val (edgeOpt, kvOpt) =
        if (kvs.isEmpty) (None, None)
        else {
          import CanSKeyValue._
          val snapshotEdgeOpt = io.toSnapshotEdge(kvs.head, queryRequest, isInnerCall = true, parentEdges = Nil)
          val _kvOpt = kvs.headOption
          (snapshotEdgeOpt, _kvOpt)
        }
      (edgeOpt, kvOpt)
    } recoverWith { case ex: Throwable =>
      logger.error(s"fetchQueryParam failed. fallback return.", ex)
      throw new FetchTimeoutException(s"${edge.toLogString}")
    }
  }
}
