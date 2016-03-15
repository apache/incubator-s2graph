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

package org.apache.s2graph.core

import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs}

import scala.collection.Seq

object QueryResult {
  def fromVertices(query: Query): Seq[QueryRequestWithResult] = {
    if (query.steps.isEmpty || query.steps.head.queryParams.isEmpty) {
      Seq.empty
    } else {
      val queryParam = query.steps.head.queryParams.head
      val label = queryParam.label
      val currentTs = System.currentTimeMillis()
      val propsWithTs = Map(LabelMeta.timeStampSeq ->
        InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs))
      for {
        vertex <- query.vertices
      } yield {
        val edge = Edge(vertex, vertex, queryParam.labelWithDir, propsWithTs = propsWithTs)
        val edgeWithScore = EdgeWithScore(edge, Graph.DefaultScore)
        QueryRequestWithResult(QueryRequest(query, -1, vertex, queryParam),
          QueryResult(edgeWithScoreLs = Seq(edgeWithScore)))
      }
    }
  }
}
case class QueryRequestWithResult(queryRequest: QueryRequest, queryResult: QueryResult)

case class QueryRequest(query: Query,
                        stepIdx: Int,
                        vertex: Vertex,
                        queryParam: QueryParam)


case class QueryResult(edgeWithScoreLs: Seq[EdgeWithScore] = Nil,
                       tailCursor: Array[Byte] = Array.empty,
                       timestamp: Long = System.currentTimeMillis(),
                       isFailure: Boolean = false)

case class EdgeWithScore(edge: Edge, score: Double)
