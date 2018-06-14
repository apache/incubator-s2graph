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

package org.apache.s2graph.core.fetcher.fasttext

import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.utils.logger

import scala.concurrent.{ExecutionContext, Future}


class FastTextFetcher(val graph: S2GraphLike) extends EdgeFetcher {
  import org.apache.s2graph.core.TraversalHelper._
  val builder = graph.elementBuilder
  var fastText: FastText = _

  override def init(config: Config)(implicit ec: ExecutionContext): Unit = {
    val dbPath = config.getString(FastText.DBPathKey)

    try {
      fastText = new FastText(dbPath)
    } catch {
      case e: Throwable =>
        logger.error(s"[Init]: Failed.", e)
        println(e)
        throw e
    }
  }

  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val stepResultLs = queryRequests.map { queryRequest =>
      val vertex = queryRequest.vertex
      val queryParam = queryRequest.queryParam
      val shouldBuildParents = queryRequest.query.queryOption.returnTree || queryParam.whereHasParent
      val parentEdges = if (shouldBuildParents) prevStepEdges.getOrElse(queryRequest.vertex.id, Nil) else Nil

      val line = fastText.getLine(vertex.innerId.toIdString())

      val edgeWithScores = fastText.predict(line, queryParam.limit).flatMap { case (_label, score) =>
        val tgtVertexId = builder.newVertexId(queryParam.label.service,
          queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir), _label)

        val props: Map[String, Any] = if (queryParam.label.metaPropsInvMap.contains("score")) Map("score" -> score) else Map.empty
        val edge = graph.toEdge(vertex.innerId.value, tgtVertexId.innerId.value, queryParam.labelName, queryParam.direction, props = props)

        edgeToEdgeWithScore(queryRequest, edge, parentEdges)
      }

      StepResult(edgeWithScores, Nil, Nil)
    }

    Future.successful(stepResultLs)
  }

  override def close(): Unit = if (fastText != null) fastText.close()

  // not supported yet.
  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] =
    Future.successful(Nil)
}
