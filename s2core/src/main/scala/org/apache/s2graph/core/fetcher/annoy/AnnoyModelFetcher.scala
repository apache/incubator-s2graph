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

package org.apache.s2graph.core.fetcher.annoy

import annoy4s.Converters.KeyConverter
import annoy4s._
import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.VertexId

import scala.concurrent.{ExecutionContext, Future}

object AnnoyModelFetcher {
  val IndexFilePathKey = "annoyIndexFilePath"
  val DimensionKey = "annoyIndexDimension"
  val IndexTypeKey = "annoyIndexType"

  def buildAnnoy4s[T](indexPath: String)(implicit converter: KeyConverter[T]): Annoy[T] = {
    Annoy.load[T](indexPath)
  }
}

class AnnoyModelFetcher(val graph: S2GraphLike) extends EdgeFetcher {
  import AnnoyModelFetcher._
  import TraversalHelper._

  val builder = graph.elementBuilder

  var model: Annoy[String] = _

  override def init(config: Config)(implicit ec: ExecutionContext): Unit = {
    model = AnnoyModelFetcher.buildAnnoy4s(config.getString(IndexFilePathKey))
  }

  /** Fetch **/
  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val stepResultLs = queryRequests.map { queryRequest =>
      val vertex = queryRequest.vertex
      val queryParam = queryRequest.queryParam
      val shouldBuildParents = queryRequest.query.queryOption.returnTree || queryParam.whereHasParent
      val parentEdges = if (shouldBuildParents) prevStepEdges.getOrElse(queryRequest.vertex.id, Nil) else Nil

      val edgeWithScores = model.query(vertex.innerId.toIdString(), queryParam.limit).getOrElse(Nil).flatMap { case (tgtId, score) =>
        val tgtVertexId = builder.newVertexId(queryParam.label.service,
          queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir), tgtId)

        val props: Map[String, Any] = if (queryParam.label.metaPropsInvMap.contains("score")) Map("score" -> score) else Map.empty
        val edge = graph.toEdge(vertex.innerId.value, tgtVertexId.innerId.value, queryParam.labelName, queryParam.direction, props = props)

        edgeToEdgeWithScore(queryRequest, edge, parentEdges)
      }

      StepResult(edgeWithScores, Nil, Nil)
    }

    Future.successful(stepResultLs)
  }

  override def close(): Unit = {
    // do clean up
    model.close
  }

  // not supported yet.
  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] =
    Future.successful(Nil)
}
