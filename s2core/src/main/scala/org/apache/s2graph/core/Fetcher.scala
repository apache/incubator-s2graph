package org.apache.s2graph.core

import com.typesafe.config.Config
import org.apache.s2graph.core.types.VertexId

import scala.concurrent.{ExecutionContext, Future}

trait Fetcher {

  def init(config: Config)(implicit ec: ExecutionContext): Future[Fetcher] =
    Future.successful(this)

  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]]

  def close(): Unit = {}
}
