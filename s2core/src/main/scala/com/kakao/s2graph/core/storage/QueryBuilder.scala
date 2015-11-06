package com.kakao.s2graph.core.storage

import com.google.common.cache.Cache
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.types.{LabelWithDirection, VertexId}
import com.kakao.s2graph.core.utils.logger
import scala.collection.{Map, Seq}
import scala.concurrent.{Future, ExecutionContext}
import scala.util.Try

abstract class QueryBuilder[R, T](storage: Storage)(implicit ec: ExecutionContext) {

  def buildRequest(queryRequest: QueryRequest): R

  def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): T

  def fetch(queryRequest: QueryRequest): T

  def toCacheKeyBytes(request: R): Array[Byte]

  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryResult]]


  def fetchStep(queryResultsLs: Seq[QueryResult],
                q: Query,
                stepIdx: Int): Future[Seq[QueryResult]] = {

    val prevStepOpt = if (stepIdx > 0) Option(q.steps(stepIdx - 1)) else None
    val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.DefaultThreshold)
    val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
    val step = q.steps(stepIdx)
    val alreadyVisited =
      if (stepIdx == 0) Map.empty[(LabelWithDirection, Vertex), Boolean]
      else Graph.alreadyVisitedVertices(queryResultsLs)

    val groupedBy = queryResultsLs.flatMap { queryResult =>
      queryResult.edgeWithScoreLs.map { case edgeWithScore =>
        edgeWithScore.edge.tgtVertex -> edgeWithScore
      }
    }.groupBy { case (vertex, edgeWithScore) => vertex }

    val groupedByFiltered = for {
      (vertex, edgesWithScore) <- groupedBy
      aggregatedScore = edgesWithScore.map(_._2.score).sum if aggregatedScore >= prevStepThreshold
    } yield vertex -> aggregatedScore

    val prevStepTgtVertexIdEdges = for {
      (vertex, edgesWithScore) <- groupedBy
    } yield vertex.id -> edgesWithScore.map { case (vertex, edgeWithScore) => edgeWithScore }

    val nextStepSrcVertices = if (prevStepLimit >= 0) {
      groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
    } else {
      groupedByFiltered.toSeq
    }

    val queryRequests = for {
      (vertex, prevStepScore) <- nextStepSrcVertices
      queryParam <- step.queryParams
    } yield QueryRequest(q, stepIdx, vertex, queryParam, prevStepScore, None, Nil, isInnerCall = false)

    Graph.filterEdges(fetches(queryRequests, prevStepTgtVertexIdEdges), q, stepIdx, alreadyVisited)(ec)
  }

  def fetchStepFuture(queryResultLsFuture: Future[Seq[QueryResult]],
                      q: Query,
                      stepIdx: Int): Future[Seq[QueryResult]] = {
    for {
      queryResultLs <- queryResultLsFuture
      ret <- fetchStep(queryResultLs, q, stepIdx)
    } yield ret
  }

  def getEdges(q: Query): Future[Seq[QueryResult]] = {
    Try {
      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        Future.successful(q.vertices.map(v => QueryResult(query = q, stepIdx = 0, queryParam = QueryParam.Empty)))
      } else {
        val startQueryResultLs = QueryResult.fromVertices(q)
        q.steps.zipWithIndex.foldLeft(Future.successful(startQueryResultLs)) { case (acc, (_, idx)) =>
          fetchStepFuture(acc, q, idx)
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        Future.successful(q.vertices.map(v => QueryResult(query = q, stepIdx = 0, queryParam = QueryParam.Empty)))
    } get
  }
}
