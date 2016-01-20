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

  def fetch(queryRequest: QueryRequest,
            prevStepScore: Double,
            isInnerCall: Boolean,
            parentEdges: Seq[EdgeWithScore]): T

  def toCacheKeyBytes(request: R): Array[Byte]

  def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]]


  def fetchStep(orgQuery: Query, queryRequestWithResultsLs: Seq[QueryRequestWithResult]): Future[Seq[QueryRequestWithResult]] = {
    if (queryRequestWithResultsLs.isEmpty) Future.successful(Nil)
    else {
      val queryRequest = queryRequestWithResultsLs.head.queryRequest
      val q = orgQuery
      val queryResultsLs = queryRequestWithResultsLs.map(_.queryResult)

      val stepIdx = queryRequest.stepIdx + 1

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
      } yield (QueryRequest(q, stepIdx, vertex, queryParam), prevStepScore)

      Graph.filterEdges(fetches(queryRequests, prevStepTgtVertexIdEdges), alreadyVisited)(ec)
    }
  }

  def fetchStepFuture(orgQuery: Query, queryRequestWithResultLsFuture: Future[Seq[QueryRequestWithResult]]): Future[Seq[QueryRequestWithResult]] = {
    for {
      queryRequestWithResultLs <- queryRequestWithResultLsFuture
      ret <- fetchStep(orgQuery, queryRequestWithResultLs)
    } yield ret
  }

  def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = {
    val fallback = {
      val queryRequest = QueryRequest(query = q, stepIdx = 0, q.vertices.head, queryParam = QueryParam.Empty)
      Future.successful(q.vertices.map(v => QueryRequestWithResult(queryRequest, QueryResult())))
    }
    Try {
      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        fallback
      } else {
        // current stepIdx = -1
        val startQueryResultLs = QueryResult.fromVertices(q)
        q.steps.foldLeft(Future.successful(startQueryResultLs)) { case (acc, step) =>
          fetchStepFuture(q, acc)
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        fallback
    } get
  }
}
