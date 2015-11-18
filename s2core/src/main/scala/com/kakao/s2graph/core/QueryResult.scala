package com.kakao.s2graph.core

import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.types.{InnerVal, InnerValLikeWithTs}

object QueryResult {
  def fromVertices(query: Query): Seq[QueryResult] = {
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
      QueryResult(query, stepIdx = -1, queryParam = queryParam, edgeWithScoreLs = Seq(edgeWithScore))
    }
  }
}

case class QueryResult(query: Query,
                       stepIdx: Int,
                       queryParam: QueryParam,
                       edgeWithScoreLs: Seq[EdgeWithScore] = Nil,
                       timestamp: Long = System.currentTimeMillis(),
                       isFailure: Boolean = false)

case class EdgeWithScore(edge: Edge, score: Double)
