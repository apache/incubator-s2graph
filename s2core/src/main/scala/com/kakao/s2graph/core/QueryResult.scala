package com.kakao.s2graph.core

object QueryResult {
  def fromVertices(query: Query): Seq[QueryResult] = {
    val queryParam = query.steps.head.queryParams.head
    for {
      vertex <- query.vertices
    } yield {
      val edgeWithScore = EdgeWithScore(Edge(vertex, vertex, queryParam.labelWithDir), Graph.DefaultScore)
      QueryResult(query, stepIdx = -1, queryParam = queryParam, edgeWithScoreLs = Seq(edgeWithScore))
    }
  }
}

case class QueryResult(query: Query,
                       stepIdx: Int,
                       queryParam: QueryParam,
                       edgeWithScoreLs: Seq[EdgeWithScore] = Nil,
                       timestamp: Long = System.currentTimeMillis())

case class EdgeWithScore(edge: Edge, score: Double)
