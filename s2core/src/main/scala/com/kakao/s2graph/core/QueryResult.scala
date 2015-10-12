package com.kakao.s2graph.core

/**
 * Created by shon on 6/26/15.
 */
//case class QueryResultLs(queryResults: Seq[QueryResult] = Seq.empty[QueryResult])
object QueryResult {
  def fromVertices(query: Query): Seq[QueryResult] = {
    val queryParam =  query.steps.head.queryParams.head
    for {
      vertex <- query.vertices
    } yield {
      val edgeWithScoreLs = Seq((Edge(vertex, vertex, queryParam.labelWithDir), Graph.DefaultScore))
      QueryResult(query, stepIdx = -1, queryParam = queryParam, edgeWithScoreLs = edgeWithScoreLs)
    }
  }
}

case class QueryResult(query: Query,
                       stepIdx: Int,
                       queryParam: QueryParam,
                       edgeWithScoreLs: Seq[(Edge, Double)] = Seq.empty[(Edge, Double)],
                       timestamp: Long = System.currentTimeMillis())

case class EdgeWithScore(edge: Edge, score: Double)
