package com.kakao.s2graph.core

/**
 * Created by shon on 6/26/15.
 */
//case class QueryResultLs(queryResults: Seq[QueryResult] = Seq.empty[QueryResult])
object QueryResult {
  def fromVertices(query: Query, stepIdx: Int, queryParams: Seq[QueryParam], vertices: Seq[Vertex]): Seq[QueryResult] = {
    for {
      vertex <- vertices
      queryParam <- queryParams
    } yield QueryResult(query, stepIdx, queryParam, Seq((Edge(vertex, vertex, queryParam.labelWithDir), Graph.DEFAULT_SCORE)))
  }
}

case class QueryResult(query: Query,
                       stepIdx: Int,
                       queryParam: QueryParam,
                       edgeWithScoreLs: Seq[(Edge, Double)] = Seq.empty[(Edge, Double)],
                       timestamp: Long = System.currentTimeMillis())

case class EdgeWithScore(edge: Edge, score: Double)
