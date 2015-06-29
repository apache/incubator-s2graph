package com.daumkakao.s2graph.core

/**
 * Created by shon on 6/26/15.
 */
//case class QueryResultLs(queryResults: Seq[QueryResult] = Seq.empty[QueryResult])
object QueryResult {
  def fromVertices(vertices: Seq[Vertex], queryParams: Seq[QueryParam]): Seq[QueryResult] = {
    for {
      vertex <- vertices
      queryParam <- queryParams
    } yield QueryResult(queryParam, Seq((Edge(vertex, vertex, queryParam.labelWithDir), Graph.defaultScore)))
  }
}

case class QueryResult(queryParam: QueryParam,
                       edgeWithScoreLs: Iterable[(Edge, Double)] = Seq.empty[(Edge, Double)],
                       timestamp: Long = System.currentTimeMillis()) {

}
