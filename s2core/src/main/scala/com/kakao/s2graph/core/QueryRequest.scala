package com.kakao.s2graph.core

import scala.collection.Seq

case class QueryRequest(query: Query,
                        stepIdx: Int,
                        vertex: Vertex,
                        queryParam: QueryParam,
                        prevStepScore: Double,
                        tgtVertexOpt: Option[Vertex] = None,
                        parentEdges: Seq[EdgeWithScore] = Nil,
                        isInnerCall: Boolean = false)
