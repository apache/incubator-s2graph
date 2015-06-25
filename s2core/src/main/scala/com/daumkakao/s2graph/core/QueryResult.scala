package com.daumkakao.s2graph.core

/**
 * Created by shon on 6/26/15.
 */
//case class QueryResultLs(queryResults: Seq[QueryResult] = Seq.empty[QueryResult])
case class QueryResult(queryParam: QueryParam,
                  edgeWithScoreLs: Iterable[(Edge, Double)] = Seq.empty[(Edge, Double)])
