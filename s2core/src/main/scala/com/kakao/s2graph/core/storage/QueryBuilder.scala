package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core.{QueryResult, QueryRequest}

trait QueryBuilder[R, T] {

  def buildRequest(queryRequest: QueryRequest): R

  def fetch(queryRequest: QueryRequest): T

}
