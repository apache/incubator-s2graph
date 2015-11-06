package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core.{QueryResult, QueryRequest}

import scala.concurrent.ExecutionContext

trait QueryBuilder[R, T] {

  def buildRequest(queryRequest: QueryRequest): R

  def fetch(queryRequest: QueryRequest)(implicit ex: ExecutionContext): T

  def toCacheKeyBytes(request: R): Array[Byte]
}
