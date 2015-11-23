package com.kakao.s2graph.core.mysqls

/**
 * Created by shon on 8/5/15.
 */

import scalikejdbc._

object Bucket extends Model[Bucket] {

  val rangeDelimiter = "~"
  val INVALID_BUCKET_EXCEPTION = new RuntimeException("invalid bucket.")

  def apply(rs: WrappedResultSet): Bucket = {
    Bucket(rs.intOpt("id"),
      rs.int("experiment_id"),
      rs.string("modular"),
      rs.string("http_verb"),
      rs.string("api_path"),
      rs.string("request_body"),
      rs.int("timeout"),
      rs.string("impression_id"),
      rs.boolean("is_graph_query"),
      rs.boolean("is_empty"))
  }

  def finds(experimentId: Int)(implicit session: DBSession = AutoSession): List[Bucket] = {
    val cacheKey = "experimentId=" + experimentId
    withCaches(cacheKey) {
      sql"""select * from buckets where experiment_id = $experimentId"""
        .map { rs => Bucket(rs) }.list().apply()
    }
  }

  def toRange(str: String): Option[(Int, Int)] = {
    val range = str.split(rangeDelimiter)
    if (range.length == 2) Option(range.head.toInt, range.last.toInt)
    else None
  }

  def findByImpressionId(impressionId: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[Bucket] = {
    val cacheKey = "impressionId=" + impressionId
    val sql = sql"""select * from buckets where impression_id=$impressionId"""
      .map { rs => Bucket(rs)}
    if (useCache) {
      withCache(cacheKey) {
        sql.single().apply()
      }
    } else {
      sql.single().apply()
    }
  }
}

case class Bucket(id: Option[Int],
                  experimentId: Int,
                  modular: String,
                  httpVerb: String, apiPath: String,
                  requestBody: String, timeout: Int, impressionId: String,
                  isGraphQuery: Boolean = true,
                  isEmpty: Boolean = false) {

  import Bucket._

  lazy val rangeOpt = toRange(modular)
}
