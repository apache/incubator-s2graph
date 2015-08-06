package com.daumkakao.s2graph.core.mysqls

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
    rs.string("uuid_mods"),
    rs.string("traffic_ratios"),
    rs.string("http_verb"),
    rs.string("api_path"),
    rs.string("uuid_key"),
    rs.string("uuid_placeholder"),
    rs.string("request_body"),
    rs.int("timeout"),
    rs.string("impression_id"))
  }

  def finds(experimentId: Int): List[Bucket] = {
    val cacheKey = "serviceExperimentId=" + experimentId
    withCaches(cacheKey) {
      sql"""select * from buckets where experiment_id = ${experimentId}"""
      .map { rs => Bucket(rs) }.list().apply()
    }
  }

  def toRange(str: String): Option[(Int, Int)] = {
    val range = str.split(rangeDelimiter)
    if (range.length == 2) Option(range.head.toInt, range.last.toInt)
    else None
  }
  def toSimpleMap(map: Map[String, Seq[String]]) = {
    for {
      (k, vs) <- map
      headVal <- vs.headOption
    } yield {
      k -> headVal
    }
  }

}

case class Bucket(id: Option[Int],
                  experimentId: Int,
                  uuidMods: String,
                  trafficRatios: String,
                  httpVerb: String, apiPath: String, uuidKey: String, uuidPlaceHolder: String,
                  requestBody: String, timeout: Int, impressionId: String) {

  import Bucket._
  lazy val uuidRangeOpt = toRange(uuidMods)
  lazy val trafficRangeOpt = toRange(trafficRatios)



}