package com.daumkakao.s2graph.core.mysqls

/**
 * Created by shon on 8/5/15.
 */


import play.api.libs.json.Json
import play.api.libs.ws._
import scalikejdbc._

import scala.concurrent.Future

object ServiceExperimentBucket extends Model[ServiceExperimentBucket] {

  def apply(rs: WrappedResultSet): ServiceExperimentBucket = {
    ServiceExperimentBucket(rs.intOpt("id"),
    rs.int("service_experiment_id"),
    rs.string("cookie_mods"),
    rs.float("probability"),
    rs.string("http_verb"),
    rs.string("api_path"),
    rs.stringOpt("request_params"),
    rs.stringOpt("request_body"),
    rs.stringOpt("request_header"),
    rs.string("impression_id"))
  }

  def finds(serviceExperimentId: Int): List[ServiceExperimentBucket] = {
    val cacheKey = "serviceExperimentId=" + serviceExperimentId
    withCaches(cacheKey) {
      sql"""select * from service_experiment_buckets where service_experiment_id = ${serviceExperimentId}"""
      .map { rs => ServiceExperimentBucket(rs) }.list().apply()
    }
  }


}

case class ServiceExperimentBucket(id: Option[Int],
                                    serviceExperimentId: Int,
                                    cookieMods: String,
                                    probability: Float,
                                    httpVerb: String,
                                    apiPath: String,
                                    requestParams: Option[String],
                                    requestBody: Option[String],
                                    requestHeader: Option[String],
                                    impressionId: String) {

  val cookieModLs = cookieMods.split(",").map { x => x.toInt }
  private def toSeq(sOpt: Option[String])(delimiter: String, innerDelimiter: String) = {
    sOpt.map { s =>
      val kvs = for {
        kv <- s.split(delimiter)
        token = kv.split(innerDelimiter) if token.length == 2
      } yield {
        token.head -> token.last
      }
      kvs.toSeq
    }.getOrElse(Seq.empty)
  }
  def call(cookie: String): Future[WSResponse] = {
    val holder = WS.url(apiPath)
    val body = requestBody.map(b => Json.parse(b) ).getOrElse(Json.obj())
    holder.withHeaders(toSeq(requestHeader)(";", ":") : _*).post(body)
  }
}