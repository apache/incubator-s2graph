package com.daumkakao.s2graph.core.mysqls

import scalikejdbc._

/**
 * Created by shon on 8/5/15.
 */
object ServiceExperiment extends Model[ServiceExperiment] {

  def apply(rs: WrappedResultSet): ServiceExperiment = {
    ServiceExperiment(rs.intOpt("id"), rs.int("service_id"),
      rs.string("experiment_key"), rs.int("total_mode"), rs.string("description"))
  }

  def find(serviceId: Int, experimentKey: String): Option[ServiceExperiment] = {
    val cacheKey = "serviceId=" + serviceId + ":experimentKey=" + experimentKey
    withCache(cacheKey)(
      sql"""select * from service_experiments where service_id = ${serviceId} and ${experimentKey}"""
        .map { rs => ServiceExperiment(rs)}.single.apply
    )
  }
}

case class ServiceExperiment(id: Option[Int], serviceId: Int, expreimentKey: String,
                             totalMod: Int, description: String) {
  val buckets = ServiceExperimentBucket.finds(id.get)
  val bucketModMap = (for {
    bucket <- buckets
    mod <- bucket.cookieModLs
  } yield mod -> bucket).toMap
}