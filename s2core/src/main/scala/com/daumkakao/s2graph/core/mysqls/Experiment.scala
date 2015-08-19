package com.daumkakao.s2graph.core.mysqls

import com.daumkakao.s2graph.core.GraphUtil
import scalikejdbc._

import scala.util.Random

/**
 * Created by shon on 8/5/15.
 */
object Experiment extends Model[Experiment] {
  def apply(rs: WrappedResultSet): Experiment = {
    Experiment(rs.intOpt("id"),
      rs.int("service_id"),
      rs.string("name"),
      rs.string("description"),
      rs.string("experiment_type"),
      rs.int("total_modular"))
  }

  def finds(serviceId: Int)(implicit session: DBSession = AutoSession): List[Experiment] = {
    val cacheKey = "serviceId=" + serviceId
    withCaches(cacheKey) {
      sql"""select * from experiments where service_id = ${serviceId}"""
        .map { rs => Experiment(rs) }.list().apply()
    }
  }

  def findBy(serviceId: Int, name: String)(implicit session: DBSession = AutoSession): Option[Experiment] = {
    val cacheKey = "serviceId=" + serviceId + ":name=" + name
    withCache(cacheKey) {
      sql"""select * from experiments where service_id = ${serviceId} and name = ${name}"""
        .map { rs => Experiment(rs) }.single.apply
    }
  }

  def findById(id: Int)(implicit session: DBSession = AutoSession): Option[Experiment] = {
    val cacheKey = "id=" + id
    withCache(cacheKey)(
      sql"""select * from experiments where id = ${id}"""
        .map { rs => Experiment(rs) }.single.apply
    )
  }
}

case class Experiment(id: Option[Int],
                      serviceId: Int,
                      name: String,
                      description: String,
                      experimentType: String,
                      totalModular: Int) {

  val buckets = Bucket.finds(id.get)
  val rangeBuckets = for {
    bucket <- buckets
    range <- experimentType match {
      case "u" => bucket.uuidRangeOpt
      case _ => bucket.trafficRangeOpt
    }
  } yield range -> bucket


  def findBucket(uuid: String): Option[Bucket] = {
    val seed = experimentType match {
      case "u" => (GraphUtil.murmur3(uuid) % totalModular) + 1
      case _ => (Random.nextInt(totalModular)) + 1
    }

    findBucket(seed)
  }

  def findBucket(uuidMod: Int): Option[Bucket] = {
    rangeBuckets.find { case ((from, to), bucket) =>
      from <= uuidMod && uuidMod <= to
    }.map(_._2)
  }
}
