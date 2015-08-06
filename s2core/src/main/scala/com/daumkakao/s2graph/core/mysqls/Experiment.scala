package com.daumkakao.s2graph.core.mysqls

import com.daumkakao.s2graph.core.GraphUtil
import play.api.Logger
import scalikejdbc._

import scala.util.Random

/**
 * Created by shon on 8/5/15.
 */
object Experiment extends Model[Experiment] {
  def apply(rs: WrappedResultSet): Experiment = {
    Experiment(rs.intOpt("id"),
      rs.string("service_name"),
      rs.string("title"),
      rs.string("experiment_key"),
      rs.string("description"),
      rs.string("experiment_type"),
      rs.int("total_modular"))
  }

  def finds(serviceName: String): List[Experiment] = {
    val cacheKey = "serviceName=" + serviceName
    withCaches(cacheKey) {
      sql"""select * from experiments where service_name = ${serviceName}""".map { rs => Experiment(rs) }.list().apply()
    }
  }

  def find(serviceName: String, experimentKey: String): Option[Experiment] = {
    val cacheKey = "serviceName=" + serviceName + ":experimentKey=" + experimentKey
    withCache(cacheKey)(
      sql"""select * from experiments where service_name = ${serviceName} and experiment_key = ${experimentKey}"""
        .map { rs => Experiment(rs) }.single.apply
    )
  }

  def findBucket(serviceName: String, experimentKey: String, uuid: String): Option[Bucket] = {
    for {
      experiement <- Experiment.find(serviceName, experimentKey)
      bucket <- experiement.findBucket(uuid)
    } yield {
      bucket
    }
  }

}

case class Experiment(id: Option[Int],
                      serviceName: String,
                      title: String,
                      experimentKey: String,
                      description: String,
                      experimentType: String,
                      totalModular: Int) {

  val buckets = Bucket.finds(id.get)
  val rangeBuckets = for {
      bucket <- buckets
      range <- experimentType match {
        case "user_id_modular" => bucket.uuidRangeOpt
        case _ => bucket.trafficRangeOpt
      }
    } yield range -> bucket


  def findBucket(uuid: String): Option[Bucket] = {
    val seed = experimentType match {
      case "user_id_modular" => GraphUtil.murmur3(uuid) % totalModular + 1
      case _ => Random.nextInt(totalModular) + 1
    }
    findBucket(seed)
  }

  def findBucket(uuidMod: Int): Option[Bucket] = {
    rangeBuckets.find { case ((from, to), bucket) =>
        from <= uuidMod && uuidMod < to
    }.map(_._2)
  }
}