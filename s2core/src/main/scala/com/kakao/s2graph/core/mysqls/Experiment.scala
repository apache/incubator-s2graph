package com.kakao.s2graph.core.mysqls

import com.kakao.s2graph.core.GraphUtil
import com.kakao.s2graph.core.utils.logger
import scalikejdbc._

import scala.util.Random

object Experiment extends Model[Experiment] {
  val impressionKey = "S2-Impression-Id"

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

  val findVar = """\"?\$\{(.*?)\}\"?""".r
  val num = """(-?[0-9]+)\s*?(hour|day)""".r

  val hour = 60 * 60 * 1000L
  val day = hour * 24L

  def calculate(now: Long, n: Int, unit: String): Long = {
    val duration = unit match {
      case "hour" | "HOUR" => n * hour
      case "day" | "DAY" => n * day
      case _ => n * day
    }

    duration + now
  }

  // TODO: REFACTOR-RENAME
  def replaceVariable(now: Long, body: String): String = {
    findVar.replaceAllIn(body, m => {
      val matched = m group 1
      if (matched == "now" || matched == "NOW") now.toString
      else num.replaceAllIn(matched, m => {
        val (n, unit) = (m.group(1).toInt, m.group(2))
        calculate(now, n, unit).toString
      })

    })
  }
}

case class Experiment(id: Option[Int],
                      serviceId: Int,
                      name: String,
                      description: String,
                      experimentType: String,
                      totalModular: Int) {

  def buckets = Bucket.finds(id.get)

  def rangeBuckets = for {
    bucket <- buckets
    range <- bucket.rangeOpt
  } yield range -> bucket


  def findBucket(uuid: String, impIdOpt: Option[String] = None): Option[Bucket] = {
    impIdOpt match {
      case Some(impId) => Bucket.findByImpressionId(impId)
      case None =>
        val seed = experimentType match {
          case "u" => (GraphUtil.murmur3(uuid) % totalModular) + 1
          case _ => Random.nextInt(totalModular) + 1
        }
        findBucket(seed)
    }
  }

  def findBucket(uuidMod: Int): Option[Bucket] = {
    rangeBuckets.find { case ((from, to), bucket) =>
      from <= uuidMod && uuidMod <= to
    }.map(_._2)
  }
}
