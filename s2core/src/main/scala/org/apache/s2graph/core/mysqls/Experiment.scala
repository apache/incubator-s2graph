/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.mysqls

import org.apache.s2graph.core.GraphUtil
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
