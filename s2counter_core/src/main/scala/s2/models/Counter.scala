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

package s2.models

import com.typesafe.config.Config
import s2.config.S2CounterConfig
import s2.util.{CollectionCache, CollectionCacheConfig}
import scalikejdbc._

/**
*  Created by hsleep(honeysleep@gmail.com) on 15. 1. 30..
*/
case class Counter(id: Int, useFlag: Boolean, version: Byte, service: String, action: String,
                   itemType: Counter.ItemType.ItemType, autoComb: Boolean, dimension: String,
                   useProfile: Boolean, bucketImpId: Option[String],
                   useRank: Boolean,
                   ttl: Int, dailyTtl: Option[Int], hbaseTable: Option[String],
                   intervalUnit: Option[String],
                   rateActionId: Option[Int], rateBaseId: Option[Int], rateThreshold: Option[Int]) {
  val intervals: Array[String] = intervalUnit.map(s => s.split(',')).getOrElse(Array("t", "M", "d", "H"))
  val dimensionSp = if (dimension.isEmpty) Array.empty[String] else dimension.split(',').sorted

  val dimensionList: List[Array[String]] = {
    autoComb match {
      case true =>
        for {
          i <- (0 to math.min(4, dimensionSp.length)).toList
          combines <- dimensionSp.combinations(i)
        } yield {
          combines
        }
      case false =>
        dimensionSp isEmpty match {
          case true => List(Array())
          case false => dimensionSp.toList.map(sp => sp.split('.'))
        }
    }
  }

  val dimensionSet: Set[Set[String]] = {
    for {
      arr <- dimensionList
    } yield {
      arr.toSet
    }
  }.toSet

  val isRateCounter: Boolean = rateActionId.isDefined && rateBaseId.isDefined && rateActionId != rateBaseId
  val isTrendCounter: Boolean = rateActionId.isDefined && rateBaseId.isDefined && rateActionId == rateBaseId
}

object Counter extends SQLSyntaxSupport[Counter] {
  object ItemType extends Enumeration {
    type ItemType = Value
    val INT, LONG, STRING, BLOB = Value
  }

  def apply(c: SyntaxProvider[Counter])(rs: WrappedResultSet): Counter = apply(c.resultName)(rs)
  def apply(r: ResultName[Counter])(rs: WrappedResultSet): Counter = {
    lazy val itemType = Counter.ItemType(rs.int(r.itemType))
    Counter(rs.int(r.id), rs.boolean(r.useFlag), rs.byte(r.version), rs.string(r.service), rs.string(r.action),
      itemType, rs.boolean(r.autoComb), rs.string(r.dimension),
      rs.boolean(r.useProfile), rs.stringOpt(r.bucketImpId),
      rs.boolean(r.useRank),
      rs.int(r.ttl), rs.intOpt(r.dailyTtl), rs.stringOpt(r.hbaseTable), rs.stringOpt(r.intervalUnit),
      rs.intOpt(r.rateActionId), rs.intOpt(r.rateBaseId), rs.intOpt(r.rateThreshold))
  }

  def apply(useFlag: Boolean, version: Byte, service: String, action: String, itemType: Counter.ItemType.ItemType,
            autoComb: Boolean, dimension: String, useProfile: Boolean = false, bucketImpId: Option[String] = None,
            useRank: Boolean = false, ttl: Int = 259200, dailyTtl: Option[Int] = None,
            hbaseTable: Option[String] = None, intervalUnit: Option[String] = None,
            rateActionId: Option[Int] = None, rateBaseId: Option[Int] = None, rateThreshold: Option[Int] = None): Counter = {
    Counter(-1, useFlag, version, service, action, itemType, autoComb, dimension,
      useProfile, bucketImpId,
      useRank, ttl, dailyTtl, hbaseTable,
      intervalUnit, rateActionId, rateBaseId, rateThreshold)
  }
}

class CounterModel(config: Config) extends CachedDBModel[Counter] {
  private lazy val s2Config = new S2CounterConfig(config)
  // enable negative cache
  override val cacheConfig: CollectionCacheConfig =
    new CollectionCacheConfig(s2Config.CACHE_MAX_SIZE, s2Config.CACHE_TTL_SECONDS,
      negativeCache = true, s2Config.CACHE_NEGATIVE_TTL_SECONDS)

  val c = Counter.syntax("c")
  val r = c.result

  val multiCache = new CollectionCache[Seq[Counter]](cacheConfig)

  def findById(id: Int, useCache: Boolean = true): Option[Counter] = {
    lazy val sql = withSQL {
      selectFrom(Counter as c).where.eq(c.id, id).and.eq(c.useFlag, 1)
    }.map(Counter(c))

    if (useCache) {
      cache.withCache(s"_id:$id") {
        sql.single().apply()
      }
    } else {
      sql.single().apply()
    }
  }

  def findByServiceAction(service: String, action: String, useCache: Boolean = true): Option[Counter] = {
    lazy val sql = withSQL {
      selectFrom(Counter as c).where.eq(c.service, service).and.eq(c.action, action).and.eq(c.useFlag, 1)
    }.map(Counter(c))

    if (useCache) {
      cache.withCache(s"$service.$action") {
        sql.single().apply()
      }
    }
    else {
      sql.single().apply()
    }
  }

  def findByRateActionId(rateActionId: Int, useCache: Boolean = true): Seq[Counter] = {
    lazy val sql = withSQL {
      selectFrom(Counter as c).where.eq(c.rateActionId, rateActionId).and.ne(c.rateBaseId, rateActionId).and.eq(c.useFlag, 1)
    }.map(Counter(c))

    if (useCache) {
      multiCache.withCache(s"_rate_action_id.$rateActionId") {
        sql.list().apply()
      }
    } else {
      sql.list().apply()
    }
  }
  
  def findByRateBaseId(rateBaseId: Int, useCache: Boolean = true): Seq[Counter] = {
    lazy val sql = withSQL {
      selectFrom(Counter as c).where.eq(c.rateBaseId, rateBaseId).and.ne(c.rateActionId, rateBaseId).and.eq(c.useFlag, 1)
    }.map(Counter(c))

    if (useCache) {
      multiCache.withCache(s"_rate_base_id.$rateBaseId") {
        sql.list().apply()
      }
    } else {
      sql.list().apply()
    }
  }

  def findByTrendActionId(trendActionId: Int, useCache: Boolean = true): Seq[Counter] = {
    lazy val sql = withSQL {
      selectFrom(Counter as c).where.eq(c.rateActionId, trendActionId).and.eq(c.rateBaseId, trendActionId).and.eq(c.useFlag, 1)
    }.map(Counter(c))

    if (useCache) {
      multiCache.withCache(s"_trend_action_id.$trendActionId") {
        sql.list().apply()
      }
    } else {
      sql.list().apply()
    }
  }

  def createServiceAction(policy: Counter): Unit = {
    withSQL {
      val c = Counter.column
      insert.into(Counter).namedValues(
        c.useFlag -> policy.useFlag,
        c.version -> policy.version,
        c.service -> policy.service,
        c.action -> policy.action,
        c.itemType -> policy.itemType.id,
        c.autoComb -> policy.autoComb,
        c.dimension -> policy.dimension,
        c.useProfile -> policy.useProfile,
        c.bucketImpId -> policy.bucketImpId,
        c.useRank -> policy.useRank,
        c.ttl -> policy.ttl,
        c.dailyTtl -> policy.dailyTtl,
        c.hbaseTable -> policy.hbaseTable,
        c.intervalUnit -> policy.intervalUnit,
        c.rateActionId -> policy.rateActionId,
        c.rateBaseId -> policy.rateBaseId,
        c.rateThreshold -> policy.rateThreshold
      )
    }.update().apply()
  }

  def updateServiceAction(policy: Counter): Unit = {
    withSQL {
      val c = Counter.column
      update(Counter).set(
        c.autoComb -> policy.autoComb,
        c.dimension -> policy.dimension,
        c.useProfile -> policy.useProfile,
        c.bucketImpId -> policy.bucketImpId,
        c.useRank -> policy.useRank,
        c.intervalUnit -> policy.intervalUnit,
        c.rateActionId -> policy.rateActionId,
        c.rateBaseId -> policy.rateBaseId,
        c.rateThreshold -> policy.rateThreshold
      ).where.eq(c.id, policy.id)
    }.update().apply()
  }

  def deleteServiceAction(policy: Counter): Unit = {
    withSQL {
      val c = Counter.column
      update(Counter).set(
        c.action -> s"deleted_${System.currentTimeMillis()}_${policy.action}",
        c.useFlag -> false
      ).where.eq(c.id, policy.id)
    }.update().apply()
  }
}
