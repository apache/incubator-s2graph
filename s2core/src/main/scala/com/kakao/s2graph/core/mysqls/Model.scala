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

package com.kakao.s2graph.core.mysqls

import java.util.concurrent.Executors

import com.kakao.s2graph.core.utils.{SafeUpdateCache, logger}
import com.typesafe.config.Config
import scalikejdbc._

import scala.concurrent.ExecutionContext
import scala.language.{higherKinds, implicitConversions}
import scala.util.{Failure, Try}

object Model {
  var maxSize = 10000
  var ttl = 60
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  val ec = ExecutionContext.fromExecutor(threadPool)

  def apply(config: Config) = {
    maxSize = config.getInt("cache.max.size")
    ttl = config.getInt("cache.ttl.seconds")
    Class.forName(config.getString("db.default.driver"))

    val settings = ConnectionPoolSettings(
      initialSize = 10,
      maxSize = 10,
      connectionTimeoutMillis = 30000L,
      validationQuery = "select 1;")

    ConnectionPool.singleton(
      config.getString("db.default.url"),
      config.getString("db.default.user"),
      config.getString("db.default.password"),
      settings)
  }

  def withTx[T](block: DBSession => T): Try[T] = {
    using(DB(ConnectionPool.borrow())) { conn =>
      val res = Try {
        conn.begin()
        val session = conn.withinTxSession()
        val result = block(session)

        conn.commit()

        result
      } recoverWith {
        case e: Exception =>
          conn.rollbackIfActive()
          Failure(e)
      }

      res
    }
  }

  def shutdown() = {
    ConnectionPool.closeAll()
  }

  def loadCache() = {
    Service.findAll()
    ServiceColumn.findAll()
    Label.findAll()
    LabelMeta.findAll()
    LabelIndex.findAll()
    ColumnMeta.findAll()
  }
}

trait Model[V] extends SQLSyntaxSupport[V] {

  import Model._

  implicit val ec: ExecutionContext = Model.ec

  val cName = this.getClass.getSimpleName()
  logger.info(s"LocalCache[$cName]: TTL[$ttl], MaxSize[$maxSize]")

  val optionCache = new SafeUpdateCache[Option[V]](cName, maxSize, ttl)
  val listCache = new SafeUpdateCache[List[V]](cName, maxSize, ttl)

  val withCache = optionCache.withCache _

  val withCaches = listCache.withCache _

  val expireCache = optionCache.invalidate _

  val expireCaches = listCache.invalidate _

  def putsToCache(kvs: List[(String, V)]) = kvs.foreach {
    case (key, value) => optionCache.put(key, Option(value))
  }

  def putsToCaches(kvs: List[(String, List[V])]) = kvs.foreach {
    case (key, values) => listCache.put(key, values)
  }
}

