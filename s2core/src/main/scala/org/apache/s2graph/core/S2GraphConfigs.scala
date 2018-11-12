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

package org.apache.s2graph.core

object S2GraphConfigs {
  lazy val DEFAULTS: Map[String, AnyRef] = Map(
    S2GRAPH_STORE_BACKEND -> DEFAULT_S2GRAPH_STORE_BACKEND,
    PHASE -> DEFAULT_PHASE
  )
  lazy val DEFAULT_CONFIGS = DEFAULTS ++
    S2GraphConfigs.HBaseConfigs.DEFAULTS ++
    S2GraphConfigs.DBConfigs.DEFAULTS ++
    S2GraphConfigs.CacheConfigs.DEFAULTS ++
    S2GraphConfigs.ResourceCacheConfigs.DEFAULTS ++
    S2GraphConfigs.MutatorConfigs.DEFAULTS ++
    S2GraphConfigs.QueryConfigs.DEFAULTS ++
    S2GraphConfigs.FutureCacheConfigs.DEFAULTS ++
    S2GraphConfigs.LogConfigs.DEFAULTS

  val S2GRAPH_STORE_BACKEND = "s2graph.storage.backend"
  val DEFAULT_S2GRAPH_STORE_BACKEND = "datastore"
//    "rocks"
//    "hbase"

  val PHASE = "phase"
  val DEFAULT_PHASE = "dev"

  object HBaseConfigs {
    lazy val DEFAULTS: Map[String, AnyRef] = Map(
      HBASE_ZOOKEEPER_QUORUM -> DEFAULT_HBASE_ZOOKEEPER_QUORUM,
      HBASE_ZOOKEEPER_ZNODE_PARENT -> DEFAULT_HBASE_ZOOKEEPER_ZNODE_PARENT,
      HBASE_TABLE_NAME -> DEFAULT_HBASE_TABLE_NAME,
      HBASE_TABLE_COMPRESSION_ALGORITHM -> DEFAULT_HBASE_TABLE_COMPRESSION_ALGORITHM,
      HBASE_CLIENT_RETRIES_NUMBER -> DEFAULT_HBASE_CLIENT_RETRIES_NUMBER,
      HBASE_RPCS_BUFFERED_FLUSH_INTERVAL -> DEFAULT_HBASE_RPCS_BUFFERED_FLUSH_INTERVAL,
      HBASE_RPC_TIMEOUT -> DEFAULT_HBASE_RPC_TIMEOUT
    )
    val HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
    val DEFAULT_HBASE_ZOOKEEPER_QUORUM = "localhost"

    val HBASE_ZOOKEEPER_ZNODE_PARENT = "hbase.zookeeper.znode.parent"
    val DEFAULT_HBASE_ZOOKEEPER_ZNODE_PARENT = "/hbase"

    val HBASE_TABLE_NAME = "hbase.table.name"
    val DEFAULT_HBASE_TABLE_NAME = "s2graph"

    val HBASE_TABLE_COMPRESSION_ALGORITHM = "hbase.table.compression.algorithm"
    val DEFAULT_HBASE_TABLE_COMPRESSION_ALGORITHM = "gz"

    val HBASE_CLIENT_RETRIES_NUMBER = "hbase.client.retries.number"
    val DEFAULT_HBASE_CLIENT_RETRIES_NUMBER = java.lang.Integer.valueOf(20)

    val HBASE_RPCS_BUFFERED_FLUSH_INTERVAL = "hbase.rpcs.buffered_flush_interval"
    val DEFAULT_HBASE_RPCS_BUFFERED_FLUSH_INTERVAL = java.lang.Short.valueOf(100.toShort)

    val HBASE_RPC_TIMEOUT = "hbase.rpc.timeout"
    val DEFAULT_HBASE_RPC_TIMEOUT = java.lang.Integer.valueOf(600000)
  }
  object DBConfigs {
    lazy val DEFAULTS: Map[String, AnyRef] = Map(
      DB_DEFAULT_DRIVER -> DEFAULT_DB_DEFAULT_DRIVER,
      DB_DEFAULT_URL ->  DEFAULT_DB_DEFAULT_URL,
      DB_DEFAULT_PASSWORD -> DEFAULT_DB_DEFAULT_PASSWORD,
      DB_DEFAULT_USER -> DEFAULT_DB_DEFAULT_USER
    )
    val DB_DEFAULT_DRIVER = "db.default.driver"
    val DEFAULT_DB_DEFAULT_DRIVER = "org.h2.Driver"

    val DB_DEFAULT_URL = "db.default.url"
    val DEFAULT_DB_DEFAULT_URL = "jdbc:h2:file:./var/metastore;MODE=MYSQL"

    val DB_DEFAULT_PASSWORD = "db.default.password"
    val DEFAULT_DB_DEFAULT_PASSWORD = "graph"

    val DB_DEFAULT_USER = "db.default.user"
    val DEFAULT_DB_DEFAULT_USER = "graph"
  }
  object CacheConfigs {
    lazy val DEFAULTS: Map[String, AnyRef] = Map(
      CACHE_MAX_SIZE -> DEFAULT_CACHE_MAX_SIZE,
      CACHE_TTL_SECONDS -> DEFAULT_CACHE_TTL_SECONDS
    )
    val CACHE_MAX_SIZE = "cache.max.size"
    val DEFAULT_CACHE_MAX_SIZE = java.lang.Integer.valueOf(0)

    val CACHE_TTL_SECONDS = "cache.ttl.seconds"
    val DEFAULT_CACHE_TTL_SECONDS = java.lang.Integer.valueOf(-1)
  }
  object ResourceCacheConfigs {
    lazy val DEFAULTS: Map[String, AnyRef] = Map(
      RESOURCE_CACHE_MAX_SIZE -> DEFAULT_RESOURCE_CACHE_MAX_SIZE,
      RESOURCE_CACHE_TTL_SECONDS -> DEFAULT_RESOURCE_CACHE_TTL_SECONDS
    )
    val RESOURCE_CACHE_MAX_SIZE = "resource.cache.max.size"
    val DEFAULT_RESOURCE_CACHE_MAX_SIZE = java.lang.Integer.valueOf(1000)

    val RESOURCE_CACHE_TTL_SECONDS = "resource.cache.ttl.seconds"
    val DEFAULT_RESOURCE_CACHE_TTL_SECONDS = java.lang.Integer.valueOf(-1)
  }
  object MutatorConfigs {
    lazy val DEFAULTS: Map[String, AnyRef] = Map(
      MAX_RETRY_NUMBER -> DEFAULT_MAX_RETRY_NUMBER,
      LOCK_EXPIRE_TIME -> DEFAULT_LOCK_EXPIRE_TIME,
      MAX_BACK_OFF -> DEFAULT_MAX_BACK_OFF,
      BACK_OFF_TIMEOUT ->  DEFAULT_BACK_OFF_TIMEOUT,
      HBASE_FAIL_PROB -> DEFAULT_HBASE_FAIL_PROB,
      DELETE_ALL_FETCH_SIZE -> DEFAULT_DELETE_ALL_FETCH_SIZE,
      DELETE_ALL_FETCH_COUNT -> DEFAULT_DELETE_ALL_FETCH_COUNT
    )
    val MAX_RETRY_NUMBER = "max.retry.number"
    val DEFAULT_MAX_RETRY_NUMBER = java.lang.Integer.valueOf(100)

    val LOCK_EXPIRE_TIME = "lock.expire.time"
    val DEFAULT_LOCK_EXPIRE_TIME = java.lang.Integer.valueOf(1000 * 60 * 10)

    val MAX_BACK_OFF = "max.back.off"
    val DEFAULT_MAX_BACK_OFF = java.lang.Integer.valueOf(100)

    val BACK_OFF_TIMEOUT = "back.off.timeout"
    val DEFAULT_BACK_OFF_TIMEOUT = java.lang.Integer.valueOf(1000)

    val HBASE_FAIL_PROB = "hbase.fail.prob"
    val DEFAULT_HBASE_FAIL_PROB = java.lang.Double.valueOf(-0.1)

    val DELETE_ALL_FETCH_SIZE = "delete.all.fetch.size"
    val DEFAULT_DELETE_ALL_FETCH_SIZE = java.lang.Integer.valueOf(1000)

    val DELETE_ALL_FETCH_COUNT = "delete.all.fetch.count"
    val DEFAULT_DELETE_ALL_FETCH_COUNT = java.lang.Integer.valueOf(200)
  }
  object QueryConfigs {
    lazy val DEFAULTS: Map[String, AnyRef] = Map(
      QUERY_HARDLIMIT -> DEFAULT_QUERY_HARDLIMIT
    )
    val QUERY_HARDLIMIT = "query.hardlimit"
    val DEFAULT_QUERY_HARDLIMIT = java.lang.Integer.valueOf(100000)
  }
  object FutureCacheConfigs {
    lazy val DEFAULTS: Map[String, AnyRef] = Map(
      FUTURE_CACHE_MAX_SIZE -> DEFAULT_FUTURE_CACHE_MAX_SIZE,
      FUTURE_CACHE_EXPIRE_AFTER_WRITE -> DEFAULT_FUTURE_CACHE_EXPIRE_AFTER_WRITE,
      FUTURE_CACHE_EXPIRE_AFTER_ACCESS -> DEFAULT_FUTURE_CACHE_EXPIRE_AFTER_ACCESS,
      FUTURE_CACHE_METRIC_INTERVAL -> DEFAULT_FUTURE_CACHE_METRIC_INTERVAL
    )
    val FUTURE_CACHE_MAX_SIZE = "future.cache.max.size"
    val DEFAULT_FUTURE_CACHE_MAX_SIZE = java.lang.Integer.valueOf(100000)

    val FUTURE_CACHE_EXPIRE_AFTER_WRITE = "future.cache.expire.after.write"
    val DEFAULT_FUTURE_CACHE_EXPIRE_AFTER_WRITE = java.lang.Integer.valueOf(10000)

    val FUTURE_CACHE_EXPIRE_AFTER_ACCESS = "future.cache.expire.after.access"
    val DEFAULT_FUTURE_CACHE_EXPIRE_AFTER_ACCESS = java.lang.Integer.valueOf(5000)

    val FUTURE_CACHE_METRIC_INTERVAL = "future.cache.metric.interval"
    val DEFAULT_FUTURE_CACHE_METRIC_INTERVAL = java.lang.Integer.valueOf(60000)
  }
  object LogConfigs {
    lazy val DEFAULTS: Map[String, AnyRef] = Map(
      QUERY_LOG_SAMPLE_RATE -> DEFAULT_QUERY_LOG_SAMPLE_RATE
    )
    val QUERY_LOG_SAMPLE_RATE = "query.log.sample.rate"
    val DEFAULT_QUERY_LOG_SAMPLE_RATE = Double.box(0.05)
  }
}
