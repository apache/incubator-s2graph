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

package org.apache.s2graph.counter.config

import com.typesafe.config.Config

import scala.collection.JavaConversions._

class S2CounterConfig(config: Config) extends ConfigFunctions(config) {
   // HBase
   lazy val HBASE_ZOOKEEPER_QUORUM = getOrElse("hbase.zookeeper.quorum", "")
   lazy val HBASE_TABLE_NAME = getOrElse("hbase.table.name", "s2counter")
   lazy val HBASE_TABLE_POOL_SIZE = getOrElse("hbase.table.pool.size", 100)
   lazy val HBASE_CONNECTION_POOL_SIZE = getOrElse("hbase.connection.pool.size", 10)

   lazy val HBASE_CLIENT_IPC_POOL_SIZE = getOrElse("hbase.client.ipc.pool.size", 5)
   lazy val HBASE_CLIENT_MAX_TOTAL_TASKS = getOrElse("hbase.client.max.total.tasks", 100)
   lazy val HBASE_CLIENT_MAX_PERSERVER_TASKS = getOrElse("hbase.client.max.perserver.tasks", 5)
   lazy val HBASE_CLIENT_MAX_PERREGION_TASKS = getOrElse("hbase.client.max.perregion.tasks", 1)
   lazy val HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = getOrElse("hbase.client.scanner.timeout.period", 300)
   lazy val HBASE_CLIENT_OPERATION_TIMEOUT = getOrElse("hbase.client.operation.timeout", 100)
   lazy val HBASE_CLIENT_RETRIES_NUMBER = getOrElse("hbase.client.retries.number", 1)

   // MySQL
   lazy val DB_DEFAULT_DRIVER = getOrElse("db.default.driver", "com.mysql.jdbc.Driver")
   lazy val DB_DEFAULT_URL = getOrElse("db.default.url", "")
   lazy val DB_DEFAULT_USER = getOrElse("db.default.user", "graph")
   lazy val DB_DEFAULT_PASSWORD = getOrElse("db.default.password", "graph")

   // Redis
   lazy val REDIS_INSTANCES = (for {
     s <- config.getStringList("redis.instances")
   } yield {
     val sp = s.split(':')
     (sp(0), if (sp.length > 1) sp(1).toInt else 6379)
   }).toList

   // Graph
   lazy val GRAPH_URL = getOrElse("s2graph.url", "http://localhost:9000")
   lazy val GRAPH_READONLY_URL = getOrElse("s2graph.read-only.url", GRAPH_URL)

   // Cache
   lazy val CACHE_TTL_SECONDS = getOrElse("cache.ttl.seconds", 600)
   lazy val CACHE_MAX_SIZE = getOrElse("cache.max.size", 10000)
   lazy val CACHE_NEGATIVE_TTL_SECONDS = getOrElse("cache.negative.ttl.seconds", CACHE_TTL_SECONDS)
 }
