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

package org.apache.s2graph.counter.loader.config

import org.apache.s2graph.counter.config.ConfigFunctions
import org.apache.s2graph.spark.config.S2ConfigFactory

object StreamingConfig extends ConfigFunctions(S2ConfigFactory.config) {
  // kafka
  val KAFKA_ZOOKEEPER = getOrElse("kafka.zookeeper", "localhost")
  val KAFKA_BROKERS = getOrElse("kafka.brokers", "localhost")
  val KAFKA_TOPIC_GRAPH = getOrElse("kafka.topic.graph", "s2graphInalpha")
  val KAFKA_TOPIC_ETL = getOrElse("kafka.topic.etl", "s2counter-etl-alpha")
  val KAFKA_TOPIC_COUNTER = getOrElse("kafka.topic.counter", "s2counter-alpha")
  val KAFKA_TOPIC_COUNTER_TRX = getOrElse("kafka.topic.counter-trx", "s2counter-trx-alpha")
  val KAFKA_TOPIC_COUNTER_FAIL = getOrElse("kafka.topic.counter-fail", "s2counter-fail-alpha")

  // profile cache
  val PROFILE_CACHE_TTL_SECONDS = getOrElse("profile.cache.ttl.seconds", 60 * 60 * 24)
  // default 1 day
  val PROFILE_CACHE_MAX_SIZE = getOrElse("profile.cache.max.size", 10000)
  val PROFILE_PREFETCH_SIZE = getOrElse("profile.prefetch.size", 10)

  // graph url
  val GRAPH_URL = getOrElse("s2graph.url", "")
  val GRAPH_READONLY_URL = getOrElse("s2graph.read-only.url", GRAPH_URL)
}
