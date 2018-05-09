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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.s2graph.core.schema.{Label, ServiceColumn}
import org.apache.s2graph.core.utils.SafeUpdateCache
import scala.concurrent.ExecutionContext


object ResourceManager {

  import SafeUpdateCache._

  import scala.collection.JavaConverters._

  val ClassNameKey = "className"
  val EdgeFetcherKey = classOf[EdgeFetcher].getClass().getName

  val VertexFetcherKey = classOf[VertexFetcher].getClass().getName

  val EdgeMutatorKey = classOf[EdgeMutator].getClass.getName
  val VertexMutatorKey = classOf[VertexMutator].getClass.getName

  val DefaultConfig = ConfigFactory.parseMap(Map(MaxSizeKey -> 1000, TtlKey -> -1).asJava)
}

class ResourceManager(graph: S2GraphLike,
                      _config: Config)(implicit ec: ExecutionContext) {

  import ResourceManager._

  import scala.collection.JavaConverters._

  val cache = new SafeUpdateCache(_config)

  def getAllVertexFetchers(): Seq[VertexFetcher] = {
    cache.asMap().asScala.toSeq.collect { case (_, (obj: VertexFetcher, _, _)) => obj }
  }

  def getAllEdgeFetchers(): Seq[EdgeFetcher] = {
    cache.asMap().asScala.toSeq.collect { case (_, (obj: EdgeFetcher, _, _)) => obj }
  }

  def getOrElseUpdateVertexFetcher(column: ServiceColumn, forceUpdate: Boolean = false): Option[VertexFetcher] = {
    val cacheKey = VertexFetcherKey + "_" + column.service.serviceName + "_" + column.columnName
    cache.withCache(cacheKey, false, forceUpdate) {
      column.toFetcherConfig.map { fetcherConfig =>
        val className = fetcherConfig.getString(ClassNameKey)
        val fetcher = Class.forName(className)
          .getConstructor(classOf[S2GraphLike])
          .newInstance(graph)
          .asInstanceOf[VertexFetcher]

        fetcher.init(fetcherConfig)

        fetcher
      }
    }
  }

  def getOrElseUpdateEdgeFetcher(label: Label, forceUpdate: Boolean = false): Option[EdgeFetcher] = {
    val cacheKey = EdgeFetcherKey + "_" + label.label

    cache.withCache(cacheKey, false, forceUpdate) {
      label.toFetcherConfig.map { fetcherConfig =>
        val className = fetcherConfig.getString(ClassNameKey)
        val fetcher = Class.forName(className)
          .getConstructor(classOf[S2GraphLike])
          .newInstance(graph)
          .asInstanceOf[EdgeFetcher]

        fetcher.init(fetcherConfig)

        fetcher
      }
    }
  }

  def getOrElseUpdateVertexMutator(column: ServiceColumn, forceUpdate: Boolean = false): Option[VertexMutator] = {
    val cacheKey = VertexMutatorKey + "_" + column.service.serviceName + "_" + column.columnName
    cache.withCache(cacheKey, false, forceUpdate) {
      column.toMutatorConfig.map { mutatorConfig =>
        val className = mutatorConfig.getString(ClassNameKey)
        val fetcher = Class.forName(className)
          .getConstructor(classOf[S2GraphLike])
          .newInstance(graph)
          .asInstanceOf[VertexMutator]

        fetcher.init(mutatorConfig)

        fetcher
      }
    }
  }

  def getOrElseUpdateEdgeMutator(label: Label, forceUpdate: Boolean = false): Option[EdgeMutator] = {
    val cacheKey = EdgeMutatorKey + "_" + label.label
    cache.withCache(cacheKey, false, forceUpdate) {
      label.toMutatorConfig.map { mutatorConfig =>
        val className = mutatorConfig.getString(ClassNameKey)
        val fetcher = Class.forName(className)
          .getConstructor(classOf[S2GraphLike])
          .newInstance(graph)
          .asInstanceOf[EdgeMutator]

        fetcher.init(mutatorConfig)

        fetcher
      }
    }
  }
}
