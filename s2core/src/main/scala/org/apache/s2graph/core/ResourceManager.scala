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

import com.typesafe.config.impl.ConfigImpl
import com.typesafe.config._
import org.apache.s2graph.core.schema.{Label, ServiceColumn}
import org.apache.s2graph.core.utils.{SafeUpdateCache, logger}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.tools.nsc.typechecker.StructuredTypeStrings
import scala.util.Try


object ResourceManager {

  import SafeUpdateCache._

  import scala.collection.JavaConverters._

  val ClassNameKey = "className"
  val MaxSizeKey = "resource.cache.max.size"
  val CacheTTL = "resource.cache.ttl.seconds"

  val EdgeFetcherKey = classOf[EdgeFetcher].getName

  val VertexFetcherKey = classOf[VertexFetcher].getName

  val EdgeMutatorKey = classOf[EdgeMutator].getName
  val VertexMutatorKey = classOf[VertexMutator].getName

  val DefaultMaxSize = 1000
  val DefaultCacheTTL = -1
  val DefaultConfig = ConfigFactory.parseMap(Map(MaxSizeKey -> DefaultMaxSize, TtlKey -> DefaultCacheTTL).asJava)
}

class ResourceManager(graph: S2GraphLike,
                      _config: Config)(implicit ec: ExecutionContext) {

  import ResourceManager._

  import scala.collection.JavaConverters._

  val maxSize = Try(_config.getInt(ResourceManager.MaxSizeKey)).getOrElse(DefaultMaxSize)
  val cacheTTL = Try(_config.getInt(ResourceManager.CacheTTL)).getOrElse(DefaultCacheTTL)

  val cache = new SafeUpdateCache(maxSize, cacheTTL)

  def getAllVertexFetchers(): Seq[VertexFetcher] = {
    cache.asMap().asScala.toSeq.collect { case (_, (obj: VertexFetcher, _, _)) => obj }
  }

  def getAllEdgeFetchers(): Seq[EdgeFetcher] = {
    cache.asMap().asScala.toSeq.collect { case (_, (obj: EdgeFetcher, _, _)) => obj }
  }

  def onEvict(oldValue: AnyRef): Unit = {
    oldValue match {
      case o: Option[_] => o.foreach {
        case v: AutoCloseable => v.close()
      }
      case v: AutoCloseable => v.close()
      case _ => logger.info(s"Class does't have close() method ${oldValue.getClass.getName}")
    }

    logger.info(s"[${oldValue.getClass.getName}]: $oldValue evicted.")
  }

  def getOrElseUpdateVertexFetcher(column: ServiceColumn,
                                   cacheTTLInSecs: Option[Int] = None): Option[VertexFetcher] = {
    val cacheKey = VertexFetcherKey + "_" + column.service.serviceName + "_" + column.columnName
    cache.withCache(cacheKey, false, cacheTTLInSecs, onEvict) {
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

  def getOrElseUpdateEdgeFetcher(label: Label,
                                 cacheTTLInSecs: Option[Int] = None): Option[EdgeFetcher] = {
    val cacheKey = EdgeFetcherKey + "_" + label.label

    cache.withCache(cacheKey, false, cacheTTLInSecs, onEvict) {
      label.toFetcherConfig.map { fetcherConfig =>
        val className = fetcherConfig.getString(ClassNameKey)
        val fetcher = Class.forName(className)
          .getConstructor(classOf[S2GraphLike])
          .newInstance(graph)
          .asInstanceOf[EdgeFetcher]

        fetcher.init(
          fetcherConfig
            .withValue("labelName", ConfigValueFactory.fromAnyRef(label.label))
        )

        fetcher
      }
    }
  }

  def getOrElseUpdateVertexMutator(column: ServiceColumn,
                                   cacheTTLInSecs: Option[Int] = None): Option[VertexMutator] = {
    val cacheKey = VertexMutatorKey + "_" + column.service.serviceName + "_" + column.columnName
    cache.withCache(cacheKey, false, cacheTTLInSecs, onEvict) {
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

  def getOrElseUpdateEdgeMutator(label: Label,
                                 cacheTTLInSecs: Option[Int] = None): Option[EdgeMutator] = {
    val cacheKey = EdgeMutatorKey + "_" + label.label
    cache.withCache(cacheKey, false, cacheTTLInSecs, onEvict) {
      label.toMutatorConfig.map { mutatorConfig =>
        val className = mutatorConfig.getString(ClassNameKey)
        val fetcher = Class.forName(className)
          .getConstructor(classOf[S2GraphLike])
          .newInstance(graph)
          .asInstanceOf[EdgeMutator]

        fetcher.init(
          mutatorConfig
            .withValue("labelName", ConfigValueFactory.fromAnyRef(label.label))
        )

        fetcher
      }
    }
  }
}
