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

package org.apache.s2graph.core.fetcher

import com.typesafe.config.Config
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.utils.{Importer, logger}
import org.apache.s2graph.core.{EdgeFetcher, S2GraphLike}

import scala.concurrent.{ExecutionContext, Future}

object FetcherManager {
  val ClassNameKey = "className"
}

class FetcherManager(s2GraphLike: S2GraphLike) {

  import FetcherManager._

  private val fetcherPool = scala.collection.mutable.Map.empty[String, EdgeFetcher]

  private val ImportLock = new java.util.concurrent.ConcurrentHashMap[String, Importer]

  def toImportLockKey(label: Label): String = label.label

  def getFetcher(label: Label): EdgeFetcher = {
    fetcherPool.getOrElse(toImportLockKey(label), throw new IllegalStateException(s"$label is not imported."))
  }

  def initImporter(config: Config): Importer = {
    val className = config.getString(ClassNameKey)

    Class.forName(className)
      .getConstructor(classOf[S2GraphLike])
      .newInstance(s2GraphLike)
      .asInstanceOf[Importer]
  }

  def initFetcher(config: Config)(implicit ec: ExecutionContext): Future[EdgeFetcher] = {
    val className = config.getString(ClassNameKey)

    val fetcher = Class.forName(className)
      .getConstructor(classOf[S2GraphLike])
      .newInstance(s2GraphLike)
      .asInstanceOf[EdgeFetcher]

    fetcher.init(config)

    Future.successful(fetcher)
  }

  def importModel(label: Label, config: Config)(implicit ec: ExecutionContext): Future[Importer] = {
    val importer = ImportLock.computeIfAbsent(toImportLockKey(label), new java.util.function.Function[String, Importer] {
      override def apply(k: String): Importer = {
        val importer = initImporter(config.getConfig("importer"))

        //TODO: Update Label's extra options.
        importer
          .run(config.getConfig("importer"))
          .map { importer =>
            logger.info(s"Close importer")
            importer.close()

            initFetcher(config.getConfig("fetcher")).map { fetcher =>
              importer.setStatus(true)

              fetcherPool
                .remove(k)
                .foreach { oldFetcher =>
                  logger.info(s"Delete old storage ($k) => $oldFetcher")
                  oldFetcher.close()
                }

              fetcherPool += (k -> fetcher)
            }
          }
          .onComplete { _ =>
            logger.info(s"ImportLock release: $k")
            ImportLock.remove(k)
          }

        importer
      }
    })

    Future.successful(importer)
  }

}
