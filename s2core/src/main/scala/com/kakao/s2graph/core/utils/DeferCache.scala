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

package com.kakao.s2graph.core.utils

import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import com.stumbleupon.async.Deferred
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext

class DeferCache[R](config: Config)(implicit ex: ExecutionContext) {

  import com.kakao.s2graph.core.utils.Extensions.DeferOps

  type Value = (Long, Deferred[R])

  private val maxSize = config.getInt("future.cache.max.size")
  private val expireAfterWrite = config.getInt("future.cache.expire.after.write")
  private val expireAfterAccess = config.getInt("future.cache.expire.after.access")

  private val futureCache = CacheBuilder.newBuilder()
  .initialCapacity(maxSize)
  .concurrencyLevel(Runtime.getRuntime.availableProcessors())
  .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
  .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
  .maximumSize(maxSize).build[java.lang.Long, (Long, Deferred[R])]()


  def asMap() = futureCache.asMap()

  def getIfPresent(cacheKey: Long): Value = futureCache.getIfPresent(cacheKey)

  private def checkAndExpire(cacheKey: Long,
                             cachedAt: Long,
                             cacheTTL: Long,
                             oldDefer: Deferred[R])(op: => Deferred[R]): Deferred[R] = {
    if (System.currentTimeMillis() >= cachedAt + cacheTTL) {
      // future is too old. so need to expire and fetch new data from storage.
      futureCache.asMap().remove(cacheKey)

      val newPromise = new Deferred[R]()
      val now = System.currentTimeMillis()

      futureCache.asMap().putIfAbsent(cacheKey, (now, newPromise)) match {
        case null =>
          // only one thread succeed to come here concurrently
          // initiate fetch to storage then add callback on complete to finish promise.
          op withCallback { value =>
            newPromise.callback(value)
            value
          }
          newPromise
        case (cachedAt, oldDefer) => oldDefer
      }
    } else {
      // future is not to old so reuse it.
      oldDefer
    }
  }
  def getOrElseUpdate(cacheKey: Long, cacheTTL: Long)(op: => Deferred[R]): Deferred[R] = {
    val cacheVal = futureCache.getIfPresent(cacheKey)
    cacheVal match {
      case null =>
        val promise = new Deferred[R]()
        val now = System.currentTimeMillis()
        val (cachedAt, defer) = futureCache.asMap().putIfAbsent(cacheKey, (now, promise)) match {
          case null =>
            op.withCallback { value =>
              promise.callback(value)
              value
            }
            (now, promise)
          case oldVal => oldVal
        }
        checkAndExpire(cacheKey, cacheTTL, cachedAt, defer)(op)

      case (cachedAt, defer) =>
        checkAndExpire(cacheKey, cacheTTL, cachedAt, defer)(op)
    }
  }
}

