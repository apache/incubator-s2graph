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

package org.apache.s2graph.core.utils

import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import com.stumbleupon.async.Deferred
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.higherKinds

trait CanDefer[A, M[_], C[_]] {
  def promise: M[A]

  def future(defer: M[A]): C[A]

  def success(defer: M[A], value: A): Unit

  def onSuccess[U](defer: C[A])(pf: PartialFunction[A, U])(implicit ec: ExecutionContext)
}

object CanDefer {
  implicit def implFuture[A] = new CanDefer[A, Promise, Future] {
    override def promise: Promise[A] = Promise[A]()

    override def future(defer: Promise[A]): Future[A] = defer.future

    override def success(defer: Promise[A], value: A) = defer.success(value)

    override def onSuccess[U](defer: Future[A])(pf: PartialFunction[A, U])(implicit ec: ExecutionContext) = defer onSuccess pf
  }

  implicit def implDeferred[A] = new CanDefer[A, Deferred, Deferred] {

    import Extensions.DeferOps

    override def promise: Deferred[A] = new Deferred[A]()

    override def future(defer: Deferred[A]): Deferred[A] = defer

    override def success(defer: Deferred[A], value: A) = defer.callback(value)

    override def onSuccess[U](defer: Deferred[A])(pf: PartialFunction[A, U])(implicit _ec: ExecutionContext) = defer withCallback pf
  }
}

/**
 *
 * @param config
 * @param ec
 * @param canDefer: implicit evidence to find out implementation of CanDefer.
 * @tparam A: actual element type that will be stored in M[_]  and C[_].
 * @tparam M[_]: container type that will be stored in local cache. ex) Promise, Defer.
 * @tparam C[_]: container type that will be returned to client of this class. Ex) Future, Defer.
 */
class DeferCache[A, M[_], C[_]](config: Config)(implicit ec: ExecutionContext, canDefer: CanDefer[A, M, C]) {

  type Value = (Long, C[A])

  private val maxSize = config.getInt("future.cache.max.size")
  private val expireAfterWrite = config.getInt("future.cache.expire.after.write")
  private val expireAfterAccess = config.getInt("future.cache.expire.after.access")

  private val futureCache = CacheBuilder.newBuilder()
    .initialCapacity(maxSize)
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
    .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
    .maximumSize(maxSize).build[java.lang.Long, (Long, M[A])]()


  def asMap() = futureCache.asMap()

  def getIfPresent(cacheKey: Long): Value = {
    val (cachedAt, promise) = futureCache.getIfPresent(cacheKey)
    (cachedAt, canDefer.future(promise))
  }

  private def checkAndExpire(cacheKey: Long,
                             cachedAt: Long,
                             cacheTTL: Long,
                             oldFuture: C[A])(op: => C[A]): C[A] = {
    if (System.currentTimeMillis() >= cachedAt + cacheTTL) {
      // future is too old. so need to expire and fetch new data from storage.
      futureCache.asMap().remove(cacheKey)

      val promise = canDefer.promise
      val now = System.currentTimeMillis()

      futureCache.asMap().putIfAbsent(cacheKey, (now, promise)) match {
        case null =>
          // only one thread succeed to come here concurrently
          // initiate fetch to storage then add callback on complete to finish promise.
          canDefer.onSuccess(op) { case value =>
            canDefer.success(promise, value)
            value
          }

          canDefer.future(promise)

        case (cachedAt, oldPromise) => canDefer.future(oldPromise)
      }
    } else {
      // future is not to old so reuse it.
      oldFuture
    }
  }

  def getOrElseUpdate(cacheKey: Long, cacheTTL: Long)(op: => C[A]): C[A] = {
    val cacheVal = futureCache.getIfPresent(cacheKey)
    cacheVal match {
      case null =>
        val promise = canDefer.promise
        val now = System.currentTimeMillis()

        val (cachedAt, cachedPromise) = futureCache.asMap().putIfAbsent(cacheKey, (now, promise)) match {
          case null =>
            canDefer.onSuccess(op) { case value =>
              canDefer.success(promise, value)
              value
            }

            (now, promise)

          case oldVal => oldVal
        }
        checkAndExpire(cacheKey, cacheTTL, cachedAt, canDefer.future(cachedPromise))(op)

      case (cachedAt, cachedPromise) =>
        checkAndExpire(cacheKey, cacheTTL, cachedAt, canDefer.future(cachedPromise))(op)
    }
  }
}
