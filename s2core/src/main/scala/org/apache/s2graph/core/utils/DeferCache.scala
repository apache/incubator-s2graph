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

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.cache.CacheBuilder
import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.higherKinds

trait CanDefer[A, M[_], C[_]] {
  def promise: M[A]

  def future(defer: M[A]): C[A]

  def success(defer: M[A], value: A): Unit

  def failure(defer: M[A], cause: Throwable): Unit

  def onSuccess(defer: C[A])(pf: PartialFunction[A, A])(implicit ec: ExecutionContext)

  def onFailure(defer: C[A])(pf: PartialFunction[Throwable, A])(implicit ec: ExecutionContext)
}

object CanDefer {
  implicit def implFuture[A] = new CanDefer[A, Promise, Future] {
    override def promise: Promise[A] = Promise[A]()

    override def future(defer: Promise[A]): Future[A] = defer.future

    override def success(defer: Promise[A], value: A) = defer.success(value)

    override def failure(defer: Promise[A], cause: Throwable) = defer.failure(cause)

    override def onSuccess(defer: Future[A])(pf: PartialFunction[A, A])(implicit ec: ExecutionContext) = defer onSuccess pf

    override def onFailure(defer: Future[A])(pf: PartialFunction[Throwable, A])(implicit ec: ExecutionContext) = defer onFailure pf
  }

  implicit def implDeferred[A] = new CanDefer[A, Deferred, Deferred] {

    override def promise: Deferred[A] = new Deferred[A]()

    override def future(defer: Deferred[A]): Deferred[A] = defer

    override def success(defer: Deferred[A], value: A) = defer.callback(value)

    override def failure(defer: Deferred[A], cause: Throwable) = defer.callback(cause)

    override def onSuccess(defer: Deferred[A])(pf: PartialFunction[A, A])(implicit _ec: ExecutionContext) =
      defer.addCallback(new Callback[A, A] {
        override def call(arg: A): A = pf(arg)
      })

    override def onFailure(defer: Deferred[A])(pf: PartialFunction[Throwable, A])(implicit ec: ExecutionContext) =
      defer.addErrback(new Callback[A, Exception] {
        override def call(t: Exception): A = pf(t)
      })
  }
}

object DeferCache {
  private val scheduledThreadPool = Executors.newSingleThreadScheduledExecutor()

  def addScheduleJob(delay: Long)(block: => Unit) =
    scheduledThreadPool.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = block
    }, 1000, delay, TimeUnit.MILLISECONDS)
}

/**
 * @param config
 * @param canDefer: implicit evidence to find out implementation of CanDefer.
 * @tparam A: actual element type that will be stored in M[_]  and C[_].
 * @tparam M[_]: container type that will be stored in local cache. ex) Promise, Defer.
 * @tparam C[_]: container type that will be returned to client of this class. Ex) Future, Defer.
 */
class DeferCache[A, M[_], C[_]](config: Config, empty: => A,
                                name: String = "", useMetric: Boolean = false)(implicit canDefer: CanDefer[A, M, C]) {
  type Value = (Long, C[A])

  private val maxSize = config.getInt("future.cache.max.size")
  private val metricInterval = config.getInt("future.cache.metric.interval")
  private val expireAfterWrite = config.getInt("future.cache.expire.after.write")
  private val expireAfterAccess = config.getInt("future.cache.expire.after.access")

  private val futureCache = {
    val builder = CacheBuilder.newBuilder()
      .initialCapacity(maxSize)
      .concurrencyLevel(Runtime.getRuntime.availableProcessors())
      .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
      .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
      .maximumSize(maxSize)

    if (useMetric && metricInterval > 0) {
      val cache = builder.recordStats().build[java.lang.Long, (Long, M[A])]()
      DeferCache.addScheduleJob(delay = metricInterval) { logger.metric(s"${name}: ${cache.stats()}") }
      cache
    } else {
      builder.recordStats().build[java.lang.Long, (Long, M[A])]()
    }
  }

  def asMap() = futureCache.asMap()

  def getIfPresent(cacheKey: Long): Value = {
    val (cachedAt, promise) = futureCache.getIfPresent(cacheKey)
    (cachedAt, canDefer.future(promise))
  }

  private def checkAndExpire(cacheKey: Long,
                             cachedAt: Long,
                             cacheTTL: Long,
                             oldFuture: C[A])(op: => C[A])(implicit ec: ExecutionContext): C[A] = {
    if (System.currentTimeMillis() >= cachedAt + cacheTTL) {
      // future is too old. so need to expire and fetch new data from storage.
      futureCache.asMap().remove(cacheKey)

      val promise = canDefer.promise
      val now = System.currentTimeMillis()

      futureCache.asMap().putIfAbsent(cacheKey, (now, promise)) match {
        case null =>
          // only one thread succeed to come here concurrently
          // initiate fetch to storage then add callback on complete to finish promise.
          val result = op
          canDefer.onSuccess(result) { case value =>
            canDefer.success(promise, value)
            value
          }

          canDefer.onFailure(result) { case e: Throwable =>
            canDefer.failure(promise, e)
            empty
          }

          canDefer.future(promise)

        case (cachedAt, oldPromise) => canDefer.future(oldPromise)
      }
    } else {
      // future is not to old so reuse it.
      oldFuture
    }
  }

  def getOrElseUpdate(cacheKey: Long, cacheTTL: Long)(op: => C[A])(implicit ec: ExecutionContext): C[A] = {
    val cacheVal = futureCache.getIfPresent(cacheKey)
    cacheVal match {
      case null =>
        val promise = canDefer.promise
        val now = System.currentTimeMillis()

        val (cachedAt, cachedPromise) = futureCache.asMap().putIfAbsent(cacheKey, (now, promise)) match {
          case null =>
            val result = op
            canDefer.onSuccess(result) { case value =>
              canDefer.success(promise, value)
              value
            }

            canDefer.onFailure(result) { case e: Throwable =>
              canDefer.failure(promise, e)
              empty
            }

            (now, promise)

          case oldVal => oldVal
        }
        checkAndExpire(cacheKey, cacheTTL, cachedAt, canDefer.future(cachedPromise))(op)

      case (cachedAt, cachedPromise) =>
        checkAndExpire(cacheKey, cacheTTL, cachedAt, canDefer.future(cachedPromise))(op)
    }
  }

  def stats = futureCache.stats()
  def size = futureCache.size()
}
