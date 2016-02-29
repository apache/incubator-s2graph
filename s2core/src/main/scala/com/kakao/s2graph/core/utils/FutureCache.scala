package com.kakao.s2graph.core.utils

import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config

import scala.concurrent.{Promise, Future, ExecutionContext}


class FutureCache[R](config: Config)(implicit ex: ExecutionContext) {

  type Value = (Long, Future[R])

  private val maxSize = config.getInt("future.cache.max.size")
  private val expireAfterWrite = config.getInt("future.cache.expire.after.write")
  private val expireAfterAccess = config.getInt("future.cache.expire.after.access")

  private val futureCache = CacheBuilder.newBuilder()
  .initialCapacity(maxSize)
  .concurrencyLevel(Runtime.getRuntime.availableProcessors())
  .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
  .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
  .maximumSize(maxSize).build[java.lang.Long, (Long, Promise[R])]()


  def asMap() = futureCache.asMap()

  def getIfPresent(cacheKey: Long): Value = {
    val (cachedAt, promise) = futureCache.getIfPresent(cacheKey)
    (cachedAt, promise.future)
  }

  private def checkAndExpire(cacheKey: Long,
                             cachedAt: Long,
                             cacheTTL: Long,
                             oldFuture: Future[R])(op: => Future[R]): Future[R] = {
    if (System.currentTimeMillis() >= cachedAt + cacheTTL) {
      // future is too old. so need to expire and fetch new data from storage.
      futureCache.asMap().remove(cacheKey)

      val newPromise = Promise[R]
      val now = System.currentTimeMillis()

      futureCache.asMap().putIfAbsent(cacheKey, (now, newPromise)) match {
        case null =>
          // only one thread succeed to come here concurrently
          // initiate fetch to storage then add callback on complete to finish promise.
          op.onSuccess { case value =>
            newPromise.success(value)
            value
          }
          newPromise.future
        case (cachedAt, oldPromise) => oldPromise.future
      }
    } else {
      // future is not to old so reuse it.
      oldFuture
    }
  }
  def getOrElseUpdate(cacheKey: Long, cacheTTL: Long)(op: => Future[R]): Future[R] = {
    val cacheVal = futureCache.getIfPresent(cacheKey)
    cacheVal match {
      case null =>
        val promise = Promise[R]
        val now = System.currentTimeMillis()
        val (cachedAt, cachedPromise) = futureCache.asMap().putIfAbsent(cacheKey, (now, promise)) match {
          case null =>
            op.onSuccess { case value =>
              promise.success(value)
              value
            }
            (now, promise)
          case oldVal => oldVal
        }
        checkAndExpire(cacheKey, cacheTTL, cachedAt, cachedPromise.future)(op)

      case (cachedAt, cachedPromise) =>
        checkAndExpire(cacheKey, cacheTTL, cachedAt, cachedPromise.future)(op)
    }
  }
}
