package com.kakao.s2graph.core.utils

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.cache.CacheBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SafeUpdateCache {

  case class CacheKey(key: String)

}

class SafeUpdateCache[T](prefix: String, maxSize: Int, ttl: Int)(implicit executionContext: ExecutionContext) {

  import SafeUpdateCache._

  implicit class StringOps(key: String) {
    def toCacheKey = new CacheKey(prefix + ":" + key)
  }

  def toTs() = (System.currentTimeMillis() / 1000).toInt

  private val cache = CacheBuilder.newBuilder().maximumSize(maxSize).build[CacheKey, (T, Int, AtomicBoolean)]()

  def put(key: String, value: T) = cache.put(key.toCacheKey, (value, toTs, new AtomicBoolean(false)))

  def invalidate(key: String) = cache.invalidate(key.toCacheKey)

  def withCache(key: String)(op: => T): T = {
    val cacheKey = key.toCacheKey
    val cachedValWithTs = cache.getIfPresent(cacheKey)

    if (cachedValWithTs == null) {
      // fetch and update cache.
      val newValue = op
      cache.put(cacheKey, (newValue, toTs(), new AtomicBoolean(false)))
      newValue
    } else {
      val (cachedVal, updatedAt, isUpdating) = cachedValWithTs
      if (toTs() < updatedAt + ttl) cachedVal // in cache TTL
      else {
        val running = isUpdating.getAndSet(true)
        if (running) cachedVal
        else {
          Future(op)(executionContext) onComplete {
            case Failure(ex) =>
              cache.put(cacheKey, (cachedVal, toTs(), new AtomicBoolean(false))) // keep old value
              logger.error(s"withCache update failed.")
            case Success(newValue) =>
              cache.put(cacheKey, (newValue, toTs(), new AtomicBoolean(false))) // update new value
              logger.info(s"withCache update success: $cacheKey")
          }
          cachedVal
        }
      }
    }
  }
}

