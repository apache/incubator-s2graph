package com.daumkakao.s2graph.core.models

import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import org.apache.hadoop.hbase.client.Result

/**
 * Created by shon on 5/16/15.
 */

trait LocalCache[V <: Result] {
  protected lazy val ttl = Model.cacheTTL
  protected lazy val maxSize = Model.maxCacheSize
  //  private lazy val cName = this.getClass.getSimpleName()

  lazy val cache = CacheBuilder.newBuilder()
    .expireAfterWrite(ttl, TimeUnit.SECONDS)
    .maximumSize(maxSize)
    .build[String, V]()

  lazy val caches = CacheBuilder.newBuilder()
    .expireAfterWrite(ttl, TimeUnit.SECONDS)
    .maximumSize(maxSize / 10).build[String, List[V]]()

  def withCache(key: String)(op: => V): V = {
    val newKey = s"withCache:$key"
    val view = cache.asMap()
    if (view.containsKey(newKey)) {
      //      Logger.debug(s"$newKey => Hit")
      val value = view.get(newKey)
      if (value == null) op else value
    }
    else {
      val newVal = op
      cache.put(newKey, newVal)
      //      caches.put(newKey, newVal)
      //      Logger.debug(s"$newKey => Miss")
      newVal
    }
  }

  def withCaches(key: String)(op: => List[V]): List[V] = {
    val newKey = s"withCaches:$key"
    val view = caches.asMap()
    if (view.containsKey(newKey)) {
      //      Logger.debug(s"withCaches: $newKey => Hit")
      val value = view.get(newKey)
      if (value == null) op else value
    }
    else {
      //      Logger.debug(s"withCaches: $newKey => Miss")
      val newVal = op
      caches.put(newKey, newVal)
      //      caches.put(newKey, newVal)
      newVal
    }
  }

  def expireCache(key: String): Unit = {
    val newKey = s"$key"
    cache.invalidate(newKey)
  }

  def expireCaches(key: String): Unit = {
    val newKey = s"$key"
    caches.invalidate(newKey)
  }

  def putsToCache(kvs: List[(String, V)]) = {
    kvs.foreach {
      case (key, value) =>
        val newKey = s"$key"
        cache.put(newKey, value)
    }
  }
}