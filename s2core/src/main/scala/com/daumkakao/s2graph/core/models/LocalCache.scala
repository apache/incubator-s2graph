package com.daumkakao.s2graph.core.models

import java.util.concurrent.TimeUnit

import com.daumkakao.s2graph.core.{GraphConnection, Graph}
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import play.api.Logger
//import scalikejdbc.{AutoSession, DBSession, ConnectionPool, ConnectionPoolSettings}

/**
 * Created by shon on 5/16/15.
 */
//object Model {
//
//  val defaultConfigs = Map("db.default.driver" -> "com.mysql.jdbc.Driver",
//    "db.default.url" -> "jdbc:mysql://localhost:3306/graph_dev",
//    "db.default.password" -> "graph",
//    "db.default.user" -> "graph")
//
//  var settings: ConnectionPoolSettings = null
//  var maxSize = 10000
//  var ttl = 5
//  val logger = Graph.logger
//  def apply(config: Config) = {
//    val configVals = for ((k, v) <- defaultConfigs) yield {
//      logger.debug(s"initializing db connection: $k, $v")
//      val currentVal = GraphConnection.getOrElse(config)(k, v)
//      k -> currentVal
//    }
//
//    maxSize = GraphConnection.getOrElse(config)("cache.max.size", 10000)
//    ttl = GraphConnection.getOrElse(config)("cache.ttl.seconds", 120)
//
//    Class.forName(configVals("db.default.driver"))
//
//    settings = ConnectionPoolSettings(initialSize = 1, maxSize = 5,
//      connectionTimeoutMillis = 60000L, validationQuery = "select 1;")
//
//    ConnectionPool.singleton(configVals("db.default.url"), configVals("db.default.user"), configVals("db.default.password"), settings)
//  }
//}
trait LocalCache[V <: Object] {
  protected val ttl = HBaseModel.cacheTTL
  protected val maxSize = HBaseModel.maxCacheSize
  private lazy val cName = this.getClass.getSimpleName()

  val cache = CacheBuilder.newBuilder()
    .expireAfterWrite(ttl, TimeUnit.SECONDS)
    .maximumSize(maxSize)
    .build[String, V]()

  val caches = CacheBuilder.newBuilder()
    .expireAfterWrite(ttl, TimeUnit.SECONDS)
    .maximumSize(maxSize / 10).build[String, List[V]]()

  def withCache(key: String, useCache: Boolean = true)(op: => V): V = {
    val newKey = s"$cName:withCache:$key"
    val view = cache.asMap()
    if (useCache && view.containsKey(newKey)) {
//      Logger.debug(s"withCache: $newKey => Hit")
      view.get(newKey)
    }
    else {
//      Logger.debug(s"withCache: $newKey => Miss")
      val newVal = op
      cache.put(newKey, newVal)
      newVal
    }
  }
  def withCaches(key: String, useCache: Boolean = true)(op: => List[V]): List[V] = {
    val newKey = s"$cName:withCaches:$key"
    val view = caches.asMap()
    if (useCache && view.containsKey(newKey)) {
//      Logger.debug(s"withCaches: $newKey => Hit")
      view.get(newKey)
    }
    else {
//      Logger.debug(s"withCaches: $newKey => Miss")
      val newVal = op
      caches.put(newKey, newVal)
      newVal
    }
  }
  def expireCache(key: String): Unit = {
    val newKey = s"$cName:$key"
    cache.invalidate(newKey)
  }
  def expireCaches(key: String): Unit = {
    val newKey = s"$cName:$key"
    caches.invalidate(newKey)
  }
  def putsToCache(kvs: List[(String, V)]) = {
    kvs.foreach {
      case (key, value) =>
        val newKey = s"$cName:$key"
        cache.put(newKey, value)
    }
  }
}