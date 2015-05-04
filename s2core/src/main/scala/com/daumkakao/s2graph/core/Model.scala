package com.daumkakao.s2graph.core

import scalikejdbc.ConnectionPool
import scalikejdbc._
import com.twitter.util.SynchronizedLruMap
import HBaseElement.InnerVal
import play.api.libs.json._
import com.google.common.cache.CacheBuilder
import java.util.concurrent.{Callable, TimeUnit}
import com.google.common.cache.CacheLoader
import com.typesafe.config.Config

object Model {

  val defaultConfigs = Map("db.default.driver" -> "com.mysql.jdbc.Driver",
    "db.default.url" -> "jdbc:mysql://localhost:13306/graph_alpha",
    "db.default.password" -> "graph",
    "db.default.user" -> "graph")

  var settings: ConnectionPoolSettings = null
  var maxSize = 10000
  var ttl = 60
  def apply(config: Config) = {
    val configVals = for ((k, v) <- defaultConfigs) yield {
      //      println("initializing db connection: ", k, v)
      val currentVal = GraphConnection.getOrElse(config)(k, v)
      Logger.info(s"Model: $k -> $currentVal")
      println(s"Model: $k -> $currentVal")
      k -> currentVal
    }

    maxSize = GraphConnection.getOrElse(config)("cache.max.size", 10000)
    ttl = GraphConnection.getOrElse(config)("cache.ttl.seconds", 120)

    Class.forName(configVals("db.default.driver"))

    settings = ConnectionPoolSettings(initialSize = 1, maxSize = 5,
      connectionTimeoutMillis = 60000L, validationQuery = "select 1;")

    ConnectionPool.singleton(configVals("db.default.url"), configVals("db.default.user"), configVals("db.default.password"), settings)
  }
}
//trait LocalCache[V] {
//  import Model._
//  private lazy val cName = this.getClass.getSimpleName()
//  private val adminLogger = Logger.adminLogger
//  implicit val s: DBSession = AutoSession
//
//  val cache = CacheBuilder.newBuilder()
//  .expireAfterWrite(ttl, TimeUnit.SECONDS)
//  .maximumSize(maxSize)
//  .build[String, Option[V]]()
//
//  val caches = CacheBuilder.newBuilder()
//  .expireAfterWrite(ttl, TimeUnit.SECONDS)
//  .maximumSize(maxSize / 10).build[String, List[V]]()
//
//  def withCache(key: String)(op: => Option[V]): Option[V] = {
//    val newKey = s"$cName:$key"
//    val view = cache.asMap()
//    if (view.containsKey(newKey)) view.get(newKey)
//    else {
//      val newVal = op
//      cache.put(newKey, newVal)
//      newVal
//    }
//  }
//  def withCaches(key: String)(op: => List[V]): List[V] = {
//    val newKey = s"$cName:$key"
//    val view = caches.asMap()
//    if (view.containsKey(newKey)) view.get(newKey)
//    else {
//      val newVal = op
//      caches.put(newKey, newVal)
//      newVal
//    }
//  }
//  def expireCache(key: String): Unit = {
//    val newKey = s"$cName:$key"
//    cache.invalidate(newKey)
//  }
//  def expireCaches(key: String): Unit = {
//    val newKey = s"$cName:$key"
//    caches.invalidate(newKey)
//  }
//  def putsToCache(kvs: List[(String, V)]) = {
//    kvs.foreach {
//      case (key, value) =>
//        val newKey = s"$cName:$key"
//        cache.put(newKey, Some(value))
//    }
//  }
//}
trait Model[V] extends SQLSyntaxSupport[V] {

  import Model._
  implicit val s: DBSession = AutoSession
  private val cache = new SynchronizedLruMap[String, (Option[V], Int)](maxSize)
  private val caches = new SynchronizedLruMap[String, (List[V], Int)](maxSize / 10)
  private lazy val cName = this.getClass.getSimpleName()
  private val adminLogger = Logger.adminLogger
  private def withTTL(key: String, op: => Option[V]): Option[V] = {
    cache.get(key) match {
      case Some((existed, ts)) =>
        val elapsed = System.currentTimeMillis() / 1000 - ts
        if (elapsed > ttl) {
          adminLogger.debug(s"cache expired: $key")
          cache.remove(key)
          val fetched = op
          val current = System.currentTimeMillis() / 1000
          cache.put(key, (fetched, current.toInt))
          adminLogger.debug(s"cache renew: $key, $fetched")
          fetched
        } else {
          adminLogger.debug(s"cache hit: $key, $existed")
          existed
        }
      case None =>
        val fetched = op
        val current = System.currentTimeMillis() / 1000
        cache.put(key, (fetched, current.toInt))
        adminLogger.debug(s"cache miss: $key, $fetched")
        fetched
    }
  }
  private def withTTLs(key: String, op: => List[V]): List[V] = {
    caches.get(key) match {
      case Some((existed, ts)) =>
        val elapsed = System.currentTimeMillis() / 1000 - ts
        if (elapsed > ttl) {
          caches.remove(key)
          val fetched = op
          val current = System.currentTimeMillis() / 1000
          caches.put(key, (fetched, current.toInt))
          adminLogger.debug(s"list cache renew: $key, $fetched")
          fetched
        } else {
          adminLogger.debug(s"list cache hit: $key, $existed")
          existed
        }
      case None =>
        val fetched = op
        val current = System.currentTimeMillis() / 1000
        caches.put(key, (fetched, current.toInt))
        adminLogger.debug(s"list cache miss: $key, $fetched")
        fetched
    }
  }
  def withCache(key: String)(op: => Option[V]): Option[V] = {
    withTTL(s"$cName:$key", op)
  }
  def withCaches(key: String)(op: => List[V]): List[V] = {
    withTTLs(s"$cName:$key", op)
  }
  def expireCache(key: String): Unit = {
    val k = s"$cName:$key"
    adminLogger.info(s"expireCache: $k")
    if (cache.contains(k)) cache.remove(k)
  }
  def expireCaches(key: String): Unit = {
    val k = s"$cName:$key"
    adminLogger.info(s"expireCaches: $k")
    if (caches.contains(k)) caches.remove(k)
  }
  def putsToCache(kvs: List[(String, V)]) = {
    kvs.foreach {
      case (key, value) =>
        withTTL(s"$cName:$key", Some(value))
    }
  }
}