package com.daumkakao.s2graph.core.mysqls

import java.util.concurrent.TimeUnit

import com.daumkakao.s2graph.core.Graph._
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import play.api.Logger
import scalikejdbc._

/**
 * Created by shon on 6/3/15.
 */
object Model {
  var settings: ConnectionPoolSettings = null
  var maxSize = 10000
  var ttl = 60
  def apply(config: Config) = {

    maxSize = config.getInt("cache.max.size")
    ttl = config.getInt("cache.ttl.seconds")

    Class.forName(config.getString("db.default.driver"))

    settings = ConnectionPoolSettings(initialSize = 1, maxSize = 5,
      connectionTimeoutMillis = 60000L, validationQuery = "select 1;")

    ConnectionPool.singleton(config.getString("db.default.url"),
      config.getString("db.default.user"), config.getString("db.default.password"), settings)
  }
}
trait Model[V] extends SQLSyntaxSupport[V] {
  import Model._
  private lazy val cName = this.getClass.getSimpleName()
  implicit val s: DBSession = AutoSession

  Logger.info(s"LocalCache[$cName]: TTL[$ttl], MaxSize[$maxSize]")
  val cache = CacheBuilder.newBuilder()
  .expireAfterWrite(ttl, TimeUnit.SECONDS)
  .maximumSize(maxSize)
  .build[String, Option[V]]()

  val caches = CacheBuilder.newBuilder()
  .expireAfterWrite(ttl, TimeUnit.SECONDS)
  .maximumSize(maxSize / 10).build[String, List[V]]()

  def withCache(key: String)(op: => Option[V]): Option[V] = {
    val newKey = cName + ":" + key
//    s"$cName:$key"
    val view = cache.asMap()
    if (view.containsKey(newKey)) {
//      Logger.debug(s"$cName: withCache: $key => HIT")
      val value = view.get(newKey)
      if (value == null) op else value
    } else {
//      Logger.debug(s"$cName: withCache: $key => MISS")
      val newVal = op
//      if (!newVal.isEmpty)
        cache.put(newKey, newVal)
      newVal
    }
  }
  def withCaches(key: String)(op: => List[V]): List[V] = {
//    val newKey = s"$cName:$key"
    val newKey = cName + ":" + key
    val view = caches.asMap()
    if (view.containsKey(newKey)) {
//      Logger.debug(s"$cName: withCaches: $key => HIT")
      val value = view.get(newKey)
      if (value == null) op else value
    } else {
//      Logger.debug(s"$cName: withCaches: $key => MISS")
      val newVal = op
//      if (!newVal.isEmpty)
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
//        Logger.info(s"$cName: putsToCache: $key")
        cache.put(newKey, Some(value))
    }
  }
  def putsToCaches(kvs: List[(String, List[V])]) = {
    kvs.foreach {
      case (key, values) =>
        val newKey = s"$cName:$key"
//        Logger.info(s"$cName: putsToCaches: $key")
        caches.put(newKey, values)
    }
  }
}
