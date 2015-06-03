package com.daumkakao.s2graph.core.mysqls

import java.util.concurrent.TimeUnit

import com.daumkakao.s2graph.core.GraphConnection
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import play.api.Logger
import scalikejdbc._

/**
 * Created by shon on 6/3/15.
 */
object Model {

  val defaultConfigs = Map("db.default.driver" -> "com.mysql.jdbc.Driver",
    "db.default.url" -> "jdbc:mysql://localhost:3306/graph_dev",
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
trait Model[V] extends SQLSyntaxSupport[V] {
  import Model._
  private lazy val cName = this.getClass.getSimpleName()
  implicit val s: DBSession = AutoSession

  val cache = CacheBuilder.newBuilder()
  .expireAfterWrite(ttl, TimeUnit.SECONDS)
  .maximumSize(maxSize)
  .build[String, Option[V]]()

  val caches = CacheBuilder.newBuilder()
  .expireAfterWrite(ttl, TimeUnit.SECONDS)
  .maximumSize(maxSize / 10).build[String, List[V]]()

  def withCache(key: String)(op: => Option[V]): Option[V] = {
    val newKey = s"$cName:$key"
    val view = cache.asMap()
    if (view.containsKey(newKey)) view.get(newKey)
    else {
      val newVal = op
      cache.put(newKey, newVal)
      newVal
    }
  }
  def withCaches(key: String)(op: => List[V]): List[V] = {
    val newKey = s"$cName:$key"
    val view = caches.asMap()
    if (view.containsKey(newKey)) view.get(newKey)
    else {
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
        cache.put(newKey, Some(value))
    }
  }
}
