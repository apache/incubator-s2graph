package com.kakao.s2graph.core.mysqls

import java.util.concurrent.TimeUnit

import com.kakao.s2graph.logger
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import scalikejdbc._

import scala.util.{Failure, Try}

/**
 * Created by shon on 6/3/15.
 */
object Model {
  var maxSize = 10000
  var ttl = 60

  def apply(config: Config) = {
    maxSize = config.getInt("cache.max.size")
    ttl = config.getInt("cache.ttl.seconds")
    Class.forName(config.getString("db.default.driver"))

    val settings = ConnectionPoolSettings(
      initialSize = 10,
      maxSize = 10,
      connectionTimeoutMillis = 30000L,
      validationQuery = "select 1;")

    ConnectionPool.singleton(
      config.getString("db.default.url"),
      config.getString("db.default.user"),
      config.getString("db.default.password"),
      settings)
  }

  def withTx[T](block: DBSession => T): Try[T] = {
    val conn = DB(ConnectionPool.borrow())

    val res = Try {
      conn.begin()
      val session = conn.withinTxSession()
      val result = block(session)

      conn.commit()

      result
    } recoverWith {
      case e: Exception =>
        conn.rollbackIfActive()
        Failure(e)
    }
    conn.close()

    res
  }
}

trait Model[V] extends SQLSyntaxSupport[V] {

  import Model._

  private lazy val cName = this.getClass.getSimpleName()

  logger.info(s"LocalCache[$cName]: TTL[$ttl], MaxSize[$maxSize]")
  val cache = CacheBuilder.newBuilder()
    .expireAfterWrite(ttl, TimeUnit.SECONDS)
    .maximumSize(maxSize)
    .build[String, Option[V]]()

  val caches = CacheBuilder.newBuilder()
    .expireAfterWrite(ttl, TimeUnit.SECONDS)
    .maximumSize(maxSize / 10).build[String, List[V]]()

  def withCache(key: String)(op: => Option[V]): Option[V] = {
    val newKey = cName + ":" + key
    val oldValue = cache.getIfPresent(newKey)
    if (oldValue == null) {
      val newValue = op
      cache.put(newKey, newValue)
      newValue
    } else {
      oldValue
    }
  }

  def withCaches(key: String)(op: => List[V]): List[V] = {
    val newKey = cName + ":" + key
    val oldValue = caches.getIfPresent(newKey)
    if (oldValue == null) {
      val newValue = op
      caches.put(newKey, newValue)
      newValue
    } else {
      oldValue
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

  def putsToCaches(kvs: List[(String, List[V])]) = {
    kvs.foreach {
      case (key, values) =>
        val newKey = s"$cName:$key"
        caches.put(newKey, values)
    }
  }
}
