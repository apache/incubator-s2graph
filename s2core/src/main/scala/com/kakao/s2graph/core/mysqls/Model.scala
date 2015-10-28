package com.kakao.s2graph.core.mysqls

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.kakao.s2graph.logger
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import scalikejdbc._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure, Try}

/**
 * Created by shon on 6/3/15.
 */
object Model {
  var maxSize = 10000
  var ttl = 60
  val ex: ExecutionContext  = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

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
  implicit val ex: ExecutionContext = Model.ex
  
  private lazy val cName = this.getClass.getSimpleName()

  logger.info(s"LocalCache[$cName]: TTL[$ttl], MaxSize[$maxSize]")
  val cache = CacheBuilder.newBuilder()
//    .expireAfterWrite(ttl, TimeUnit.SECONDS)
    .maximumSize(maxSize)
    .build[String, (Option[V], Int, AtomicBoolean)]()

  val caches = CacheBuilder.newBuilder()
//    .expireAfterWrite(ttl, TimeUnit.SECONDS)
    .maximumSize(maxSize / 10).build[String, (List[V], Int, AtomicBoolean)]()

  def toTs() = (System.currentTimeMillis() / 1000).toInt

  def withCache(key: String)(op: => Option[V]): Option[V] = {
    val newKey = cName + ":" + key
    val cachedValWithTs = cache.getIfPresent(newKey)
    if (cachedValWithTs == null) {
      // fetch and update cache.
      val newValue = op
      val newUpdatedAt = toTs
      cache.put(newKey, (newValue, newUpdatedAt, new AtomicBoolean(false)))
      newValue
    } else {
      val (cachedVal, updatedAt, isRunning) = cachedValWithTs
      if (toTs() < updatedAt + ttl) cachedVal
      else {
        val running = isRunning.getAndSet(true)
        if (running) cachedVal
        else {
          Future {
            val newValue = op
            val newUpdatedAt = toTs()
            cache.put(newKey, (newValue, newUpdatedAt, new AtomicBoolean(false)))
          } onComplete {
            case Failure(ex) => logger.error(s"withCache update failed.")
            case Success(s) => logger.info(s"withCache update successed.")
          }
          cachedVal
        }
      }
    }
  }

  def withCaches(key: String)(op: => List[V]): List[V] = {
    val newKey = cName + ":" + key
    val cachedValWithTs = caches.getIfPresent(newKey)
    if (cachedValWithTs == null) {
      // fetch and update cache.
      val newValue = op
      val newUpdatedAt = toTs
      caches.put(newKey, (newValue, newUpdatedAt, new AtomicBoolean(false)))
      newValue
    } else {
      val (cachedVal, updatedAt, isRunning) = cachedValWithTs
      if (toTs() < updatedAt + ttl) cachedVal
      else {
        val running = isRunning.getAndSet(true)
        if (running) cachedVal
        else {
          Future {
            val newValue = op
            val newUpdatedAt = toTs()
            caches.put(newKey, (newValue, newUpdatedAt, new AtomicBoolean(false)))
          } onComplete {
            case Failure(ex) => logger.error(s"withCache update failed.")
            case Success(s) => logger.info(s"withCache update successed.")
          }
          cachedVal
        }
      }
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
        cache.put(newKey, (Option(value), toTs(), new AtomicBoolean((false))))
    }
  }

  def putsToCaches(kvs: List[(String, List[V])]) = {
    kvs.foreach {
      case (key, values) =>
        val newKey = s"$cName:$key"
        caches.put(newKey, (values, toTs(), new AtomicBoolean(false)))
    }
  }
}
