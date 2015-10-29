package com.kakao.s2graph.core.mysqls

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.kakao.s2graph.core.utils.{logger, SafeUpdateCache}
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import scalikejdbc._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure, Try}

import scala.language.higherKinds
import scala.language.implicitConversions

object Model {
  var maxSize = 10000
  var ttl = 60
  val ex: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

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

  val cName = this.getClass.getSimpleName()
  logger.info(s"LocalCache[$cName]: TTL[$ttl], MaxSize[$maxSize]")

  val optionCache = new SafeUpdateCache[V, Option](cName, maxSize, ttl)
  val listCache = new SafeUpdateCache[V, List](cName, maxSize, ttl)

  def withCache = optionCache.withCache _

  def withCaches = listCache.withCache _

  def expireCache = optionCache.invalidate _

  def expireCaches = listCache.invalidate _

  def putsToCache(kvs: List[(String, V)]) = kvs.foreach {
    case (key, value) => optionCache.put(key, Option(value))
  }

  def putsToCaches(kvs: List[(String, List[V])]) = kvs.foreach {
    case (key, values) => listCache.put(key, values)
  }
}

