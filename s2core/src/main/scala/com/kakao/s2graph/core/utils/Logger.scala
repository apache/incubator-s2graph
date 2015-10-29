package com.kakao.s2graph.core.utils

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.cache.CacheBuilder
import play.api.Logger
import play.api.libs.json.JsValue

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Failure}

import scala.language.higherKinds
import scala.language.implicitConversions

package object logger {

  trait Loggable[T] {
    def toLogMessage(msg: T): String
  }

  object Loggable {
    implicit val stringLoggable = new Loggable[String] {
      def toLogMessage(msg: String) = msg
    }

    implicit def numericLoggable[T: Numeric] = new Loggable[T] {
      def toLogMessage(msg: T) = msg.toString
    }

    implicit val jsonLoggable = new Loggable[JsValue] {
      def toLogMessage(msg: JsValue) = msg.toString()
    }
  }

  private val logger = Logger("application")
  private val errorLogger = Logger("error")

  def info[T: Loggable](msg: => T) = logger.info(implicitly[Loggable[T]].toLogMessage(msg))

  def debug[T: Loggable](msg: => T) = logger.debug(implicitly[Loggable[T]].toLogMessage(msg))

  def error[T: Loggable](msg: => T, exception: => Throwable) = errorLogger.error(implicitly[Loggable[T]].toLogMessage(msg), exception)

  def error[T: Loggable](msg: => T) = errorLogger.error(implicitly[Loggable[T]].toLogMessage(msg))
}

object SafeUpdateCache {
  case class CacheKey(key: String)
}

class SafeUpdateCache[T, M[_]](prefix: String, maxSize: Int, ttl: Int)(implicit executionContext: ExecutionContext) {
  import SafeUpdateCache._

  implicit class StringOps(key: String) {
    def toCacheKey = new CacheKey(prefix + ":" + key)
  }

  def toTs() = (System.currentTimeMillis() / 1000).toInt

  private val cache = CacheBuilder.newBuilder().maximumSize(maxSize).build[CacheKey, (M[T], Int, AtomicBoolean)]()

  def put(key: String, value: M[T]) = cache.put(key.toCacheKey, (value, toTs, new AtomicBoolean(false)))

  def invalidate(key: String) = cache.invalidate(key.toCacheKey)

  def withCache(key: String)(op: => M[T]): M[T] = {
    val newKey = key.toCacheKey
    val cachedValWithTs = cache.getIfPresent(newKey)

    if (cachedValWithTs == null) {
      // fetch and update cache.
      val newValue = op
      cache.put(newKey, (newValue, toTs(), new AtomicBoolean(false)))
      newValue
    } else {
      val (cachedVal, updatedAt, isUpdating) = cachedValWithTs
      if (toTs() < updatedAt + ttl) cachedVal
      else {
        val running = isUpdating.getAndSet(true)
        if (running) cachedVal
        else {
          Future {
            val newValue = op
            val newUpdatedAt = toTs()
            cache.put(newKey, (newValue, newUpdatedAt, new AtomicBoolean(false)))
            newValue
          }(executionContext) onComplete {
            case Failure(ex) => logger.error(s"withCache update failed.")
            case Success(s) => logger.info(s"withCache update success: $newKey")
          }
          cachedVal
        }
      }
    }
  }
}

