package com.kakao.s2graph.core.utils

import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future, Promise}

object Extensions {


  def retryOnSuccess[T](maxRetryNum: Int, n: Int = 1)(fn: => Future[T])(shouldStop: T => Boolean)(implicit ex: ExecutionContext): Future[T] = n match {
    case i if n <= maxRetryNum =>
      fn.flatMap { result =>
        if (!shouldStop(result)) {
          logger.info(s"retryOnSuccess $n")
          retryOnSuccess(maxRetryNum, n + 1)(fn)(shouldStop)
        } else {
          Future.successful(result)
        }
      }
    case _ => fn
  }

  def retryOnFailure[T](maxRetryNum: Int, n: Int = 1)(fn: => Future[T])(fallback: => T)(implicit ex: ExecutionContext): Future[T] = n match {
    case i if n <= maxRetryNum =>
      fn recoverWith { case t: Throwable =>
        logger.info(s"retryOnFailure $n $t")
        retryOnFailure(maxRetryNum, n + 1)(fn)(fallback)
      }
    case _ =>
      Future.successful(fallback)
  }


  implicit class DeferOps[T](d: Deferred[T])(implicit ex: ExecutionContext) {

    def withCallback[R](op: T => R): Deferred[R] = {
      d.addCallback(new Callback[R, T] {
        override def call(arg: T): R = op(arg)
      })
    }

    def recoverWith(op: Exception => T): Deferred[T] = {
      d.addErrback(new Callback[Deferred[T], Exception] {
        override def call(e: Exception): Deferred[T] = Deferred.fromResult(op(e))
      })
    }


    def toFuture: Future[T] = {
      val promise = Promise[T]

      d.addBoth(new Callback[Unit, T] {
        def call(arg: T) = arg match {
          case e: Exception => promise.failure(e)
          case _ => promise.success(arg)
        }
      })

      promise.future
    }

    def toFutureWith(fallback: => T): Future[T] = {
      toFuture recoverWith { case t: Throwable => Future.successful(fallback) }
    }

  }

  implicit class ConfigOps(config: Config) {
    def getBooleanWithFallback(key: String, defaultValue: Boolean): Boolean =
      if (config.hasPath(key)) config.getBoolean(key) else defaultValue
  }

}
