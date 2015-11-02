package com.kakao.s2graph.core.utils


import com.stumbleupon.async.{Callback, Deferred}

import scala.concurrent.{Promise, Future}

object DeferOp {

  def deferredToFuture[A](d: Deferred[A])(fallback: A): Future[A] = {
    val promise = Promise[A]

    d.addBoth(new Callback[Unit, A] {
      def call(arg: A) = arg match {
        case e: Exception =>
          logger.error(s"deferred failed with return fallback: $e", e)
          promise.success(fallback)
        case _ => promise.success(arg)
      }
    })

    promise.future
  }

  def deferredToFutureWithoutFallback[T](d: Deferred[T]) = {
    val promise = Promise[T]

    d.addBoth(new Callback[Unit, T] {
      def call(arg: T) = arg match {
        case e: Exception =>
          logger.error(s"deferred return Exception: $e", e)
          promise.failure(e)
        case _ => promise.success(arg)
      }
    })

    promise.future
  }

  def deferredCallbackWithFallback[T, R](d: Deferred[T])(f: T => R, fallback: => R) = {
    d.addCallback(new Callback[R, T] {
      def call(args: T): R = {
        f(args)
      }
    }).addErrback(new Callback[R, Exception] {
      def call(e: Exception): R = {
        logger.error(s"Exception on deferred: $e", e)
        fallback
      }
    })
  }

  def errorBack(block: => Exception => Unit) = new Callback[Unit, Exception] {
    def call(ex: Exception): Unit = block(ex)
  }
}
