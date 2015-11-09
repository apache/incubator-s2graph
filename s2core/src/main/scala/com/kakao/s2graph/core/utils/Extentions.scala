package com.kakao.s2graph.core.utils

import com.stumbleupon.async.{Callback, Deferred}

import scala.concurrent.{Promise, ExecutionContext, Future}

object Extensions {

  implicit class FutureOps[T](f: Future[T])(implicit ec: ExecutionContext) {

    def retryFallback(n: Int)(fallback: => T): Future[T] = n match {
      case i if i > 1 => f recoverWith { case t: Throwable => f.retryFallback(n - 1)(fallback) }
      case _ => Future.successful(fallback)
    }

    def retry(n: Int) = retryWith(n)(f)

    def retryWith(n: Int)(fallback: => Future[T]): Future[T] = n match {
      case i if i > 1 => f recoverWith { case t: Throwable => f.retryWith(n - 1)(fallback) }
      case _ => fallback
    }
  }
  implicit class DeferOps[T](d: Deferred[T])(implicit ex: ExecutionContext) {

    def retryFallback(n: Int)(fallback: => T): Deferred[T] = n match {
      case i if i > 1 => d.addErrback( new Callback[Deferred[T], Exception] {
        override def call(ex: Exception): Deferred[T] = { retryFallback(n - 1)(fallback) }
      })
    }

    def retry(n: Int) = retryWith(n)(d)

    def retryWith(n: Int)(fallback: => Deferred[T]): Deferred[T] = n match {
      case i if i > 1 => d.addErrback(new Callback[Deferred[T], Exception] {
        override def call(ex: Exception): Deferred[T] = { retryWith(n-1)(fallback) }
      })
    }

    def withCallback[R](op: T => R): Deferred[R]  = {
      d.addCallback(new Callback[R, T] { override def call(arg: T): R = op(arg) })
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

}
