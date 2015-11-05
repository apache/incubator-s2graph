package com.kakao.s2graph.core.utils

import scala.concurrent.{ExecutionContext, Future}

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

}
