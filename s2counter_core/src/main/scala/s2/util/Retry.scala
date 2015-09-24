package s2.util

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 1. 6..
 */
object Retry {
  @tailrec
  def apply[T](n: Int, withSleep: Boolean = true, tryCount: Int = 0)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case Failure(e) if e.isInstanceOf[RetryStopException] => throw e.getCause
      case _ if n > 1 =>
        // backoff
        if (withSleep) Thread.sleep(tryCount * 1000)
        apply(n - 1, withSleep, tryCount + 1)(fn)
      case Failure(e) => throw e
    }
  }
}

class RetryStopException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)

  def this(cause: Throwable) = this(cause.toString, cause)
}
