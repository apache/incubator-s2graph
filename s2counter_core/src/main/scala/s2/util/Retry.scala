/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package s2.util

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}
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

object RetryAsync {
  def apply[T](n: Int, withSleep: Boolean = true, tryCount: Int = 0)(fn: => Future[T])(implicit ex: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    fn onComplete {
      case Success(x) => promise.success(x)
      case Failure(e) if e.isInstanceOf[RetryStopException] => promise.failure(e.getCause)
      case _ if n > 1 =>
        // backoff
        if (withSleep) Thread.sleep(tryCount * 1000)
        apply(n - 1, withSleep, tryCount + 1)(fn)
      case Failure(e) => promise.failure(e)
    }
    promise.future
  }
}

class RetryStopException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)

  def this(cause: Throwable) = this(cause.toString, cause)
}
