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

package org.apache.s2graph.core.utils

import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future, Promise}

object Extensions {

  def retryOnSuccess[T](maxRetryNum: Int, n: Int = 1)(
      fn: => Future[T]
  )(shouldStop: T => Boolean)(implicit ex: ExecutionContext): Future[T] =
    n match {
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

  def retryOnFailure[T](maxRetryNum: Int, n: Int = 1)(
      fn: => Future[T]
  )(fallback: => T)(implicit ex: ExecutionContext): Future[T] = n match {
    case i if n <= maxRetryNum =>
      fn recoverWith {
        case t: Throwable =>
          logger.info(s"retryOnFailure $n $t")
          retryOnFailure(maxRetryNum, n + 1)(fn)(fallback)
      }
    case _ =>
      Future.successful(fallback)
  }

  implicit class DeferOps[T](d: Deferred[T])(implicit ex: ExecutionContext) {
    def map[R](dummy: => T)(op: T => R): Deferred[R] = {
      val newDefer = new Deferred[R]

      d.addCallback(new Callback[T, T] {
        override def call(arg: T): T = {
          newDefer.callback(op(arg))
          arg
        }
      })

      d.addErrback(new Callback[T, Exception] {
        override def call(e: Exception): T = {
          newDefer.callback(e)
          dummy
        }
      })

      newDefer
    }

    def mapWithFallback[R](dummy: => T)(fallback: Exception => R)(op: T => R): Deferred[R] = {
      val newDefer = new Deferred[R]

      d.addCallback(new Callback[T, T] {
        override def call(arg: T): T = {
          newDefer.callback(op(arg))
          arg
        }
      })

      d.addErrback(new Callback[T, Exception] {
        override def call(e: Exception): T = {
          newDefer.callback(fallback(e))
          dummy
        }
      })

      newDefer
    }

    def toFuture(dummy: => T): Future[T] = {
      val promise = Promise[T]

      val cb = new Callback[T, T] {
        override def call(arg: T): T = {
          promise.success(arg)
          arg
        }
      }

      val eb = new Callback[T, Exception] {
        override def call(e: Exception): T = {
          promise.failure(e)
          dummy
        }
      }

      d.addCallbacks(cb, eb)

      promise.future
    }
  }

  implicit class ConfigOps(config: Config) {
    def getBooleanWithFallback(key: String, defaultValue: Boolean): Boolean =
      if (config.hasPath(key)) config.getBoolean(key) else defaultValue
  }
}
