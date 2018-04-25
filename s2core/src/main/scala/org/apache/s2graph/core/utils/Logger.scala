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

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue

import scala.language.{higherKinds, implicitConversions}
import scala.util.{Random, Try}

object logger {
  val conf = ConfigFactory.load()

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

    implicit val booleanLoggable = new Loggable[Boolean] {
      def toLogMessage(msg: Boolean) = msg.toString()
    }
  }

  private val logger = LoggerFactory.getLogger("application")
  private val errorLogger = LoggerFactory.getLogger("error")
  private val metricLogger = LoggerFactory.getLogger("metrics")
  private val queryLogger = LoggerFactory.getLogger("query")
  private val malformedLogger = LoggerFactory.getLogger("malformed")
  private val syncLogger = LoggerFactory.getLogger("meta_sync")

  def metric[T: Loggable](msg: => T) = metricLogger.info(implicitly[Loggable[T]].toLogMessage(msg))

  def syncInfo[T: Loggable](msg: => T) = syncLogger.info(implicitly[Loggable[T]].toLogMessage(msg))

  def info[T: Loggable](msg: => T) = logger.info(implicitly[Loggable[T]].toLogMessage(msg))

  def warn[T: Loggable](msg: => T) = logger.warn(implicitly[Loggable[T]].toLogMessage(msg))

  def debug[T: Loggable](msg: => T) = if (logger.isDebugEnabled) logger.debug(implicitly[Loggable[T]].toLogMessage(msg))

  def error[T: Loggable](msg: => T, exception: => Throwable) = errorLogger.error(implicitly[Loggable[T]].toLogMessage(msg), exception)

  def error[T: Loggable](msg: => T) = errorLogger.error(implicitly[Loggable[T]].toLogMessage(msg))

  def query[T: Loggable](msg: => T) = queryLogger.info(implicitly[Loggable[T]].toLogMessage(msg))

  def malformed[T: Loggable](msg: => T, exception: => Throwable) = malformedLogger.error(implicitly[Loggable[T]].toLogMessage(msg), exception)
}


