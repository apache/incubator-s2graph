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

package com.kakao.s2graph.core.utils

import play.api.libs.json.JsValue
import org.slf4j.LoggerFactory

import scala.language.{higherKinds, implicitConversions}

object logger {

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

  def info[T: Loggable](msg: => T) = logger.info(implicitly[Loggable[T]].toLogMessage(msg))

  def debug[T: Loggable](msg: => T) = logger.debug(implicitly[Loggable[T]].toLogMessage(msg))

  def error[T: Loggable](msg: => T, exception: => Throwable) = errorLogger.error(implicitly[Loggable[T]].toLogMessage(msg), exception)

  def error[T: Loggable](msg: => T) = errorLogger.error(implicitly[Loggable[T]].toLogMessage(msg))
}


