package com.kakao.s2graph

import play.api.Logger
import play.api.libs.json.JsValue

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



