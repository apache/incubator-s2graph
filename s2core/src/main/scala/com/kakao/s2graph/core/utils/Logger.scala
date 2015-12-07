package com.kakao.s2graph.core.utils

//import play.api.Logger
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue

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


