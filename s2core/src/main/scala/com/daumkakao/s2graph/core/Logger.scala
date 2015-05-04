package com.daumkakao.s2graph.core
object Logger {

  def ticked[T](s: String)(op: => T) = {
    val startAt = System.currentTimeMillis()
    val ret = op
    val duration = System.currentTimeMillis() - startAt
    info(s"$s elapsed time: $duration")
    ret
  }
  def debug(message: String) = play.api.Logger.debug(message)
  def info(message: String) = play.api.Logger.info(message)
  def error(message: String, ex: Throwable = null) = if (ex == null) play.api.Logger.error(message) else play.api.Logger.error(message, ex)
  val adminLogger = play.api.Logger("admin")
  val queryLogger = play.api.Logger("query")
  val actorLogger = play.api.Logger("actor")
  val logger = play.api.Logger
}