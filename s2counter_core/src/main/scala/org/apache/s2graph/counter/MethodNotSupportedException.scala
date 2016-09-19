package org.apache.s2graph.counter

case class MethodNotSupportedException(message: String, cause: Throwable = null) extends Exception(message, cause)