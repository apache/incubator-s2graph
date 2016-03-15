package org.apache.s2graph

package object counter {
  val VERSION_1: Byte = 1
  val VERSION_2: Byte = 2

  case class MethodNotSupportedException(message: String, cause: Throwable = null) extends Exception(message, cause)
}
