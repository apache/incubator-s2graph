package com.daumkakao.s2graph.core


object KGraphExceptions {

  case class JsonParseException(msg: String) extends Exception(msg)

  case class LabelNotExistException(msg: String) extends Exception(msg)

  case class ModelNotFoundException(msg: String) extends Exception(msg)

  case class MaxPropSizeReachedException(msg: String) extends Exception(msg)

  case class LabelAlreadyExistException(msg: String) extends Exception(msg)

  case class InternalException(msg: String) extends Exception(msg)

  case class IllegalDataTypeException(msg: String) extends Exception(msg)

  case class BadQueryException(msg: String, ex: Exception = null) extends Exception(msg, ex)

}
