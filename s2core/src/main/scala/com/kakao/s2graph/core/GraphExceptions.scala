package com.kakao.s2graph.core


object GraphExceptions {

  case class JsonParseException(msg: String) extends Exception(msg)

  case class LabelNotExistException(msg: String) extends Exception(msg)

  case class ModelNotFoundException(msg: String) extends Exception(msg)

  case class MaxPropSizeReachedException(msg: String) extends Exception(msg)

  case class LabelAlreadyExistException(msg: String) extends Exception(msg)

  case class InternalException(msg: String) extends Exception(msg)

  case class IllegalDataTypeException(msg: String) extends Exception(msg)

  case class WhereParserException(msg: String, ex: Exception = null) extends Exception(msg, ex)

  case class BadQueryException(msg: String, ex: Throwable = null) extends Exception(msg, ex)

  case class InvalidHTableException(msg: String) extends Exception(msg)

  case class FetchTimeoutException(msg: String) extends Exception(msg)

  case class DropRequestException(msg: String) extends Exception(msg)
}
