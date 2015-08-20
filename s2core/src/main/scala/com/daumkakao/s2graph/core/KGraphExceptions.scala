package com.daumkakao.s2graph.core


object KGraphExceptions {

  case class JsonParseException(msg: String) extends Exception(msg)
  case class LabelNotExistException(msg: String) extends Exception(msg)
  case class LabelMetaExistException(msg: String) extends Exception(msg)
  case class MaxPropSizeReachedException(msg: String) extends Exception(msg)
  case class LabelAlreadyExistException(msg: String) extends Exception(msg)
  case class InternalException(msg: String) extends Exception(msg)
  case class IllegalDataTypeException(msg: String) extends Exception(msg)
  case class IllegalDataRangeException(msg: String) extends Exception(msg)
  case class HBaseStorageException(msg: String) extends Exception(msg)
  case class HBaseConnectionException(msg: String) extends Exception(msg)
  case class DuplicateTimestampException(msg: String) extends Exception(msg)
  case class BadQueryException(msg: String, ex: Throwable = null) extends Exception(msg, ex)
}
