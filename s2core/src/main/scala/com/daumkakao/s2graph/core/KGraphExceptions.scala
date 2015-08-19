package com.daumkakao.s2graph.core


object KGraphExceptions {

  case class JsonParseException(msg: String) extends Exception(s"$msg\n")
  case class LabelNotExistException(msg: String) extends Exception(s"$msg\n")
  case class LabelMetaExistException(msg: String) extends Exception(s"$msg\n")
  case class MaxPropSizeReachedException(msg: String) extends Exception(s"$msg\n")
  case class LabelAlreadyExistException(msg: String) extends Exception(s"$msg\n")
  case class InternalException(msg: String) extends Exception(s"$msg\n")
  case class IllegalDataTypeException(msg: String) extends Exception(s"$msg\n")
  case class IllegalDataRangeException(msg: String) extends Exception(s"$msg\n")
  case class HBaseStorageException(msg: String) extends Exception(s"$msg\n")
  case class HBaseConnectionException(msg: String) extends Exception(s"$msg\n")
  case class DuplicateTimestampException(msg: String) extends Exception(s"$msg\n")
  case class BadQueryException(msg: String, ex: Throwable = null) extends Exception(s"$msg\n", ex)
}
