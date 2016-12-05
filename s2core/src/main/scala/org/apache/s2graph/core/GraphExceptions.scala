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

package org.apache.s2graph.core

object GraphExceptions {
  var fillStckTrace = true
  class BaseException(msg: String) extends Exception(msg) {
    override def fillInStackTrace: Exception = {
      if (fillStckTrace) super.fillInStackTrace()
      this
    }
  }
  class NoStackException(msg: String) extends Exception(msg) {
    override def fillInStackTrace: Exception = {
      this
    }
  }

  class NoStackCauseException(msg: String, ex: Throwable)
      extends Exception(msg, ex) {
    override def fillInStackTrace: Exception = {
      this
    }
  }

  case class JsonParseException(msg: String) extends Exception(msg)

  case class LabelNotExistException(msg: String) extends Exception(msg)

  case class ModelNotFoundException(msg: String) extends Exception(msg)

  case class MaxPropSizeReachedException(msg: String) extends Exception(msg)

  case class LabelAlreadyExistException(msg: String) extends Exception(msg)

  case class LabelNameTooLongException(msg: String) extends Exception(msg)

  case class InternalException(msg: String) extends Exception(msg)

  case class IllegalDataTypeException(msg: String) extends Exception(msg)

  case class WhereParserException(msg: String, ex: Exception = null)
      extends Exception(msg, ex)

  case class BadQueryException(msg: String, ex: Throwable = null)
      extends Exception(msg, ex)

  case class InvalidHTableException(msg: String) extends Exception(msg)

  case class FetchTimeoutException(msg: String) extends NoStackException(msg)

  case class DropRequestException(msg: String) extends Exception(msg)

  case class FetchAllStepFailException(msg: String) extends Exception(msg)

  case class AccessDeniedException(amsg: String) extends Exception(amsg)
}
