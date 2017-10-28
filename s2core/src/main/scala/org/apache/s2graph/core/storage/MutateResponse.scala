package org.apache.s2graph.core.storage

object MutateResponse {
  val Success = new MutateResponse(isSuccess = true)
  val Failure = new MutateResponse(isSuccess = false)
  val IncrementFailure = new IncrementResponse(isSuccess = false, -1, -1)
  val IncrementSuccess = new IncrementResponse(isSuccess = true, -1, -1)
}

class MutateResponse(val isSuccess: Boolean)

case class IncrementResponse(override val isSuccess: Boolean, afterCount: Long, beforeCount: Long) extends MutateResponse(isSuccess)
