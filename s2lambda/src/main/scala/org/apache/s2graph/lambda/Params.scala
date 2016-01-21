package org.apache.s2graph.lambda

/**
  * Params, for Initializing a processor
  */
trait Params

case class EmptyParams() extends Params

case class EmptyOrNotGivenParams() extends Params()

object Params {

  val emptyParams = EmptyParams()

  val emptyOrNotGivenParams = EmptyOrNotGivenParams()

}