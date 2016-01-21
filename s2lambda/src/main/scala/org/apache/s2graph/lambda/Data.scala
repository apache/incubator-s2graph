package org.apache.s2graph.lambda

/**
  * Data, for I/O between processors
  */
trait Data

case class EmptyData() extends Data

case class PredecessorData(asMap: Map[String, Any]) extends Data

object Data {

  val emptyData = EmptyData()

  val emptyPredecessorData = PredecessorData(Map.empty[String, Any])

}