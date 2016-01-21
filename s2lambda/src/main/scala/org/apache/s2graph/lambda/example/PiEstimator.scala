package org.apache.s2graph.lambda.example

import org.apache.s2graph.lambda.{BaseDataProcessor, Data}

case class PiData(pi: Double) extends Data

class PiEstimator extends BaseDataProcessor[Tuple2RandomNumberData, PiData] {

  override protected def processBlock(input: Tuple2RandomNumberData): PiData = {

    val count = input.rdd.map { case (x, y) =>
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)

    val pi = 4.0 * count / input.num

    show("Pi is roughly " + pi)

    PiData(pi)
  }

}
