package com.kakao.ml.example

import com.kakao.ml.{BaseDataProcessor, Data}
import org.apache.spark.sql.SQLContext

case class PiData(pi: Double) extends Data

class PiEstimator extends BaseDataProcessor[Tuple2RandomNumberData, PiData] {

  override protected def processBlock(sqlContext: SQLContext, input: Tuple2RandomNumberData): PiData = {

    val count = input.rdd.map { case (x, y) =>
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)

    val pi = 4.0 * count / input.num

    logInfo("Pi is roughly " + pi)

    PiData(pi)
  }

}
