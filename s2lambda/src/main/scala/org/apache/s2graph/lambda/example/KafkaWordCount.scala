package org.apache.s2graph.lambda.example

import org.apache.s2graph.lambda.source.KafkaInput
import org.apache.s2graph.lambda.{BaseDataProcessor, Data}
import org.apache.spark.rdd.RDD

case class KafkaWordCountData(counts: RDD[(String, Int)]) extends Data

class KafkaWordCount extends BaseDataProcessor[KafkaInput, WordCountData] {

  override protected def processBlock(input: KafkaInput): WordCountData = {
    val counts = input.rdd.map(_._2)
        .flatMap(_.split("\\s+").filter(_.nonEmpty))
        .map(word => (word, 1))
        .reduceByKey(_ + _)

    WordCountData(counts)
  }

}
