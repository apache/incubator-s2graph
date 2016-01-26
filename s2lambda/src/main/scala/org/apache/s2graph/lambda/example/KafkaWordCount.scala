package org.apache.s2graph.lambda.example

import org.apache.s2graph.lambda.source.KafkaInput
import org.apache.s2graph.lambda.{BaseDataProcessor, Context, Data}
import org.apache.spark.rdd.RDD

case class KafkaWordCountData(counts: RDD[(String, Int)]) extends Data

class KafkaWordCount extends BaseDataProcessor[KafkaInput, WordCountData] {

  override protected def processBlock(input: KafkaInput, context: Context): WordCountData = {
    val counts = input.rdd.map(_._2)
        .flatMap(_.split("\\s+").filter(_.nonEmpty))
        .map(word => (word, 1))
        .reduceByKey(_ + _)

    WordCountData(counts)
  }

}
