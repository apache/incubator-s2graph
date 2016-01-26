package org.apache.s2graph.lambda.example

import org.apache.s2graph.lambda.source.TextFileData
import org.apache.s2graph.lambda.{BaseDataProcessor, Context, Data}
import org.apache.spark.rdd.RDD

case class WordCountData(counts: RDD[(String, Int)]) extends Data

class WordCount extends BaseDataProcessor[TextFileData, WordCountData] {

  override protected def processBlock(input: TextFileData, context: Context): WordCountData = {
    val counts = input.rdd
        .flatMap(_.split("\\s+").filter(_.nonEmpty))
        .map(word => (word, 1))
        .reduceByKey(_ + _)

    WordCountData(counts)
  }

}
