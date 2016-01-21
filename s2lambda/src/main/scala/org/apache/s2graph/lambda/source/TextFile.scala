package org.apache.s2graph.lambda.source

import org.apache.s2graph.lambda._
import org.apache.spark.rdd.RDD

case class TextFileParams(path: String) extends Params

case class TextFileData(rdd: RDD[String]) extends Data

class TextFile(params: TextFileParams) extends Source[TextFileData](params) {
  override protected def processBlock(input: EmptyData): TextFileData =
    TextFileData(context.sparkContext.textFile(params.path))
}
