package org.apache.s2graph.lambda.source

import org.apache.spark.streaming.dstream.DStream

case class FileStreamContainerParams(interval: Long, path: String, timeout: Option[Long]) extends StreamContainerParams

class FileStreamContainer(params: FileStreamContainerParams) extends StreamContainer[String](params) {

  override protected lazy val stream: DStream[String] = streamingContext.textFileStream(params.path)
}

