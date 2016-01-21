package org.apache.s2graph.lambda.source

import org.apache.s2graph.lambda.{BaseDataProcessor, Data, EmptyData, Params}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext, Time}

import scala.reflect.ClassTag

case class StreamFrontEndData[T: ClassTag](rdd: RDD[T], time: Time) extends Data

class StreamFrontEnd[T: ClassTag](params: Params, parent: String) extends BaseDataProcessor[EmptyData, StreamFrontEndData[T]](params) {

  var rdd: RDD[T] = null
  var time: Time = null

  override protected def processBlock(input: EmptyData): StreamFrontEndData[T] = {
    StreamFrontEndData(rdd, time)
  }

  def setRDD(rdd: RDD[T], time: Time): Unit = {
    this.rdd = rdd
    this.time = time
  }
}

trait StreamContainerParams extends Params {
  val interval: Long
  val timeout: Option[Long] // testing purpose
}

abstract class StreamContainer[T: ClassTag](params: StreamContainerParams)
    extends Source[EmptyData](params) {

  final def getParams: StreamContainerParams = params

  final val frontEnd: StreamFrontEnd[T] = new StreamFrontEnd[T](params, getClass.getSimpleName)

  final lazy val streamingContext: StreamingContext = new StreamingContext(context.sparkContext, Durations.seconds(params.interval))

  final def foreachBatch(foreachFunc: => Unit): Unit = {
    stream.foreachRDD { (rdd, time) =>
      frontEnd.setRDD(rdd, time)
      val rddAfterProcess = { foreachFunc; rdd }
      postProcess(rddAfterProcess, time)
    }
  }

  final override protected def processBlock(input: EmptyData): EmptyData = throw new IllegalAccessError

  protected val stream: DStream[T]

  def postProcess(rdd: RDD[T], time: Time): Unit = {}
}
