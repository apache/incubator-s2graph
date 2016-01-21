package org.apache.s2graph.lambda.source

import kafka.serializer.StringDecoder
import org.apache.s2graph.lambda.Data
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, StreamHelper}

case class KafkaInput(rdd: RDD[(String, String)], time: Time) extends Data

case class KafkaStreamContainerParams(
    interval: Long,
    kafkaZkQuorum: String,
    brokerList: String,
    topics: String,
    groupId: Option[String],
    zkTimeout: Option[Long],
    offset: Option[String],
    timeout: Option[Long]) extends StreamContainerParams

class KafkaStreamContainer(params: KafkaStreamContainerParams) extends StreamContainer[(String, String)](params) {

  val topics = params.topics.split(",").toSet
  val groupId = params.groupId.getOrElse(params.topics.replaceAll(",", "_") + "_stream")
  val zkTimeout = params.zkTimeout.map(_.toString).getOrElse("10000")
  val offset = params.offset.getOrElse("largest")

  val kafkaParams = Map(
    "zookeeper.connect" -> params.kafkaZkQuorum,
    "group.id" -> groupId,
    "metadata.broker.list" -> params.brokerList,
    "zookeeper.connection.timeout.ms" -> zkTimeout,
    "auto.offset.reset" -> offset)

  val streamHelper = new StreamHelper(kafkaParams)

  override protected lazy val stream: DStream[(String, String)] = {
    streamHelper.createStream[String, String, StringDecoder, StringDecoder](streamingContext, topics)
  }

  override def postProcess(rdd: RDD[(String, String)], time: Time): Unit = {
    val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsets.foreach { offset =>
      logger.info(s"commit $offset")
      streamHelper.commitConsumerOffset(offset)
    }
  }

}
