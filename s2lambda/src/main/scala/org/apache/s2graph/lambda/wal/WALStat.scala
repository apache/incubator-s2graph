package org.apache.s2graph.lambda.wal

import java.util.Properties

import com.kakao.s2graph.core.Graph
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.s2graph.lambda.source.KafkaInput
import org.apache.s2graph.lambda.{BaseDataProcessor, Context, Data, EmptyData, Params}

case class WALStatParams(phase: String, dbUrl: String, brokerList: String, topic: String) extends Params

class WALStat(params: WALStatParams) extends BaseDataProcessor[KafkaInput, EmptyData](params) {

  override protected def processBlock(input: KafkaInput, context: Context): EmptyData = {
    val phase = params.phase
    val dbUrl = params.dbUrl
    val brokerList = params.brokerList
    val topic = params.topic
    val ts = input.time.milliseconds
    val statProducer = WALStat.getProducer[String, String](brokerList)

    val elements = input.rdd.mapPartitions { partition =>
      GraphSubscriberHelper.apply(phase, dbUrl, "none", brokerList)

      partition.map { case (key, msg) =>
        Graph.toGraphElement(msg) match {
          case Some(elem) =>
            val serviceName = elem.serviceName
            msg.split("\t", 7) match {
              case Array(_, operation, log_type, _, _, label, _*) =>
                Seq(serviceName, label, operation, log_type).mkString("\t")
              case _ =>
                Seq("no_service_name", "no_label", "no_operation", "parsing_error").mkString("\t")
            }
          case None =>
            Seq("no_service_name", "no_label", "no_operation", "no_element_error").mkString("\t")
        }
      }
    }

    val countByKey = elements.map(_ -> 1L).reduceByKey(_ + _).collect()
    val totalCount = countByKey.map(_._2).sum
    val keyedMessage = countByKey.map { case (key, value) =>
      new KeyedMessage[String, String](topic, s"$ts\t$key\t$value\t$totalCount")
    }

    statProducer.send(keyedMessage: _*)

    Data.emptyData
  }
}

object WALStat {

  def producerConfig(brokerList: String, requireAcks: String = "1", producerType: String = "sync") = {
    val props = new Properties()
    props.setProperty("metadata.broker.list", brokerList)
    props.setProperty("request.required.acks", requireAcks)
    props.setProperty("producer.type", producerType)
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder")
    props.setProperty("compression.codec", "snappy")
    props.setProperty("message.send.max.retries", "1")
    new ProducerConfig(props)
  }

  def getProducer[K, V](config: ProducerConfig): Producer[K, V] = {
    new Producer[K, V](config)
  }

  def getProducer[K, V](brokers: String): Producer[K, V] = {
    getProducer(producerConfig(brokers))
  }

}
