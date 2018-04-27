/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core

import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.producer._
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.JsValue

class ExceptionHandler(config: Config) {


  import ExceptionHandler._

  val keyBrokerList = "kafka.metadata.broker.list"
  val phase = if (config.hasPath("phase")) config.getString("phase") else "dev"
  val useKafka = config.hasPath(keyBrokerList) && config.getString(keyBrokerList) != "localhost"

  val producer: Option[KafkaProducer[Key, Val]] =
    if (useKafka) {
      try {
        Option(new KafkaProducer[Key, Val](toKafkaProp(config)))
      } catch {
        case e: Exception =>
          logger.error(s"Initialize kafka fail with: ${toKafkaProp(config)}")
          None
      }
    } else None

  def enqueue(m: KafkaMessage): Unit = {
    producer match {
      case None => logger.debug(s"skip log to Kafka: ${m}")
      case Some(kafka) =>
        kafka.send(m.msg, new Callback() {
          override def onCompletion(meta: RecordMetadata, e: Exception) = {
            if (e == null) {
              // success
            } else {
              logger.error(s"log publish failed: ${m}", e)
              // failure
            }
          }
        })
    }
  }

  def shutdown() = producer.foreach(_.close)
}

object ExceptionHandler {
  val mainBrokerKey = "kafka.metadata.broker.list"
  val subBrokerKey = "kafka_sub.metadata.broker.list"

  type Key = String
  type Val = String

  def toKafkaMessage(topic: String,
                     element: GraphElement,
                     originalString: Option[String] = None,
                     produceJson: Boolean = false) = {
    val edgeString = originalString.getOrElse(element.toLogString())
    val msg = edgeString

    KafkaMessage(
      new ProducerRecord[Key, Val](
        topic,
        element.queuePartitionKey,
        msg))
  }

  // only used in deleteAll
  def toKafkaMessage(topic: String, tsv: String) = {
    KafkaMessage(new ProducerRecord[Key, Val](topic, null, tsv))
  }

  def toKafkaMessage(topic: String, jsValue: JsValue): KafkaMessage = toKafkaMessage(topic, jsValue.toString())

  case class KafkaMessage(msg: ProducerRecord[Key, Val])

  def toKafkaProducer(config: Config): Option[KafkaProducer[String, String]] = {
    val brokerKey = "kafka.producer.brokers"
    if (config.hasPath(brokerKey)) {
      val brokers = config.getString("kafka.producer.brokers")
      Option(new KafkaProducer[String, String](toKafkaProp(brokers)))
    } else {
      None
    }
  }

  def toKafkaProp(config: Config): Properties = {
    /* all default configuration for new producer */
    val brokers =
      if (config.hasPath("kafka.metadata.broker.list")) config.getString("kafka.metadata.broker.list")
      else "localhost"

    toKafkaProp(brokers)
  }

  /*
   * http://kafka.apache.org/082/documentation.html#producerconfigs
   * if we change our kafka version, make sure right configuration is set.
   */
  def toKafkaProp(brokers: String): Properties = {
    val props = new Properties()

    props.put("bootstrap.servers", brokers)
    props.put("acks", "1")
    props.put("buffer.memory", "33554432")
    props.put("compression.type", "snappy")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "100")
    props.put("max.request.size", "1048576")
    props.put("receive.buffer.bytes", "32768")
    props.put("send.buffer.bytes", "131072")
    props.put("timeout.ms", "30000")
    props.put("block.on.buffer.full", "false")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props
  }

}
