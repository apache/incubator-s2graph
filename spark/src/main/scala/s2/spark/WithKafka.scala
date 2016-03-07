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

package s2.spark

import java.util.Properties

import kafka.producer.{Producer, ProducerConfig}

trait WithKafka {
  def kafkaConf(brokerList: String) = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("request.required.acks", "0")
    props.put("producer.type", "async")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("compression.codec", "1")
    props.put("message.send.max.retries", "3")
    props.put("batch.num.messages", "1000")
    new ProducerConfig(props)
  }

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

  /**
   * Kafka DefaultPartitioner
   * @param k
   * @param n
   * @return
   */
  def getPartKey(k: Any, n: Int): Int = {
    kafka.utils.Utils.abs(k.hashCode()) % n
  }

  def makeKafkaGroupId(topic: String, ext: String): String = {
    val phase = System.getProperty("phase")

    var groupId = s"${topic}_$ext"

    groupId += {
      System.getProperty("spark.master") match {
        case x if x.startsWith("local") => "_local"
        case _ => ""
      }
    }

    groupId += {
      phase match {
        case "alpha" => "_alpha"
        case _ => ""
      }}

    groupId
  }
}
