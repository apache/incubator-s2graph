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

package s2.counter.stream

import com.kakao.s2graph.core.GraphUtil
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.KafkaRDDFunctions.rddToKafkaRDDFunctions
import s2.config.{S2ConfigFactory, S2CounterConfig, StreamingConfig}
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * can be @deprecated
 * Created by hsleep(honeysleep@gmail.com) on 15. 3. 16..
 */
object GraphToETLStreaming extends SparkApp with WithKafka {
  lazy val config = S2ConfigFactory.config
  lazy val s2Config = new S2CounterConfig(config)
  lazy val className = getClass.getName.stripSuffix("$")
  lazy val producer = getProducer[String, String](StreamingConfig.KAFKA_BROKERS)

  override def run(): Unit = {
    validateArgument("interval", "topic")
    val (intervalInSec, topic) = (seconds(args(0).toLong), args(1))

    val groupId = buildKafkaGroupId(topic, "graph_to_etl")
    val kafkaParam = Map(
//      "auto.offset.reset" -> "smallest",
      "group.id" -> groupId,
      "metadata.broker.list" -> StreamingConfig.KAFKA_BROKERS,
      "zookeeper.connect" -> StreamingConfig.KAFKA_ZOOKEEPER,
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    val conf = sparkConf(s"$topic: $className")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val acc = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    /**
     * consume graphIn topic and produce messages to etl topic
     * two purpose
     * 1. partition by target vertex id
     * 2. expand kafka partition count
     */
    val stream = getStreamHelper(kafkaParam).createStream[String, String, StringDecoder, StringDecoder](ssc, topic.split(',').toSet)
    stream.foreachRDD { rdd =>
      rdd.foreachPartitionWithOffsetRange { case (osr, part) =>
        val m = MutableHashMap.empty[Int, mutable.MutableList[String]]
        for {
          (k, v) <- part
          line <- GraphUtil.parseString(v)
        } {
          try {
            val sp = GraphUtil.split(line)
            // get partition key by target vertex id
            val partKey = getPartKey(sp(4), 20)
            val values = m.getOrElse(partKey, mutable.MutableList.empty[String])
            values += line
            m.update(partKey, values)
          } catch {
            case ex: Throwable =>
              log.error(s"$ex: $line")
          }
        }

        m.foreach { case (k, v) =>
          v.grouped(1000).foreach { grouped =>
            producer.send(new KeyedMessage[String, String](StreamingConfig.KAFKA_TOPIC_ETL, null, k, grouped.mkString("\n")))
          }
        }

        getStreamHelper(kafkaParam).commitConsumerOffset(osr)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
