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

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.KafkaRDDFunctions.rddToKafkaRDDFunctions
import org.apache.spark.streaming.kafka.{HasOffsetRanges, StreamHelper}
import s2.config.{S2ConfigFactory, S2CounterConfig, StreamingConfig}
import s2.counter.core.CounterFunctions
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 19..
 */
object RankingCounterStreaming extends SparkApp with WithKafka {
  lazy val config = S2ConfigFactory.config
  lazy val s2Config = new S2CounterConfig(config)
  lazy val className = getClass.getName.stripSuffix("$")

  lazy val producer = getProducer[String, String](StreamingConfig.KAFKA_BROKERS)

  val inputTopics = Set(StreamingConfig.KAFKA_TOPIC_COUNTER_TRX)
  val strInputTopics = inputTopics.mkString(",")
  val groupId = buildKafkaGroupId(strInputTopics, "ranking_v2")
  val kafkaParam = Map(
//    "auto.offset.reset" -> "smallest",
    "group.id" -> groupId,
    "metadata.broker.list" -> StreamingConfig.KAFKA_BROKERS,
    "zookeeper.connect" -> StreamingConfig.KAFKA_ZOOKEEPER,
    "zookeeper.connection.timeout.ms" -> "10000"
  )
  val streamHelper = StreamHelper(kafkaParam)

  override def run() = {
    validateArgument("interval", "clear")
    val (intervalInSec, clear) = (seconds(args(0).toLong), args(1).toBoolean)

    if (clear) {
      streamHelper.kafkaHelper.consumerGroupCleanup()
    }

    val conf = sparkConf(s"$strInputTopics: $className")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    implicit val acc: HashMapAccumulable = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    // make stream
    val stream = streamHelper.createStream[String, String, StringDecoder, StringDecoder](ssc, inputTopics)
    stream.foreachRDD { (rdd, ts) =>
      // for at-least once semantic
      val nextRdd = {
        CounterFunctions.makeRankingRdd(rdd, sc.defaultParallelism).foreachPartition { part =>
          // update ranking counter
          CounterFunctions.updateRankingCounter(part, acc)
        }
        rdd
      }

      streamHelper.commitConsumerOffsets(nextRdd.asInstanceOf[HasOffsetRanges])
//      CounterFunctions.makeRankingRdd(rdd, offsets.length).foreachPartitionWithIndex { (i, part) =>
//        // update ranking counter
//        CounterFunctions.updateRankingCounter(part, acc)
//
//        // commit offset range
//        streamHelper.commitConsumerOffset(offsets(i))
//      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
