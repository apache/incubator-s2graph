package org.apache.s2graph.counter.loader.stream

import kafka.serializer.StringDecoder
import org.apache.s2graph.counter.config.S2CounterConfig
import org.apache.s2graph.counter.loader.config.StreamingConfig
import org.apache.s2graph.counter.loader.core.CounterFunctions
import org.apache.s2graph.spark.config.S2ConfigFactory
import org.apache.s2graph.spark.spark.{WithKafka, SparkApp, HashMapParam}
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.KafkaRDDFunctions.rddToKafkaRDDFunctions
import org.apache.spark.streaming.kafka.{HasOffsetRanges, StreamHelper}
import scala.collection.mutable.{HashMap => MutableHashMap}

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
