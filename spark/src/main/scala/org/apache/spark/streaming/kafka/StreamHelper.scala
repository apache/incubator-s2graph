package org.apache.spark.streaming.kafka

import kafka.KafkaHelper
import kafka.common.TopicAndPartition
import kafka.consumer.PartitionTopicInfo
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{Logging, SparkException}

import scala.reflect.ClassTag

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 4. 22..
 */
case class StreamHelper(kafkaParams: Map[String, String]) extends Logging {
  // helper for kafka zookeeper
  lazy val kafkaHelper = KafkaHelper(kafkaParams)
  lazy val kc = new KafkaCluster(kafkaParams)

  // 1. get leader's earliest and latest offset
  // 2. get consumer offset
  // 3-1. if (2) is bounded in (1) use (2) for stream
  // 3-2. else use (1) by "auto.offset.reset"
  private def getStartOffsets(topics: Set[String]): Map[TopicAndPartition, Long] = {
    lazy val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    lazy val consumerOffsets = kafkaHelper.getConsumerOffsets(topics.toSeq)

    {
      for {
        topicPartitions <- kc.getPartitions(topics).right
        smallOffsets <- kc.getEarliestLeaderOffsets(topicPartitions).right
        largeOffsets <- kc.getLatestLeaderOffsets(topicPartitions).right
      } yield {
        {
          for {
            tp <- topicPartitions
          } yield {
            val co = consumerOffsets.getOrElse(tp, PartitionTopicInfo.InvalidOffset)
            val so = smallOffsets.get(tp).map(_.offset).get
            val lo = largeOffsets.get(tp).map(_.offset).get

            logWarning(s"$tp: $co $so $lo")

            if (co >= so && co <= lo) {
              (tp, co)
            } else {
              (tp, reset match {
                case Some("smallest") => so
                case _ => lo
              })
            }
          }
        }.toMap
      }
    }.fold(errs => throw new SparkException(errs.mkString("\n")), ok => ok)
  }

  def createStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag](ssc: StreamingContext, topics: Set[String]): InputDStream[(K, V)] = {
    type R = (K, V)
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key(), mmd.message())

    kafkaHelper.registerConsumerInZK(topics)

    new DirectKafkaInputDStream[K, V, KD, VD, R](ssc, kafkaParams, getStartOffsets(topics), messageHandler)
  }

  def commitConsumerOffsets(offsets: HasOffsetRanges): Unit = {
    val offsetsMap = {
      for {
        range <- offsets.offsetRanges if range.fromOffset < range.untilOffset
      } yield {
        logDebug(range.toString())
        TopicAndPartition(range.topic, range.partition) -> range.untilOffset
      }
    }.toMap

    kafkaHelper.commitConsumerOffsets(offsetsMap)
  }

  def commitConsumerOffset(range: OffsetRange): Unit = {
    if (range.fromOffset < range.untilOffset) {
      try {
        val tp = TopicAndPartition(range.topic, range.partition)
        logDebug("Committed offset " + range.untilOffset + " for topic " + tp)
        kafkaHelper.commitConsumerOffset(tp, range.untilOffset)
      } catch {
        case t: Throwable =>
          // log it and let it go
          logWarning("exception during commitOffsets",  t)
          throw t
      }
    }
  }

  def commitConsumerOffsets[R](stream: InputDStream[R]): Unit = {
    stream.foreachRDD { rdd =>
      commitConsumerOffsets(rdd.asInstanceOf[HasOffsetRanges])
    }
  }
}
