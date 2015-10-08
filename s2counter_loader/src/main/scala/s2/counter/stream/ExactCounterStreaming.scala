package s2.counter.stream

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, StreamHelper}
import s2.config.{S2ConfigFactory, S2CounterConfig, StreamingConfig}
import s2.counter.core.{BlobExactKey, CounterFunctions}
import s2.models.Counter.ItemType
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.language.postfixOps

/**
 * Streaming job for counter topic
 * Created by hsleep(honeysleep@gmail.com) on 15. 1. 15..
 */
object ExactCounterStreaming extends SparkApp with WithKafka {
  lazy val config = S2ConfigFactory.config
  lazy val s2Config = new S2CounterConfig(config)
  lazy val className = getClass.getName.stripSuffix("$")

  lazy val producer = getProducer[String, String](StreamingConfig.KAFKA_BROKERS)

  val inputTopics = Set(StreamingConfig.KAFKA_TOPIC_COUNTER)
  val strInputTopics = inputTopics.mkString(",")
  val groupId = buildKafkaGroupId(strInputTopics, "counter_v2")
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
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val exactRDD = CounterFunctions.makeExactRdd(rdd, offsets.length)

      // for at-least once semantic
      exactRDD.mapPartitionsWithIndex { case (i, part) =>
        val seq = part.toSeq
        // insert blob first
        CounterFunctions.insertBlobValue(seq.map(_._1).filter(_.itemType == ItemType.BLOB).map(_.asInstanceOf[BlobExactKey]), acc)
        // update exact counter
        val trxLogs = CounterFunctions.updateExactCounter(seq, acc)
        CounterFunctions.produceTrxLog(trxLogs)

        // commit offset range
        val osr = offsets(i)
        streamHelper.commitConsumerOffset(osr)
        Iterator.empty
      }.foreach {
        (_: Nothing) => ()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
