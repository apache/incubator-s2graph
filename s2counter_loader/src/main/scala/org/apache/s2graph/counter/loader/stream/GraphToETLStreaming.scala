package org.apache.s2graph.counter.loader.stream

import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.s2graph.core.GraphUtil
import org.apache.s2graph.counter.config.S2CounterConfig
import org.apache.s2graph.counter.loader.config.StreamingConfig
import org.apache.s2graph.spark.config.S2ConfigFactory
import org.apache.s2graph.spark.spark.{WithKafka, SparkApp, HashMapParam}
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.KafkaRDDFunctions.rddToKafkaRDDFunctions
import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}

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
