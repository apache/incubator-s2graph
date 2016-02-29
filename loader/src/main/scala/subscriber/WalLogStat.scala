package subscriber

import java.text.SimpleDateFormat
import java.util.Date

import com.kakao.s2graph.core.Graph
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.HasOffsetRanges
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.language.postfixOps

object WalLogStat extends SparkApp with WithKafka {

  override def run() = {

    validateArgument("kafkaZkQuorum", "brokerList", "topics", "intervalInSec", "dbUrl", "statTopic")

    val kafkaZkQuorum = args(0)
    val brokerList = args(1)
    val topics = args(2)
    val intervalInSec = seconds(args(3).toLong)
    val dbUrl = args(4)
    val statTopic = args(5)


    val conf = sparkConf(s"$topics: ${getClass.getSimpleName}")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val groupId = topics.replaceAll(",", "_") + "_stat"

    val kafkaParams = Map(
      "zookeeper.connect" -> kafkaZkQuorum,
      "group.id" -> groupId,
      "metadata.broker.list" -> brokerList,
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.offset.reset" -> "largest")

    val stream = getStreamHelper(kafkaParams).createStream[String, String, StringDecoder, StringDecoder](ssc, topics.split(",").toSet)
    val statProducer = getProducer[String, String](brokerList)

    stream.foreachRDD { (rdd, time) =>

      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val ts = time.milliseconds

      val elements = rdd.mapPartitions { partition =>
        // set executor setting.
        val phase = System.getProperty("phase")
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
        new KeyedMessage[String, String](statTopic, s"$ts\t$key\t$value\t$totalCount")
      }

      statProducer.send(keyedMessage: _*)

      elements.mapPartitionsWithIndex { (i, part) =>
        // commit offset range
        val osr = offsets(i)
        getStreamHelper(kafkaParams).commitConsumerOffset(osr)
        Iterator.empty
      }.foreach {
        (_: Nothing) => ()
      }

    }

    ssc.start()
    ssc.awaitTermination()
  }
}
