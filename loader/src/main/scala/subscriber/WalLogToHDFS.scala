package subscriber

import java.text.SimpleDateFormat
import java.util.Date

import com.daumkakao.s2graph.core.Graph
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, StreamHelper}
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.language.postfixOps

object WalLogToHDFS extends SparkApp with WithKafka {
  private def toOutputPath(ts: Long): String = {
    val formatter = new SimpleDateFormat("yyyy/MM/dd/HH")
    formatter.format(new Date(ts))
  }
  val usages =
    s"""
       |/**
       |this job consume edges/vertices from kafka topic then load them into s2graph.
       |params:
       |  1. kafkaZkQuorum: kafka zk address to consume events
       |  2. brokerList: kafka cluster`s broker list.
       |  3. topics: , delimited list of topics to consume
       |  4. intervalInSec: batch  interval for this job.
       |  5. dbUrl:
       |  6. labelMapping: oldLabel:newLabel delimited by ,
       |  7. outputPath:
       |*/
   """.stripMargin
  override def run() = {
    if (args.length != 7) {
      System.err.println(usages)
      System.exit(1)
    }
    val kafkaZkQuorum = args(0)
    val brokerList = args(1)
    val topics = args(2)
    val intervalInSec = seconds(args(3).toLong)
    val dbUrl = args(4)
    val labelMapping = GraphSubscriberHelper.toLabelMapping(args(5))
    val outputPath = args(6)


    val conf = sparkConf(s"$topics: WalLogToHDFS")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val groupId = topics.replaceAll(",", "_") + "_stream"
    val fallbackTopic = topics.replaceAll(",", "_") + "_stream_failed"

    val kafkaParams = Map(
      "zookeeper.connect" -> kafkaZkQuorum,
      "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000",
      "metadata.broker.list" -> brokerList,
      "auto.offset.reset" -> "largest")

    val streamHelper = StreamHelper(kafkaParams)
    val stream = streamHelper.createStream[String, String, StringDecoder, StringDecoder](ssc, topics.split(",").toSet)
//    val stream = createKafkaValueStreamMulti(ssc, kafkaParams, topics, 8, None).flatMap(s => s.split("\n"))

    val mapAcc = sc.accumulable(new MutableHashMap[String, Long](), "Throughput")(HashMapParam[String, Long](_ + _))


    stream.foreachRDD((rdd, time) => {
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val elements = rdd.mapPartitions { partition =>
        // set executor setting.
        val phase = System.getProperty("phase")
        GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")

        for {
          (key, msg) <- partition
          element <- Graph.toGraphElement(msg, labelMapping)
        } yield {
          Seq(msg, element.serviceName).mkString("\t")
        }

      }

      val ts = time.milliseconds


      val path = s"$outputPath/${toOutputPath(ts)}/"

      elements.saveAsTextFile(path)

      rdd.mapPartitionsWithIndex { case (i, part) =>
        // commit offset range
        val osr = offsets(i)
        streamHelper.commitConsumerOffset(osr)
        Iterator.empty
      }.foreach {
        (_: Nothing) => ()
      }
    })


    logInfo(s"counter: ${mapAcc.value}")
    println(s"counter: ${mapAcc.value}")
    ssc.start()
    ssc.awaitTermination()
  }
}
