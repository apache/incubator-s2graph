package subscriber

import java.text.SimpleDateFormat
import java.util.Date

import com.kakao.s2graph.core.Graph
import kafka.serializer.StringDecoder
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.HasOffsetRanges
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.language.postfixOps

object WalLogToHDFS extends SparkApp with WithKafka {

  override def run() = {

    validateArgument("kafkaZkQuorum", "brokerList", "topics", "intervalInSec", "dbUrl", "outputPath", "hiveDatabase", "hiveTable", "splitListPath")

    val kafkaZkQuorum = args(0)
    val brokerList = args(1)
    val topics = args(2)
    val intervalInSec = seconds(args(3).toLong)
    val dbUrl = args(4)
    val outputPath = args(5)
    val hiveDatabase = args(6)
    val hiveTable = args(7)
    val splitListPath = args(8)

    val conf = sparkConf(s"$topics: WalLogToHDFS")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val groupId = topics.replaceAll(",", "_") + "_stream"
    val fallbackTopic = topics.replaceAll(",", "_") + "_stream_failed"

    val kafkaParams = Map(
      "zookeeper.connect" -> kafkaZkQuorum,
      "group.id" -> groupId,
      "metadata.broker.list" -> brokerList,
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.offset.reset" -> "largest")

    val stream = getStreamHelper(kafkaParams).createStream[String, String, StringDecoder, StringDecoder](ssc, topics.split(",").toSet)

    val mapAcc = sc.accumulable(new MutableHashMap[String, Long](), "Throughput")(HashMapParam[String, Long](_ + _))

    var splits = Array("all")
    stream.foreachRDD { (rdd, time) =>
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val elements = rdd.mapPartitions { partition =>
        // set executor setting.
        val phase = System.getProperty("phase")
        GraphSubscriberHelper.apply(phase, dbUrl, "none", brokerList)

        partition.flatMap { case (key, msg) =>
          val optMsg = Graph.toGraphElement(msg).flatMap { element =>
            val n = msg.split("\t", 7).length
            if(n == 6) {
              Some(Seq(msg, "{}", element.serviceName).mkString("\t"))
            }
            else if(n == 7) {
              Some(Seq(msg, element.serviceName).mkString("\t"))
            }
            else {
              None
            }
          }
          optMsg
        }
      }

      try {
        // update splits
        val read = sc.textFile(splitListPath).collect().map(_.trim)
        if (read.length > 0) splits =  read
      } catch {
        case _: Throwable => // use previous splits
      }

      val ts = time.milliseconds
      val dateId = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))

      /** make sure that `elements` are not running at the same time */
      val elementsWritten = {
        elements.cache()
        splits.foreach {
          case split if split == "all" =>
            val path = s"$outputPath/split=$split/date_id=$dateId/ts=$ts"
            elements.saveAsTextFile(path)
          case split =>
            val path = s"$outputPath/split=$split/date_id=$dateId/ts=$ts"
            val strlen = split.length
            val splitData = elements.filter(_.takeRight(strlen) == split).cache()
            val numPartitions = math.max(1, (splitData.count() / 5e5).toInt)
            splitData.repartition(numPartitions).saveAsTextFile(path)
            splitData.unpersist()
        }
        elements.unpersist()
        elements
      }

      elementsWritten.mapPartitionsWithIndex { (i, part) =>
        // commit offset range
        val osr = offsets(i)
        getStreamHelper(kafkaParams).commitConsumerOffset(osr)
        Iterator.empty
      }.foreach {
        (_: Nothing) => ()
      }

      val hiveContext = new HiveContext(sc)
      splits.foreach { split =>
        val path = s"$outputPath/split=$split/date_id=$dateId/ts=$ts"
        hiveContext.sql(s"use $hiveDatabase")
        hiveContext.sql(s"alter table $hiveTable add partition (split='$split', date_id='$dateId', ts='$ts') location '$path'")
      }
    }

    logInfo(s"counter: ${mapAcc.value}")
    println(s"counter: ${mapAcc.value}")
    ssc.start()
    ssc.awaitTermination()
  }
}
