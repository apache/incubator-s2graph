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

    val hdfsBlockSize = 134217728 // 128M
    val hiveContext = new HiveContext(sc)
    var splits = Array[String]()
    var excludeLabels = Set[String]()
    var excludeServices = Set[String]()
    stream.foreachRDD { (rdd, time) =>
      try {
        val read = sc.textFile(splitListPath).collect().map(_.split("=")).flatMap {
          case Array(value) => Some(("split", value))
          case Array(key, value) => Some((key, value))
          case _ => None
        }
        splits = read.filter(_._1 == "split").map(_._2)
        excludeLabels = read.filter(_._1 == "exclude_label").map(_._2).toSet
        excludeServices = read.filter(_._1 == "exclude_service").map(_._2).toSet
      } catch {
        case _: Throwable => // use previous information
      }

      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val elements = rdd.mapPartitions { partition =>
        // set executor setting.
        val phase = System.getProperty("phase")
        GraphSubscriberHelper.apply(phase, dbUrl, "none", brokerList)

        partition.flatMap { case (key, msg) =>
          val optMsg = Graph.toGraphElement(msg).flatMap { element =>
            val arr = msg.split("\t", 7)
            val service = element.serviceName
            val label = arr(5)
            val n = arr.length

            if (excludeServices.contains(service) || excludeLabels.contains(label)) {
              None
            } else if(n == 6) {
              Some(Seq(msg, "{}", service).mkString("\t"))
            }
            else if(n == 7) {
              Some(Seq(msg, service).mkString("\t"))
            }
            else {
              None
            }
          }
          optMsg
        }
      }

      val ts = time.milliseconds
      val dateId = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))

      /** make sure that `elements` are not running at the same time */
      val elementsWritten = {
        elements.cache()
        (Array("all") ++ splits).foreach {
          case split if split == "all" =>
            val path = s"$outputPath/split=$split/date_id=$dateId/ts=$ts"
            elements.saveAsTextFile(path)
          case split =>
            val path = s"$outputPath/split=$split/date_id=$dateId/ts=$ts"
            val strlen = split.length
            val splitData = elements.filter(_.takeRight(strlen) == split).cache()
            val totalSize = splitData
                .mapPartitions { iterator =>
                  val s = iterator.map(_.length.toLong).sum
                  Iterator.single(s)
                }
                .sum
                .toLong
            val numPartitions = math.max(1, (totalSize / hdfsBlockSize.toDouble).toInt)
            splitData.coalesce(math.min(splitData.partitions.length, numPartitions)).saveAsTextFile(path)
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

      (Array("all") ++ splits).foreach { split =>
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
