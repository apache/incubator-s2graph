package subscriber
import org.apache.spark.streaming.Durations._
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.language.postfixOps

object GraphSubscriberStreaming extends SparkApp with WithKafka {
  override def run() = {
    if (args.length < 4) {
      System.err.println("Usage: GraphSubscriberStreaming <kafkaZkQuorum> <brokerList> <topics> <numOfWorkers> <interval> <batchSize>")
      System.exit(1)
    }
    val kafkaZkQuorum = args(0)
    val brokerList = args(1)
    val topics = args(2)
    val intervalInSec = seconds(args(1).toLong)
    val dbUrl = args(4)
    val batchSize = args(5).toInt

    val conf = sparkConf(s"$topics: GraphSubscriberStreaming")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val topicSet = topics.split(",").toSet
    val groupId = topics.replaceAll(",", "_") + "_stream"
    val fallbackTopic = topics.replaceAll(",", "_") + "_stream_failed"

    val kafkaParams = Map(
      "zookeeper.connect" -> kafkaZkQuorum,
      "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000",
      "metadata.broker.list" -> brokerList)

    //    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //      ssc, kafkaParams, topicSet).flatMap(kv => kv._2.split("\n"))
    val stream = createKafkaValueStreamMulti(ssc, kafkaParams, topics, 8, None).flatMap(s => s.split("\n"))

    val mapAcc = sc.accumulable(new MutableHashMap[String, Long](), "Throughput")(HashMapParam[String, Long](_ + _))
    stream.foreachRDD(rdd => {

      rdd.foreachPartition(partition => {
        GraphSubscriberHelper(System.getProperty("phase"), dbUrl, "none", brokerList)
        val iter = partition
        iter.grouped(batchSize).foreach { msgs =>
          try {
            val counts = GraphSubscriberHelper.store(msgs)(Some(mapAcc))
            for ((k, v) <- counts) {
              mapAcc += (k, v)
            }
          } catch {
            case e: Throwable =>
              println(e)
              println(s"[Exception]: $e")

              msgs.foreach { msg =>
                GraphSubscriberHelper.report(msg, Some(e.getMessage()), topic = fallbackTopic)
              }
          }
        }
      })
      logInfo(s"counter: ${mapAcc.value}")
    })
    logInfo(s"counter: ${mapAcc.value}")
    println(s"counter: ${mapAcc.value}")
    ssc.start()
    ssc.awaitTermination()
  }
}
