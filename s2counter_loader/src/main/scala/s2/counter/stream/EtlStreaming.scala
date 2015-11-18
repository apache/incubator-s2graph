package s2.counter.stream

import com.kakao.s2graph.core.{Graph, GraphUtil}
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.KafkaRDDFunctions.rddToKafkaRDDFunctions
import org.apache.spark.streaming.kafka.StreamHelper
import s2.config.{S2ConfigFactory, S2CounterConfig, StreamingConfig}
import s2.counter.core.{CounterEtlFunctions, CounterEtlItem, DimensionProps}
import s2.models.{CounterModel, DBModel}
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.concurrent.ExecutionContext

/**
  * Created by hsleep(honeysleep@gmail.com) on 15. 10. 6..
  */
object EtlStreaming extends SparkApp with WithKafka {
  lazy val config = S2ConfigFactory.config
  lazy val s2Config = new S2CounterConfig(config)
  lazy val counterModel = new CounterModel(config)
  lazy val className = getClass.getName.stripSuffix("$")
  lazy val producer = getProducer[String, String](StreamingConfig.KAFKA_BROKERS)

  implicit val graphEx = ExecutionContext.Implicits.global

  val initialize = {
    println("streaming initialize")
//    Graph(config)
    DBModel.initialize(config)
    true
  }

  val inputTopics = Set(StreamingConfig.KAFKA_TOPIC_ETL)
  val strInputTopics = inputTopics.mkString(",")
  val groupId = buildKafkaGroupId(strInputTopics, "etl_to_counter")
  val kafkaParam = Map(
    "group.id" -> groupId,
    "metadata.broker.list" -> StreamingConfig.KAFKA_BROKERS,
    "zookeeper.connect" -> StreamingConfig.KAFKA_ZOOKEEPER,
    "zookeeper.connection.timeout.ms" -> "10000"
  )
  val streamHelper = StreamHelper(kafkaParam)

  override def run(): Unit = {
    validateArgument("interval")
    val (intervalInSec) = seconds(args(0).toLong)

    val conf = sparkConf(s"$strInputTopics: $className")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val acc = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    /**
     * read message from etl topic and join user profile from graph and then produce whole message to counter topic
     */
    val stream = streamHelper.createStream[String, String, StringDecoder, StringDecoder](ssc, inputTopics)

    // etl logic
    stream.foreachRDD { (rdd, ts) =>
      rdd.foreachPartitionWithOffsetRange { case (osr, part) =>
        assert(initialize)

        // convert to edge format
        val items = {
          for {
            (k, v) <- part
            line <- GraphUtil.parseString(v)
            item <- CounterEtlFunctions.parseEdgeFormat(line)
          } yield {
            acc += ("Edges", 1)
            item
          }
        }

        // join user profile
        val joinItems = items.toList.groupBy { e =>
          (e.service, e.action)
        }.flatMap { case ((service, action), v) =>
          CounterEtlFunctions.checkPolicyAndMergeDimension(service, action, v)
        }

        // group by kafka partition key and send to kafka
        val m = MutableHashMap.empty[Int, mutable.MutableList[CounterEtlItem]]
        joinItems.foreach { item =>
          if (item.useProfile) {
            acc += ("ETL", 1)
          }
          val k = getPartKey(item.item, 20)
          val values: mutable.MutableList[CounterEtlItem] = m.getOrElse(k, mutable.MutableList.empty[CounterEtlItem])
          values += item
          m.update(k, values)
        }
        m.foreach { case (k, v) =>
          v.map(_.toKafkaMessage).grouped(1000).foreach { grouped =>
            acc += ("Produce", grouped.size)
            producer.send(new KeyedMessage[String, String](StreamingConfig.KAFKA_TOPIC_COUNTER, null, k, grouped.mkString("\n")))
          }
        }

        streamHelper.commitConsumerOffset(osr)
      }

      if (ts.milliseconds / 1000 % 60 == 0) {
        log.warn(DimensionProps.getCacheStatsString)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
