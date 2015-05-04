package s2.spark

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{Accumulable, Logging, SparkConf}

import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * Created by alec.k on 14. 12. 26..
 */
trait SparkApp extends Logging {
  type HashMapAccumulable = Accumulable[MutableHashMap[String, Int], (String, Int)]
  
  protected def args: Array[String] = _args

  private var _args: Array[String] = _

  // 상속받은 클래스에서 구현해줘야 하는 함수
  def run()

  def getArgs(index: Int) = args(index)

  // TODO: 공통 루틴을 더 만들어 보자.
  def main(args: Array[String]) {
    _args = args
    run()
  }

  def validateArgument(argNames: String*): Unit = {
    if (args == null || args.length < argNames.length) {
      System.err.println(s"Usage: ${getClass.getName} " + argNames.map(s => s"<$s>").mkString(" "))
      System.exit(1)
    }
  }

  def buildKafkaGroupId(topic: String, ext: String): String = {
    val phase = System.getProperty("phase")

    var groupId = s"${topic}_$ext"

    groupId += {
      System.getProperty("spark.master") match {
        case x if x.startsWith("local") => "_local"
        case _ => ""
      }
    }

    groupId += {
      phase match {
        case "alpha" => "_alpha"
        case _ => ""
    }}

    groupId
  }

  def sparkConf(jobName: String): SparkConf = {
    val conf = new SparkConf()
    conf.setAppName(jobName)
    // TODO: 런처를 만들고 공통된 configure를 넣어주는 방시으로 변경하자.
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.streaming.unpersist", "true")
    conf
  }

  def streamingContext(sparkConf: SparkConf, interval: Duration, checkPoint: Option[String] = None) = {
    val ssc = new StreamingContext(sparkConf, interval)
    checkPoint.foreach { dir =>
      ssc.checkpoint(dir)
    }

    // for watch tower
    ssc.addStreamingListener(new SubscriberListener(ssc))

    ssc
  }

  def createKafkaPairStream(ssc: StreamingContext, kafkaParam: Map[String, String], topics: String, numPartition: Option[Int] = None): DStream[(String, String)] = {
    val topicMap = topics.split(",").map((_, 1)).toMap
    /**
     * stream에서 만들어지는 partition 갯수는 batchInterval / spark.streaming.blockInterval 이다.
     * stream.map 함수를 호출한 후에 repartition을 호출하면, 전체 처리 시간이 오래 걸리는 점을 발견.
     * worker 갯수에 맞춰 repartition 해주고, map 함수를 호출하도록 수정.
     */
    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
    numPartition.map(n =>
      stream.repartition(n)
    ).getOrElse(stream)
  }

  def createKafkaValueStream(ssc: StreamingContext, kafkaParam: Map[String, String], topics: String, numPartition: Option[Int] = None): DStream[String] = {
    createKafkaPairStream(ssc, kafkaParam, topics, numPartition).map(_._2)
  }

  def createKafkaPairStreamMulti(ssc: StreamingContext, kafkaParam: Map[String, String], topics: String, receiverCount: Int, numPartition: Option[Int] = None): DStream[(String, String)] = {
    // receiver를 위한 executor가 다 뜬 후에 실제 receiver를 만들기 위한 code
    Stream.continually(ssc.sparkContext.getExecutorStorageStatus).takeWhile(_.length < receiverCount).foreach { arr =>
      Thread.sleep(100)
    }
    Thread.sleep(1000)

    val topicMap = topics.split(",").map((_, 1)).toMap

    val stream = {
      val streams = {
        (1 to receiverCount) map { _ =>
          KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
        }
      }
      ssc.union(streams)
    }
    numPartition.map(n =>
      stream.repartition(n)
    ).getOrElse(stream)
  }

  def createKafkaValueStreamMulti(ssc: StreamingContext, kafkaParam: Map[String, String], topics: String, receiverCount: Int, numPartition: Option[Int] = None): DStream[String] = {
    createKafkaPairStreamMulti(ssc, kafkaParam, topics , receiverCount, numPartition).map(_._2)
  }
}
