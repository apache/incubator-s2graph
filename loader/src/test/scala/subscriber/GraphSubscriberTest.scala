package subscriber

import org.scalatest.{ FunSuite, Matchers }
import s2.spark.WithKafka
import kafka.javaapi.producer.Producer
import config.Config

class GraphSubscriberTest extends FunSuite with Matchers with WithKafka {
 
  test("kafka producer") { 
    val brokerList = Config.KAFKA_METADATA_BROKER_LIST
    println(brokerList)
    val producer = new Producer[String, String](kafkaConf(brokerList))
    println(producer)
  }
  
  test("GraphSubscriberHelper.store") {
    
  }
}