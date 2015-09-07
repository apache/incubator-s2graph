package com.daumkakao.s2graph.core

import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import akka.routing.{Broadcast, RoundRobinPool}

import com.typesafe.config.Config
import org.apache.kafka.clients.producer._
import scala.concurrent.duration._

/**
 * Created by shon on 7/16/15.
 */
object ExceptionHandler {

  var producer: Option[Producer[Key, Val]] = None
  var properties: Option[Properties] = None
  val numOfRoutees = 1
  val actorSystem = ActorSystem("ExceptionHandler")
  var routees: Option[ActorRef] = None
  var shutdownTime = 1000 millis
  var phase = "dev"
  lazy val failTopic = s"mutateFailed_${phase}"

  def apply(config: Config) = {
    properties =
      if (config.hasPath("kafka.metadata.broker.list")) Option(kafkaConfig(config))
      else None
    phase = if (config.hasPath("phase")) config.getString("phase") else "dev"
    producer = for {
      props <- properties
      p <- try { Option(new KafkaProducer[Key, Val](props)) } catch { case e: Throwable => None }
    } yield {
        p
      }
    init()
  }



  def props(producer: Producer[Key, Val]) = Props(classOf[KafkaAggregatorActor], producer)

  def init() = {
    for {
      p <- producer
    } {
      routees = Option(actorSystem.actorOf(RoundRobinPool(numOfRoutees).props(props(p))))
    }
  }

  def shutdown() = {
    routees.map ( _ ! Broadcast(PoisonPill) )
    Thread.sleep(shutdownTime.length)
  }

  def enqueues(msgs: Seq[KafkaMessage]) = {
    msgs.foreach(enqueue)
  }

  def enqueue(msg: KafkaMessage) = {
    routees.map ( _ ! msg )
  }


  def kafkaConfig(config: Config) = {
    val props = new Properties();

    /** all default configuration for new producer */
    val brokers =
      if (config.hasPath("kafka.metadata.broker.list")) config.getString("kafka.metadata.broker.list")
      else "localhost"
    props.put("bootstrap.servers", brokers)
    props.put("acks", "1")
    props.put("buffer.memory", "33554432")
    props.put("compression.type", "snappy")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "0")
    props.put("max.request.size", "1048576")
    props.put("receive.buffer.bytes", "32768")
    props.put("send.buffer.bytes", "131072")
    props.put("timeout.ms", "30000")
    props.put("block.on.buffer.full", "false")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  type Key = String
  type Val = String

  def toKafkaMessage(topic: String = failTopic, element: GraphElement, originalString: Option[String] = None) = {
    KafkaMessage(new ProducerRecord[Key, Val](topic, element.queuePartitionKey,
      originalString.getOrElse(element.toLogString())))
  }

  case class KafkaMessage(msg: ProducerRecord[Key, Val])

  case class Message(topic: String, msg: String)

  case class BufferedKafkaMessage(msgs: Seq[ProducerRecord[Key, Val]], bufferSize: Int)

  case class BufferedMessage(topic: String, bufferedMsgs: String, bufferSize: Int)

  case object FlushBuffer

  case class UpdateHealth(isHealty: Boolean)

  case object ShowMetrics

}

class KafkaAggregatorActor(kafkaProducer: Producer[String, String]) extends Stash with ActorLogging {

  import ExceptionHandler._

  val failedCount = new AtomicLong(0L)
  val successCount = new AtomicLong(0L)
  val stashCount = new AtomicLong(0L)

  implicit val ex = context.system.dispatcher

  context.system.scheduler.schedule(0 millis, 10 seconds) {
    self ! ShowMetrics
  }

  override def receive = {
    case ShowMetrics =>
      log.info(s"[Stats]: failed[${failedCount.get}], stashed[${stashCount.get}], success[${successCount.get}]")

    case m: KafkaMessage =>
      val replayTo = self
      try {
        kafkaProducer.send(m.msg, new Callback() {
          override def onCompletion(meta: RecordMetadata, e: Exception) = {
            if (e == null) {
              // success
              successCount.incrementAndGet()
              unstashAll()
              stashCount.set(0L)
            } else {
              // failure
              log.error(s"onCompletion: $e", e)
              failedCount.incrementAndGet()
              replayTo ! m
            }
          }
        })
      } catch {
        case e@(_: org.apache.kafka.clients.producer.BufferExhaustedException | _: Throwable) =>
          log.error(s"$e", e)
          log.info(s"stash")
          stash()
          stashCount.incrementAndGet()
      }

  }
}
