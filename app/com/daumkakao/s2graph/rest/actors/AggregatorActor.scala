package com.daumkakao.s2graph.rest.actors

import com.daumkakao.s2graph.rest.config.Config
import com.daumkakao.s2graph.core.{ Edge, Graph, GraphElement, Vertex }
import java.util.Properties
import akka.actor.{ ActorRef, PoisonPill, Props, Stash }
import akka.routing.{ Broadcast, RoundRobinPool }
import play.api.Logger
import play.libs.Akka
import scala.concurrent.duration._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.atomic.AtomicLong

object Protocol {
  type Key = String
  type Val = String

  def elementToKafkaMessage(topic: String, element: GraphElement, originalString: Option[String]) = {
    KafkaMessage(new ProducerRecord[Key, Val](topic, element.queuePartitionKey, originalString.getOrElse(element.toString)))
  }
  case class KafkaMessage(msg: ProducerRecord[Key, Val])
  case class Message(topic: String, msg: String)
  private[actors] case class BufferedKafkaMessage(msgs: Seq[ProducerRecord[Key, Val]], bufferSize: Int)
  private[actors] case class BufferedMessage(topic: String, bufferedMsgs: String, bufferSize: Int)
  private[actors] case object FlushBuffer
  private[actors] case class UpdateHealth(isHealty: Boolean)
  private[actors] case object ShowMetrics
}
trait WithProducer {
  import Protocol._

  val kafkaConf = {
    val props = new Properties();
    /** all default configuration for new producer */
    props.put("bootstrap.servers", Config.KAFKA_METADATA_BROKER_LIST)
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
  val kafkaProducer = new KafkaProducer[Key, Val](kafkaConf)
}
object KafkaAggregatorActor extends WithProducer {
  var routees: ActorRef = _
  var shutdownTime = 1000 millis
  var isKafkaAvailable = false
  def init() = {
    Logger.info(s"init KafkaAggregatorActor.")
    if (Config.KAFKA_PRODUCER_POOL_SIZE > 0) {
      isKafkaAvailable = true
      routees = Akka.system.actorOf(RoundRobinPool(Config.KAFKA_PRODUCER_POOL_SIZE).props(KafkaAggregatorActor.props()))
    }
  }
  def shutdown() = {
    Logger.info(s"shutdown KafkaAggregatorActor.")
    if (isKafkaAvailable) routees ! Broadcast(PoisonPill)
    Thread.sleep(shutdownTime.length)
  }
  def enqueues(msgs: Seq[Protocol.KafkaMessage]) = {
    msgs.foreach(enqueue)
  }
  def enqueue(msg: Protocol.KafkaMessage) = {
    if (isKafkaAvailable) routees ! msg
  }
  def props() = Props(new KafkaAggregatorActor)
}

/**
 * TODO: change this to wal log handler.
 */
class KafkaAggregatorActor extends WithProducer with Stash {
  import Protocol._

  val failedCount = new AtomicLong(0L)
  val successCount = new AtomicLong(0L)
  val stashCount = new AtomicLong(0L)
  val logger = Logger("actor")

  implicit val ex = context.system.dispatcher

  context.system.scheduler.schedule(0 millis, 10 seconds) {
    self ! ShowMetrics
  }
  override def receive = {
    case ShowMetrics =>
      logger.info(s"[Stats]: failed[${failedCount.get}], stashed[${stashCount.get}], success[${successCount.get}]")

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
              logger.error(s"onCompletion", e)
              failedCount.incrementAndGet()
              replayTo ! m
            }
          }
        })
      } catch {
        case e @ (_: org.apache.kafka.clients.producer.BufferExhaustedException | _: Throwable) =>
          logger.error(s"$e", e)
          logger.info(s"stash")
          stash()
          stashCount.incrementAndGet()
      }

  }
}