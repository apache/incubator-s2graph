package com.daumkakao.s2graph.rest.actors

import com.daumkakao.s2graph.rest.config.Config
import com.daumkakao.s2graph.core.{ Edge, Graph, GraphElement, Vertex }
import java.util.Properties
import java.util.concurrent.TimeoutException
import akka.actor.{ Actor, ActorLogging, ActorRef, PoisonPill, Props, Stash }
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akka.routing.{ Broadcast, ConsistentHashingPool, RoundRobinPool }
import play.api.Logger
import play.libs.Akka
import scala.collection.mutable.SynchronizedQueue
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
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
//
//object GraphAggregatorActor extends WithProducer {
//  var routees: ActorRef = _
//  var shutdownTime = 1000 millis
//  def init() = {
//    Logger.info(s"init GraphAggregatorActor.")
//    //    routees = Akka.system().actorOf(RoundRobinPool(Config.CLIENT_AGGREGATE_POOL_SIZE).props(GraphAggregatorActor.props()))
//    routees = Akka.system().actorOf(ConsistentHashingPool(Config.CLIENT_AGGREGATE_POOL_SIZE, hashMapping = hashMapping).props(GraphAggregatorActor.props()))
//  }
//  def init(poolSize: Int) = {
//    Logger.info(s"init GraphAggregatorActor.")
//    routees = Akka.system().actorOf(RoundRobinPool(poolSize).props(GraphAggregatorActor.props()))
//  }
//  def shutdown() = {
//    Logger.info(s"shutdown GraphAggregatorActor.")
//    routees ! Broadcast(PoisonPill)
//    Thread.sleep(shutdownTime.length)
//  }
//  def enqueue(element: GraphElement) = {
//    routees ! element
//  }
//  def updateHealth(isHealthy: Boolean) = {
//    routees ! Broadcast(Protocol.UpdateHealth(isHealthy))
//  }
//  def props() = Props(new GraphAggregatorActor)
//
//  def hashMapping: ConsistentHashMapping = {
//    case element: GraphElement => element match {
//      case v: Vertex => v.id
//      case e: Edge => (e.srcVertex.innerId, e.labelWithDir.labelId, e.tgtVertex.innerId)
//    }
//  }
//}
object KafkaAggregatorActor extends WithProducer {
  var routees: ActorRef = _
  var shutdownTime = Config.KAFKA_AGGREGATE_FLUSH_TIMEOUT millis
  def init() = {
    Logger.info(s"init KafkaAggregatorActor.")
    routees = Akka.system.actorOf(RoundRobinPool(Config.KAFKA_PRODUCER_POOL_SIZE).props(KafkaAggregatorActor.props()))
  }
  def shutdown() = {
    Logger.info(s"shutdown KafkaAggregatorActor.")
    routees ! Broadcast(PoisonPill)
    Thread.sleep(shutdownTime.length)
  }
  def enqueues(msgs: Seq[Protocol.KafkaMessage]) = {
    msgs.foreach(enqueue)
  }
  def enqueue(msg: Protocol.KafkaMessage) = {
    routees ! msg
  }
  def updateHealth(isHealthy: Boolean) = {
    routees ! Broadcast(Protocol.UpdateHealth(isHealthy))
  }
  def props() = Props(new KafkaAggregatorActor)
}
//abstract class AggregatorActor[T] extends Actor with ActorLogging {
//
//  import Protocol._
//  private val cName = this.getClass().getName()
//  private val flushSize = Config.CLIENT_AGGREGATE_BUFFER_SIZE
//  private val flushTimeInMillis = Config.CLIENT_AGGREGATE_BUFFER_FLUSH_TIME
//  private var isHealthy = true
//
//  protected val schedulerEx: ExecutionContext = Akka.system.dispatchers.lookup("contexts.scheduler")
//  protected implicit val blockingEx: ExecutionContext = Akka.system.dispatchers.lookup("contexts.blocking")
//  protected val logger = Logger("actor")
//
//  val buffer = new SynchronizedQueue[T]
//  var bufferSize = 0L
//  var startTs = System.currentTimeMillis()
//
//  context.system.scheduler.schedule(0 millis, flushTimeInMillis millis, self, Protocol.FlushBuffer)(executor = schedulerEx)
//
//  def shouldFlush() = bufferSize >= flushSize || (System.currentTimeMillis() - startTs) >= flushTimeInMillis
//  def buildBufferedMsgs(): Seq[T] = buffer.dequeueAll(_ => true)
//  def sendBufferedMsgs(msgs: Seq[T]): Unit
//
//  def receive = {
//    case bufferedMsgs: Seq[T] => sendBufferedMsgs(bufferedMsgs)
//
//    case Protocol.FlushBuffer => self ! buildBufferedMsgs
//
//    case Protocol.UpdateHealth(newHealth) => isHealthy = newHealth
//
//    case msg: T =>
//      //      Logger.debug(s"$self : $msg")
//      if (bufferSize == 0) startTs = System.currentTimeMillis()
//      buffer += msg
//      bufferSize += 1
//      if (shouldFlush) self ! buildBufferedMsgs
//  }
//
//  def withTimeout[T](op: => Future[T], fallback: => T)(implicit timeout: Duration): Future[T] = {
//    val timeoutFuture = akka.pattern.after(timeout.toMillis millis, using = context.system.scheduler) { Future { fallback } }
//    //    val timeoutFuture = play.api.libs.concurrent.Promise.timeout(fallback, timeout)
//    Future.firstCompletedOf(Seq(op, timeoutFuture))
//
//  }
//
//  def withFallbackAndTimeout[T](startAt: Long = System.currentTimeMillis(), seqs: Seq[T])(future: => Future[Unit])(timeout: FiniteDuration) = {
//    val f = if (isHealthy) future else Future { throw new TimeoutException(s"$this isHealty[$isHealthy]") }
//    val newFuture = withTimeout[Unit](f, {})(timeout)
//    newFuture.onComplete {
//      case Failure(ex) =>
//        val duration = System.currentTimeMillis() - startAt
//        logger.error(s"[Failed] $this flush: ${seqs.size} took $duration $ex", ex)
//
//        seqs.foreach { e =>
//          val newVal = e match {
//            case graphElement: GraphElement => Some(e.toString)
//            case kafkaMessage: KafkaMessage => Some(kafkaMessage.msg.value)
//            case _ =>
//              logger.error(s"wrong type of message in actor. only GraphElement/KafkaMessage type is allowed. $e")
//              None
//          }
//          newVal.foreach { v =>
//            KafkaAggregatorActor.enqueue(Protocol.KafkaMessage(new ProducerRecord[Key, Val](Config.KAFKA_FAIL_TOPIC, v)))
//          }
//        }
//
//        logger.error(s"[Failed] $this publish to ${Config.KAFKA_FAIL_TOPIC}")
//      case Success(s) =>
//        val duration = System.currentTimeMillis() - startAt
//        logger.info(s"[Success] $this flush: ${seqs.size} took $duration")
//    }
//    newFuture
//  }
//}
//
//class GraphAggregatorActor extends AggregatorActor[GraphElement] {
//  private val timeout = Config.CLIENT_AGGREGATE_FLUSH_TIMEOUT millis
//
//  override def sendBufferedMsgs(msgs: Seq[GraphElement]) = {
//    withFallbackAndTimeout[GraphElement](seqs = msgs) {
//      Future { Graph.bulkMutates(msgs, mutateInPlace = false) }(blockingEx)
//    }(timeout)
//  }
//}

class KafkaAggregatorActor extends WithProducer with Stash {
  import Protocol._

  val timeout = Config.KAFKA_AGGREGATE_FLUSH_TIMEOUT millis

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