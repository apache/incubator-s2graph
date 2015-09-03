package actors

import java.util.concurrent.TimeUnit

import actors.Protocol.FlushAll
import akka.actor._
import akka.actor.Actor.Receive
import akka.routing.RoundRobinRouter
import com.daumkakao.s2graph.core.ExceptionHandler._
import com.daumkakao.s2graph.core._
import config.Config
import play.api.Logger
import play.api.libs.concurrent.Akka
import play.api.Play.current
import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
 * Created by shon on 9/2/15.
 */
object Protocol {
  case object Flush
  case object FlushAll
}

object QueueActor {
  /** we are throttling down here so fixed number of actor to constant */
  var router: ActorRef = _
//    Akka.system.actorOf(props(), name = "queueActor")
  def init() = {
    router = Akka.system.actorOf(props())
  }
  def shutdown() = {
    router ! FlushAll
    Akka.system.shutdown()
    Thread.sleep(Config.ASYNC_HBASE_CLIENT_FLUSH_INTERVAL * 2)
  }
  def props(): Props = Props[QueueActor]
}

class QueueActor extends Actor with ActorLogging {
  import Protocol._
  implicit val ec = context.system.dispatcher
//  Logger.error(s"QueueActor: $self")
  var queue = mutable.Queue.empty[GraphElement]
  var queueSize = 0L
  val maxQueueSize = Config.LOCAL_QUEUE_ACTOR_MAX_QUEUE_SIZE
  val timeUnitInMillis = 10
  val rateLimitTimeStep = 1000 / timeUnitInMillis
  val rateLimit = Config.LOCAL_QUEUE_ACTOR_RATE_LIMIT / rateLimitTimeStep


  context.system.scheduler.schedule(Duration.Zero, Duration(timeUnitInMillis, TimeUnit.MILLISECONDS), self, Flush)

  override def receive: Receive = {
    case element: GraphElement =>
//      Logger.error(s"Receive: $element")
      queueSize += 1L
      if (queueSize > maxQueueSize) {
        Logger.error(s"local queue overflow. $queueSize")
        ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_FAIL_TOPIC, element, None))
      } else {
        queue.enqueue(element)
      }

    case Flush =>
      val (elementsToFlush, remains) = queue.splitAt(rateLimit)
      val flushSize = elementsToFlush.size
      queue = remains
      queueSize -= elementsToFlush.length
      Graph.mutateElements(elementsToFlush)
      if (flushSize > 0) Logger.info(s"flush: $flushSize.size, $queueSize")

    case FlushAll =>
      Graph.mutateElements(queue)
      context.stop(self)

    case _ => log.error("unknown protocol")
  }
}
