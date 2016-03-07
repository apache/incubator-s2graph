/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package actors

import java.util.concurrent.TimeUnit

import actors.Protocol.FlushAll
import akka.actor._
import com.kakao.s2graph.core.ExceptionHandler._
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.utils.logger
import config.Config
import play.api.Play.current
import play.api.libs.concurrent.Akka

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
  def init(s2: Graph) = {
    router = Akka.system.actorOf(props(s2))
  }

  def shutdown() = {
    router ! FlushAll
    Akka.system.shutdown()
    Thread.sleep(Config.ASYNC_HBASE_CLIENT_FLUSH_INTERVAL * 2)
  }

  def props(s2: Graph): Props = Props(classOf[QueueActor], s2)
}

class QueueActor(s2: Graph) extends Actor with ActorLogging {

  import Protocol._

  implicit val ec = context.system.dispatcher
  //  logger.error(s"QueueActor: $self")
  val queue = mutable.Queue.empty[GraphElement]
  var queueSize = 0L
  val maxQueueSize = Config.LOCAL_QUEUE_ACTOR_MAX_QUEUE_SIZE
  val timeUnitInMillis = 10
  val rateLimitTimeStep = 1000 / timeUnitInMillis
  val rateLimit = Config.LOCAL_QUEUE_ACTOR_RATE_LIMIT / rateLimitTimeStep


  context.system.scheduler.schedule(Duration.Zero, Duration(timeUnitInMillis, TimeUnit.MILLISECONDS), self, Flush)

  override def receive: Receive = {
    case element: GraphElement =>

      if (queueSize > maxQueueSize) {
        ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_FAIL_TOPIC, element, None))
      } else {
        queueSize += 1L
        queue.enqueue(element)
      }

    case Flush =>
      val elementsToFlush =
        if (queue.size < rateLimit) queue.dequeueAll(_ => true)
        else (0 until rateLimit).map(_ => queue.dequeue())

      val flushSize = elementsToFlush.size

      queueSize -= elementsToFlush.length
      s2.mutateElements(elementsToFlush)

      if (flushSize > 0) {
        logger.info(s"flush: $flushSize, $queueSize")
      }

    case FlushAll =>
      s2.mutateElements(queue)
      context.stop(self)

    case _ => logger.error("unknown protocol")
  }
}
