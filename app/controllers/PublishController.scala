package controllers

import com.daumkakao.s2graph.core.ExceptionHandler
import config.Config
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.mvc._

import scala.concurrent.Future

object PublishController extends Controller {

  import ApplicationController._
  import ExceptionHandler._
  import play.api.libs.concurrent.Execution.Implicits._

  /**
   * never check validation on string. just redirect strings to kafka.
   */
  val serviceNotExistException = new RuntimeException(s"service is not created in s2graph. create service first.")

  //  private def toService(topic: String): String = {
  //    Service.findByName(topic).map(service => s"${service.serviceName}-${Config.PHASE}").getOrElse(throw serviceNotExistException)
  //  }
  def publishOnly(topic: String) = withHeaderAsync(parse.text) { request =>
    if (!Config.IS_WRITE_SERVER) Future.successful(UNAUTHORIZED)
    //  val kafkaTopic = toService(topic)
    val strs = request.body.split("\n")
    strs.foreach(str => {
      val keyedMessage = new ProducerRecord[Key, Val](Config.KAFKA_LOG_TOPIC, str)
      //    val keyedMessage = new ProducerRecord[Key, Val](kafkaTopic, s"$str")
      //        logger.debug(s"$kafkaTopic, $str")
      ExceptionHandler.enqueue(KafkaMessage(keyedMessage))
    })
    Future.successful(
      Ok("publish success.\n").withHeaders(CONNECTION -> "Keep-Alive", "Keep-Alive" -> "timeout=10, max=10")
    )
    //    try {
    //
    //    } catch {
    //      case e: Exception => Future.successful(BadRequest(e.getMessage))
    //    }
  }

  def publish(topic: String) = publishOnly(topic)

  //  def mutateBulk(topic: String) = Action.async(parse.text) { request =>
  //    EdgeController.mutateAndPublish(Config.KAFKA_LOG_TOPIC, Config.KAFKA_FAIL_TOPIC, request.body).map { result =>
  //      result.withHeaders(CONNECTION -> "Keep-Alive", "Keep-Alive" -> "timeout=10, max=10")
  //    }
  //  }
  def mutateBulk(topic: String) = withHeaderAsync(parse.text) { request =>
    EdgeController.mutateAndPublish(request.body)
  }
}
