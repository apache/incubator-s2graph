package controllers

import com.daumkakao.s2graph.rest.actors.{Protocol, KafkaAggregatorActor}
import com.daumkakao.s2graph.rest.config.{Instrumented, Config}
import com.daumkakao.s2graph.core.{ Edge, Graph, GraphElement, GraphUtil, Vertex, KGraphExceptions }
import play.api.Logger
import play.api.libs.json.{Json, JsValue}
import play.api.mvc.{ Controller, Result }
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object EdgeController extends Controller with Instrumented with RequestParser {

  import controllers.ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._

  private val maxLength = 1024 * 1024 * 16
  private[controllers] def tryMutates(jsValue: JsValue, operation: String): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)
    else {
      try {
        Logger.debug(s"$jsValue")
        val edges = toEdges(jsValue, operation)
        for { edge <- edges } {
          if (edge.isAsync) {
            KafkaAggregatorActor.enqueue(Protocol.elementToKafkaMessage(s"${edge.serviceName}-${Config.PHASE}", edge, None))
          } else {
            KafkaAggregatorActor.enqueue(Protocol.elementToKafkaMessage(Config.KAFKA_LOG_TOPIC, edge, None))
          }
        }
        // store valid edges came to system.
//        getOrElseUpdateMetric("IncommingEdges")(metricRegistry.counter("IncommingEdges")).inc(edges.size)

        val edgesToStore = edges.filterNot(e => e.isAsync)
        //FIXME:
        Graph.mutateEdges(edgesToStore).map { rets =>
          Ok(s"${Json.toJson(rets)}").as(QueryController.applicationJsonHeader)
        }
        
      } catch {
        case e: KGraphExceptions.JsonParseException => Future.successful(BadRequest(s"e"))
        case e: Throwable =>
          play.api.Logger.error(s"mutateAndPublish: $e", e)
          Future.successful(InternalServerError(s"${e.getStackTraceString}"))
      }
    }
  }
  
//  private[controllers] def aggregateElements(elements: Seq[GraphElement], originalString: Seq[Option[String]]): Future[Seq[Boolean]] = {
//    elements.foreach {
//
//    }
//    KafkaAggregatorActor.enqueue(Protocol.elementToKafkaMessage(Config.KAFKA_LOG_TOPIC, element, originalString))
//    Graph.mutateElements(elements)
//  }
  private[controllers] def mutateAndPublish(str: String): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)

    Logger.debug(s"$str")
    val edgeStrs = str.split("\\n")

    var vertexCnt = 0L
    var edgeCnt = 0L
    try {
      val elements =
        for (edgeStr <- edgeStrs; str <- GraphUtil.parseString(edgeStr); element <- Graph.toGraphElement(str)) yield {
          element match {
            case v: Vertex => vertexCnt += 1
            case e: Edge => edgeCnt += 1
          }
          if (element.isAsync) {
            KafkaAggregatorActor.enqueue(Protocol.elementToKafkaMessage(s"${element.serviceName}-${Config.PHASE}", element, Some(str)))
          } else {
            KafkaAggregatorActor.enqueue(Protocol.elementToKafkaMessage(Config.KAFKA_LOG_TOPIC, element, Some(str)))
          }
          element
        }
//      val elements = edgesWithStrs.map(_._1)
//      val strs = edgesWithStrs.map(_._2)
//      getOrElseUpdateMetric("IncommingVertices")(metricRegistry.counter("IncommingVertices")).inc(vertexCnt)
//      getOrElseUpdateMetric("IncommingEdges")(metricRegistry.counter("IncommingEdges")).inc(edgeCnt)

      //FIXME:
      val elementsToStore = elements.filterNot(e => e.isAsync)
      Graph.mutateElements(elementsToStore).map { rets =>
        Ok(s"${Json.toJson(rets)}").as(QueryController.applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
      case e: Throwable =>
        play.api.Logger.error(s"mutateAndPublish: $e", e)
        Future.successful(InternalServerError(s"${e.getStackTraceString}"))
    }
  }

  def mutateBulk() = withHeaderAsync(parse.text) { request =>
    mutateAndPublish(request.body)
  }


  def inserts() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "insert")
  }


  def insertsBulk() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "insertBulk")
  }

  def deletes() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "delete")
  }

  def updates() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "update")
  }

  def increments() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "increment")
  }

}