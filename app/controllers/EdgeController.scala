package controllers

import com.daumkakao.s2graph.rest.actors._
import com.wordnik.swagger.annotations._
import com.daumkakao.s2graph.rest.config.{Instrumented, Config}
import com.daumkakao.s2graph.core.{ Edge, Graph, GraphElement, GraphUtil, Vertex, KGraphExceptions }
import play.api.Logger
import play.api.libs.json.JsValue
import play.api.mvc.{ Controller, Result }
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object EdgeController extends Controller with Instrumented with RequestParser {

  import controllers.ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._

  private val maxLength = 1024 * 1024 * 16
  import com.daumkakao.s2graph.core.Logger._

  private[controllers] def tryMutates(jsValue: JsValue, operation: String): Future[Result] = {
    Future {
      if (!Config.IS_WRITE_SERVER) Unauthorized

      val edges = new ListBuffer[Edge]
      try {
        edges ++= toEdges(jsValue, operation)
        // store valid edges came to system.
        ticked(s"[Write]: ${edges.size}") {
          for (edge <- edges) {
            try {
              aggregateElement(edge, None)
            } catch {
              case e: Throwable => Logger.error(s"tryMutates: $edge, $e", e)
            }
          }
          getOrElseUpdateMetric("IncommingEdges")(metricRegistry.counter("IncommingEdges")).inc(edges.size)
          Ok(s"${edges.size} $operation success\n")
        }
      } catch {
        case e: KGraphExceptions.JsonParseException => BadRequest(s"e")
        case e: Throwable =>
          play.api.Logger.error(s"mutateAndPublish: $e", e)
          InternalServerError(s"${e.getStackTraceString}")
      }
    }
  }
  
  private[controllers] def aggregateElement(element: GraphElement, originalString: Option[String]) = {
    if (element.isAsync) KafkaAggregatorActor.enqueue(Protocol.elementToKafkaMessage(s"${element.serviceName}-${Config.phase}", element, originalString))
    KafkaAggregatorActor.enqueue(Protocol.elementToKafkaMessage(Config.KAFKA_LOG_TOPIC, element, originalString))
    if (!element.isAsync) {
      element match {
        case v: Vertex => Graph.mutateVertex(v)
        case e: Edge => Graph.mutateEdge(e)
        case _ => Logger.error(s"InvalidType: $element, $originalString")
      }
    }
//    if (!element.isAsync) GraphAggregatorActor.enqueue(element)
  }
  private[controllers] def mutateAndPublish(str: String) = {
    Future {
      if (!Config.IS_WRITE_SERVER) Unauthorized
      
      val edgeStrs = str.split("\\n")

      val elements = new ListBuffer[GraphElement]
      var vertexCnt = 0L
      var edgeCnt = 0L
      try {
        ticked(s"[Write]: ${edgeStrs.size}") {
          val parsedElements =
            for (edgeStr <- edgeStrs; str <- GraphUtil.parseString(edgeStr); element <- Graph.toGraphElement(str)) yield {
              element match {
                case v: Vertex => vertexCnt += 1
                case e: Edge => edgeCnt += 1
              }
              try {
                aggregateElement(element, Some(str))
              } catch {
                case e: Throwable => Logger.error(s"mutateAndPublish: $element, $e", e)
              }
              element
            }
          elements ++= parsedElements

          getOrElseUpdateMetric("IncommingVertices")(metricRegistry.counter("IncommingVertices")).inc(vertexCnt)
          getOrElseUpdateMetric("IncommingEdges")(metricRegistry.counter("IncommingEdges")).inc(edgeCnt)

          Ok(s" ${parsedElements.size} mutation success.\n")
        }
      } catch {
        case e: KGraphExceptions.JsonParseException => BadRequest(s"$e")
        case e: Throwable =>
          play.api.Logger.error(s"mutateAndPublish: $e", e)
          InternalServerError(s"${e.getStackTraceString}")
      }
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