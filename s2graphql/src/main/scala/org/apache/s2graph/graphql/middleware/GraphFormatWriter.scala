package org.apache.s2graph.graphql.middleware

import org.apache.s2graph.core.schema.ServiceColumn
import org.apache.s2graph.core.{GraphElement, S2EdgeLike, S2VertexLike}
import org.apache.s2graph.graphql.types.PlayJsonScalarType
import org.slf4s.LoggerFactory
import play.api.libs.json._
import sangria.execution._
import sangria.schema.Context


object GraphFormatted extends Middleware[Any] with MiddlewareAfterField[Any] with MiddlewareExtension[Any] {
  implicit val logger = LoggerFactory.getLogger(this.getClass)

  type QueryVal = java.util.concurrent.ConcurrentHashMap[GraphElement, Unit]

  type FieldVal = Long


  def beforeQuery(context: MiddlewareQueryContext[Any, _, _]) = {
    new java.util.concurrent.ConcurrentHashMap[GraphElement, Unit]()
  }

  def afterQuery(queryVal: QueryVal, context: MiddlewareQueryContext[Any, _, _]) = ()

  def toVertexId(v: S2VertexLike, c: ServiceColumn): String = {
    val innerId = v.innerId.toIdString()

    s"${c.service.serviceName}.${c.columnName}.${innerId}"
  }

  def toVertexJson(v: S2VertexLike, c: ServiceColumn): JsValue = {
    Json.obj(
      "id" -> toVertexId(v, c),
      "label" -> v.innerId.toIdString()
    )
  }

  def toEdgeJson(e: S2EdgeLike): JsValue = {
    Json.obj(
      "source" -> toVertexId(e.srcVertex, e.innerLabel.srcColumn),
      "target" -> toVertexId(e.tgtVertex, e.innerLabel.tgtColumn),
      "id" -> s"${toVertexId(e.srcVertex, e.innerLabel.srcColumn)}.${e.label()}.${toVertexId(e.tgtVertex, e.innerLabel.tgtColumn)}",
      "label" -> e.label()
    )
  }

  def afterQueryExtensions(queryVal: QueryVal,
                           context: MiddlewareQueryContext[Any, _, _]
                          ): Vector[Extension[_]] = {

    import scala.collection.JavaConverters._
    val elements = queryVal.keys().asScala.toVector

    val edges = elements.collect { case e: S2EdgeLike => e }
    val vertices = elements.collect { case v: S2VertexLike => v -> v.serviceColumn }
    val verticesFromEdges = edges.flatMap { e =>
      val label = e.innerLabel
      Vector((e.srcVertex, label.srcColumn), (e.tgtVertex, label.srcColumn))
    }

    val verticesJson = (vertices ++ verticesFromEdges).map { case (v, c) => toVertexJson(v, c) }.distinct
    val edgeJson = edges.map(toEdgeJson).distinct

    val jsElements = Json.obj(
      "nodes" -> verticesJson,
      "edges" -> edgeJson
    )

    val graph = Json.obj("graph" -> jsElements)

    /**
      * nodes: [{id, label, x, y, size}, ..],
      * edges: [{id, source, target, label}]
      */
    implicit val iu = PlayJsonScalarType.PlayJsonInputUnmarshaller
    Vector(Extension[JsValue](graph))
  }

  def beforeField(queryVal: QueryVal, mctx: MiddlewareQueryContext[Any, _, _], ctx: Context[Any, _]) = {
    continue(System.currentTimeMillis())
  }

  def afterField(queryVal: QueryVal, fieldVal: FieldVal, value: Any, mctx: MiddlewareQueryContext[Any, _, _], ctx: Context[Any, _]) = {
    //    logger.info(s"${ctx.parentType.name}.${ctx.field.name} = ${value.getClass.getName}")

    value match {
      case ls: Seq[_] => ls.foreach {
        case e: GraphElement => queryVal.put(e, ())
        case _ =>
      }
      case e: GraphElement => queryVal.put(e, ())
      case _ =>
    }

    None
  }
}



