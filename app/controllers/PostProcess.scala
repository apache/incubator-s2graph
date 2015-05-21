package controllers

import com.daumkakao.s2graph.core.HBaseElement._
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.models.HServiceColumn
import play.api.Logger
import play.api.libs.json.{JsString, JsObject, Json}

import scala.collection.TraversableOnce
import scala.collection.mutable.HashSet

/**
 * Created by jay on 14. 9. 1..
 */
object PostProcess extends JSONParser {

  private val queryLogger = Logger
  /**
   * Result Entity score field name
   */
  val SCORE_FIELD_NAME = "scoreSum"

  def groupEdgeResult(edgesWithRank: Seq[(Edge, Double)], excludeIds: Option[Map[InnerVal, Boolean]] = None) = {
    val groupedEdgesWithRank = edgesWithRank.groupBy {
      case (edge, rank) if edge.labelWithDir.dir == GraphUtil.directions("in") =>
        (edge.label.srcColumn.columnName, edge.label.tgtColumn.columnName, edge.tgtVertex.innerId)
      case (edge, rank) =>
        (edge.label.tgtColumn.columnName, edge.label.srcColumn.columnName, edge.tgtVertex.innerId)
    }
    for (((tgtColumnName, srcColumnName, target), edgesAndRanks) <- groupedEdgesWithRank if !excludeIds.getOrElse(Map[InnerVal, Boolean]()).contains(target)) yield {
      val (edges, ranks) = edgesAndRanks.groupBy(x => x._1.srcVertex).map(_._2.head).unzip
      Json.obj("name" -> tgtColumnName, "id" -> target.toString, SCORE_FIELD_NAME -> ranks.sum,
        "aggr" -> Json.obj("name" -> srcColumnName, "ids" -> edges.map(edge => edge.srcVertex.innerId.toString)))
    }
  }

  def sortWithFormatted[T](in: TraversableOnce[T], scoreField: Any = "scoreSum")(decrease: Boolean = true): JsObject = {
    var sortedJsons =
      in match {
        case inTrav: TraversableOnce[JsObject] =>
          in.toList.sortBy {
            case v: JsObject if scoreField.isInstanceOf[String] => (v \ scoreField.asInstanceOf[String]).as[Double]
          }
        case inTrav: TraversableOnce[String] =>
          in.toList.sortBy {
            case v: String => v
          }
      }
    if (decrease) sortedJsons = sortedJsons.reverse
    queryLogger.debug(s"sortedJsons : $sortedJsons")
    Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons.asInstanceOf[List[JsObject]])
  }

  def simple(edgesPerVertex: Seq[Iterable[(Edge, Double)]]) = {
    val ids = edgesPerVertex.flatMap(edges => edges.map(edge => edge._1.srcVertex.innerId.toString))
    val size = ids.size
    queryLogger.info(s"Result: $size")
    Json.obj("size" -> size, "results" -> ids)
    //    sortWithFormatted(ids)(false)
  }

  def summarizeWithListExcludeFormatted(exclude: Seq[Iterable[(Edge, Double)]], edgesPerVertexWithRanks: Seq[Iterable[(Edge, Double)]]) = {
    val excludeIds = exclude.flatMap(ex => ex.map { case (edge, score) => (edge.tgtVertex.innerId, true) }) toMap

    val seen = new HashSet[InnerVal]
    val edgesWithRank = edgesPerVertexWithRanks.flatten
    val jsons = groupEdgeResult(edgesWithRank, Some(excludeIds))
    val reverseSort = sortWithFormatted(jsons) _
    reverseSort(true)
  }

  /**
   * This method will be deprecated(because our response format will change by summarizeWithListExcludeFormatted functions' logic)
   * @param exclude
   * @param edgesPerVertexWithRanks
   * @return
   */
  def summarizeWithListExclude(exclude: Seq[Iterable[(Edge, Double)]],
                               edgesPerVertexWithRanks: Seq[Iterable[(Edge, Double)]]) = {
    val excludeIds = exclude.flatMap(ex => ex.map { case (edge, score) => (edge.tgtVertex.innerId, true) }) toMap

    val seen = new HashSet[InnerVal]
    val edgesWithRank = edgesPerVertexWithRanks.flatten
    val groupedEdgesWithRank = edgesWithRank.groupBy { case (edge, rank) => (edge.label.tgtColumn.columnName, edge.label.srcColumn.columnName, edge.tgtVertex.innerId) }
    val jsons = for (((tgtColumnName, srcColumnName, target), edgesAndRanks) <- groupedEdgesWithRank if !excludeIds.contains(target)) yield {
      val (edges, ranks) = edgesAndRanks.groupBy(x => x._1.srcVertex).map(_._2.head).unzip
      Json.obj(tgtColumnName -> target.toString, s"${srcColumnName}s" -> edges.map(edge => edge.srcVertex.innerId.toString), "scoreSum" -> ranks.sum)
    }
    val sortedJsons = jsons.toList.sortBy { jsObj => (jsObj \ "scoreSum").as[Double] }.reverse
    Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
  }

  def summarizeWithList(edgesPerVertexWithRanks: Seq[Iterable[(Edge, Double)]]) = {
    val edgesWithRank = edgesPerVertexWithRanks.flatten
    val jsons = groupEdgeResult(edgesWithRank)
    val reverseSort = sortWithFormatted(jsons) _
    reverseSort(true)
  }

  def summarizeWithListFormatted(edgesPerVertexWithRanks: Seq[Iterable[(Edge, Double)]]) = {
    val edgesWithRank = edgesPerVertexWithRanks.flatten
    val jsons = groupEdgeResult(edgesWithRank)
    val reverseSort = sortWithFormatted(jsons) _
    reverseSort(true)
  }

  def noFormat(edgesPerVertex: Seq[Iterable[(Edge, Double)]]) = {
    Json.obj("edges" -> edgesPerVertex.toString)
  }
  def toSimpleVertexArrJson(edgesPerVertex: Seq[Iterable[(Edge, Double)]]) = {
    val withScore = true
    import play.api.libs.json.Json

    val jsons = for {
      edges <- edgesPerVertex
      (edge, score) <- edges
    } yield {
        edgeToJson(edge, score)
      }
    val results =
      if (withScore) {
        jsons.sortBy(js => ((js \ "score").as[Double], (js \ "_timestamp").as[Long])).reverse
      } else {
        jsons.toList
      }
    queryLogger.info(s"Result: ${results.size}")
    Json.obj("size" -> jsons.size, "results" -> results)
  }
  def toSiimpleVertexArrJson(exclude: Seq[Iterable[(Edge, Double)]],
                             edgesPerVertexWithRanks: Seq[Iterable[(Edge, Double)]]) = {
    val excludeIds = exclude.flatMap(ex => ex.map { case (edge, score) => (edge.tgtVertex.innerId, true) }) toMap
    val withScore = true
    import play.api.libs.json.Json
    val jsons = for {
      edges <- edgesPerVertexWithRanks
      (edge, score) <- edges if !excludeIds.contains(edge.tgtVertex.innerId)
    } yield {
        edgeToJson(edge, score)
      }
    val results =
      if (withScore) {
        jsons.sortBy(js => ((js \ "score").as[Double], (js \ "_timestamp").as[Long])).reverse
      } else {
        jsons.toList
      }
    queryLogger.info(s"Result: ${results.size}")
    Json.obj("size" -> jsons.size, "results" -> results)
  }
  def verticesToJson(vertices: Iterable[Vertex]) = {
    Json.toJson(vertices.map { v => vertexToJson(v) })
  }
  def vertexToJson(vertex: Vertex) = {
    val serviceColumn = HServiceColumn.findById(vertex.id.colId)
    Json.obj("columnName" -> serviceColumn.columnName, "id" -> vertex.id.innerId.toString,
      "props" -> propsToJson(serviceColumn.metaNamesMap, vertex.props))
  }
  def propsToJson(edge: Edge) = {
    for ((seq, v) <- edge.props; name <- edge.label.metaPropNamesMap.get(seq) if seq > 0) yield {
      (name, innerValToJsValue(v))
    }
  }
  def edgeToJson(edge: Edge, score: Double) = {
    //    
    //    Logger.debug(s"edgeProps: ${edge.props} => ${props}")
    Json.obj("from" -> innerValToJsValue(edge.srcVertex.id.innerId),
      "to" -> (if (edge.tgtVertex == null) JsString("degree") else innerValToJsValue(edge.tgtVertex.id.innerId)),
      "label" -> edge.label.label,
      "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
      "_timestamp" -> edge.ts,
      //      "props" -> propsToJson(propNames, edge.props),
      "props" -> propsToJson(edge),
      //      "prev_step_props" -> edge.srcVertex.propsWithName,
      //      "metas" -> propsToJson(label.metaSeqsToNames, edge.metas),
      "score" -> score)
  }

  private def keysToName(seqsToNames: Map[Byte, String], props: Map[Byte, InnerVal]) = {
    for {
      (seq, value) <- props
      name <- seqsToNames.get(seq)
    } yield (name, value)
  }
  private def propsToJson(seqsToNames: Map[Byte, String], props: Map[Byte, InnerVal]) = {
    for ((keyName, innerVal) <- keysToName(seqsToNames, props)) yield (keyName -> innerVal.toString)
  }
  def toSimpleJson(edges: Iterable[(Vertex, Double)]) = {
    import play.api.libs.json.Json

    val arr = Json.arr(edges.map { case (v, w) => Json.obj("vId" -> v.id.toString, "score" -> w) })
    Json.obj("size" -> edges.size, "results" -> arr)
  }

  def sumUp(l: Iterable[(Vertex, Double)]) = {
    l.groupBy(_._1).map { case (v, list) => (v, list.foldLeft(0.0) { case (sum, (vertex, r)) => sum + r }) }
  }

  // Assume : l,r are unique lists
  def union(l: Iterable[(Vertex, Double)], r: Iterable[(Vertex, Double)]) = {
    val ret = l.toList ::: r.toList
    sumUp(ret)
  }

  // Assume : l,r are unique lists
  def intersect(l: Iterable[(Vertex, Double)], r: Iterable[(Vertex, Double)]) = {
    val ret = l.toList ::: r.toList
    sumUp(ret.groupBy(_._1).filter(_._2.size > 1).map(_._2).flatten)
  }

}