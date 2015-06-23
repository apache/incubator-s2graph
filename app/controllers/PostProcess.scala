package controllers

import com.daumkakao.s2graph.core._

//import com.daumkakao.s2graph.core.mysqls._

import com.daumkakao.s2graph.core.models._
import com.daumkakao.s2graph.core.types2.{InnerVal, InnerValLike}

import play.api.Logger
import play.api.libs.json.{JsObject, Json}

import scala.collection.TraversableOnce
import scala.collection.mutable.{ListBuffer, HashSet}

/**
 * Created by jay on 14. 9. 1..
 */
object PostProcess extends JSONParser {

  private val queryLogger = Logger
  /**
   * Result Entity score field name
   */
  val SCORE_FIELD_NAME = "scoreSum"

  def groupEdgeResult(edgesWithRank: Seq[(Edge, Double)], excludeIds: Option[Map[InnerValLike, Boolean]] = None) = {
    //    filterNot {case (edge, score) => edge.props.contains(LabelMeta.degreeSeq)}
    val groupedEdgesWithRank = edgesWithRank.groupBy {
      case (edge, rank) if edge.labelWithDir.dir == GraphUtil.directions("in") =>
        (edge.label.srcColumn, edge.label.tgtColumn, edge.tgtVertex.innerId, edge.props.contains(LabelMeta.degreeSeq))
      case (edge, rank) =>
        (edge.label.tgtColumn, edge.label.srcColumn, edge.tgtVertex.innerId, edge.props.contains(LabelMeta.degreeSeq))
    }
    for {
      ((tgtColumn, srcColumn, target, isDegreeEdge), edgesAndRanks) <- groupedEdgesWithRank
      if !excludeIds.getOrElse(Map[InnerValLike, Boolean]()).contains(target) && !isDegreeEdge
      (edges, ranks) = edgesAndRanks.groupBy(x => x._1.srcVertex).map(_._2.head).unzip
      id <- innerValToJsValue(target, tgtColumn.columnType)
    } yield {
      Json.obj("name" -> tgtColumn.columnName, "id" -> id, SCORE_FIELD_NAME -> ranks.sum,
        "aggr" -> Json.obj("name" -> srcColumn.columnName,
          "ids" -> edges.flatMap(edge => innerValToJsValue(edge.srcVertex.innerId, srcColumn.columnType))))
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

  def simple(edgesPerVertex: Seq[Iterable[(Edge, Double)]], q: Query) = {
    val ids = edgesPerVertex.flatMap(edges => edges.map(edge => edge._1.srcVertex.innerId.toString))
    val size = ids.size
    queryLogger.info(s"Result: $size")
    Json.obj("size" -> size, "results" -> ids)
    //    sortWithFormatted(ids)(false)
  }

  def summarizeWithListExcludeFormatted(exclude: Seq[Iterable[(Edge, Double)]], edgesPerVertexWithRanks: Seq[Iterable[(Edge, Double)]], q: Query) = {
    val excludeIds = exclude.flatMap(ex => ex.map { case (edge, score) => (edge.tgtVertex.innerId, true) }) toMap

    val seen = new HashSet[InnerValLike]
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


    val edgesWithRank = edgesPerVertexWithRanks.flatten
    val groupedEdgesWithRank = edgesWithRank.filterNot {case (edge, score) =>
      edge.props.contains(LabelMeta.degreeSeq)
    }.groupBy { case (edge, rank) =>
      (edge.label.tgtColumn, edge.label.srcColumn, edge.tgtVertex.innerId)
    }
    val jsons = for {
      ((tgtColumn, srcColumn, target), edgesAndRanks) <- groupedEdgesWithRank if !excludeIds.contains(target)
      (edges, ranks) = edgesAndRanks.groupBy(x => x._1.srcVertex).map(_._2.head).unzip
      tgtId <- innerValToJsValue(target, tgtColumn.columnType)
    } yield {
        Json.obj(tgtColumn.columnName -> tgtId,
          s"${srcColumn.columnName}s" ->
            edges.flatMap(edge => innerValToJsValue(edge.srcVertex.innerId, srcColumn.columnType)), "scoreSum" -> ranks.sum)
      }
    val sortedJsons = jsons.toList.sortBy { jsObj => (jsObj \ "scoreSum").as[Double] }.reverse
    Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
  }

  def summarizeWithList(edgesPerVertexWithRanks: Seq[Iterable[(Edge, Double)]], q: Query) = {
    val edgesWithRank = edgesPerVertexWithRanks.flatten
    val jsons = groupEdgeResult(edgesWithRank)
    val reverseSort = sortWithFormatted(jsons) _
    reverseSort(true)
  }

  def summarizeWithListFormatted(edgesPerVertexWithRanks: Seq[Iterable[(Edge, Double)]], q: Query) = {
    val edgesWithRank = edgesPerVertexWithRanks.flatten
    val jsons = groupEdgeResult(edgesWithRank)
    val reverseSort = sortWithFormatted(jsons) _
    reverseSort(true)
  }

  def noFormat(edgesPerVertex: Seq[Iterable[(Edge, Double)]]) = {
    Json.obj("edges" -> edgesPerVertex.toString)
  }



  def toSimpleVertexArrJson(edgesPerVertex: Seq[Iterable[(Edge, Double)]], q: Query) = {
    val withScore = true
    import play.api.libs.json.Json
    val degreeJsons = ListBuffer[JsObject]()
    val degrees = ListBuffer[JsObject]()
    val edgeJsons = ListBuffer[JsObject]()

    for {
      edges <- edgesPerVertex
      (edge, score) <- edges
    } {
      if (edge.propsWithTs.contains(LabelMeta.degreeSeq)) {
        //          degreeJsons += edgeJson
        degrees += Json.obj("label" -> edge.label.label,
          LabelMeta.degree.name ->
            innerValToJsValue(edge.propsWithTs(LabelMeta.degreeSeq).innerVal, InnerVal.LONG)
        )
      } else {
        for {
          edgeJson <- edgeToJson(edge, score, q)
        } {
          edgeJsons += edgeJson
        }
      }
    }

    val results =
      if (withScore) {
        degreeJsons ++ edgeJsons.sortBy(js => ((js \ "score").as[Double], (js \ "_timestamp").as[Long])).reverse
      } else {
        degreeJsons ++ edgeJsons.toList
      }

    queryLogger.info(s"Result: ${results.size}")
    Json.obj("size" -> results.size, "degrees" -> degrees, "results" -> results)
  }

  def toSiimpleVertexArrJson(exclude: Seq[Iterable[(Edge, Double)]],
                             edgesPerVertexWithRanks: Seq[Iterable[(Edge, Double)]], q: Query) = {
    val excludeIds = exclude.flatMap(ex => ex.map { case (edge, score) => (edge.tgtVertex.innerId, true) }) toMap
    val withScore = true
    import play.api.libs.json.Json
    val jsons = for {
      edges <- edgesPerVertexWithRanks
      (edge, score) <- edges if !excludeIds.contains(edge.tgtVertex.innerId) && !edge.props.contains(LabelMeta.degreeSeq)
      edgeJson <- edgeToJson(edge, score, q)
    } yield edgeJson

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
    Json.toJson(vertices.flatMap { v => vertexToJson(v) })
  }

  def propsToJson(edge: Edge) = {
    for {
      (seq, v) <- edge.props
      metaProp <- edge.label.metaPropsMap.get(seq) if seq >= 0
      jsValue <- innerValToJsValue(v, metaProp.dataType)
    } yield {
      (metaProp.name, jsValue)
    }
  }

  def edgeToJson(edge: Edge, score: Double, q: Query): Option[JsObject] = {
    //
    //    Logger.debug(s"edgeProps: ${edge.props} => ${props}")
    val shouldBeReverted = q.labelSrcTgtInvertedMap.get(edge.labelWithDir.labelId).getOrElse(false)
    val json = for {
      from <- {
        if (shouldBeReverted) {
          innerValToJsValue(edge.srcVertex.id.innerId, edge.label.tgtColumn.columnType)
        } else {
          innerValToJsValue(edge.srcVertex.id.innerId, edge.label.srcColumn.columnType)
        }
      }
      to <- {
        if (shouldBeReverted) {
          innerValToJsValue(edge.tgtVertex.id.innerId, edge.label.srcColumn.columnType)
        } else {
          innerValToJsValue(edge.tgtVertex.id.innerId, edge.label.tgtColumn.columnType)
        }
      }
    } yield {
        Json.obj(
          "from" -> from,
          "to" -> to,
          "label" -> edge.label.label,
          "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
          "_timestamp" -> edge.ts,
          "props" -> propsToJson(edge),
          "score" -> score
        )
      }

    json
  }

  def vertexToJson(vertex: Vertex): Option[JsObject] = {
    val serviceColumn = ServiceColumn.findById(vertex.id.colId)
    for {

      id <- innerValToJsValue(vertex.innerId, serviceColumn.columnType)
    } yield {
      Json.obj("serviceName" -> serviceColumn.service.serviceName,
        "columnName" -> serviceColumn.columnName,
        "id" -> id, "props" -> propsToJson(vertex), "timestamp" -> vertex.ts)
    }
  }

  private def keysToName(seqsToNames: Map[Int, String], props: Map[Int, InnerValLike]) = {
    for {
      (seq, value) <- props
      name <- seqsToNames.get(seq)
    } yield (name, value)
  }

  private def propsToJson(vertex: Vertex) = {
    val serviceColumn = vertex.serviceColumn
    val props = for {
      (propKey, innerVal) <- vertex.props
      columnMeta <- ColumnMeta.findByIdAndSeq(serviceColumn.id.get, propKey.toByte, useCache = true)
      jsValue <- innerValToJsValue(innerVal, columnMeta.dataType)
    } yield {
        (columnMeta.name -> jsValue)
      }
    props.toMap
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