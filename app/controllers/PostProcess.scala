package controllers

import com.daumkakao.s2graph.core._

//import com.daumkakao.s2graph.core.mysqls._

import com.daumkakao.s2graph.core.models._
import com.daumkakao.s2graph.core.types2.{InnerVal, InnerValLike}
import play.api.Logger
import play.api.libs.json.{JsObject, Json}

import scala.collection.TraversableOnce
import scala.collection.mutable.ListBuffer

/**
 * Created by jay on 14. 9. 1..
 */
object PostProcess extends JSONParser {

  private val queryLogger = Logger
  /**
   * Result Entity score field name
   */
  val SCORE_FIELD_NAME = "scoreSum"

  def groupEdgeResult(queryResultLs: Seq[QueryResult], excludeIds: Option[Map[InnerValLike, Boolean]] = None) = {
    //    filterNot {case (edge, score) => edge.props.contains(LabelMeta.degreeSeq)}
    val groupedEdgesWithRank = (for {
      queryResult <- queryResultLs
      (edge, score) <- queryResult.edgeWithScoreLs
    } yield {
        (edge, score)
      }).groupBy {
      case (edge, rank) if edge.labelWithDir.dir == GraphUtil.directions("in") =>
        (edge.label.srcColumn, edge.label.label, edge.label.tgtColumn, edge.tgtVertex.innerId, edge.props.contains(LabelMeta.degreeSeq))
      case (edge, rank) =>
        (edge.label.tgtColumn, edge.label.label, edge.label.srcColumn, edge.tgtVertex.innerId, edge.props.contains(LabelMeta.degreeSeq))
    }
    for {
      ((tgtColumn, labelName, srcColumn, target, isDegreeEdge), edgesAndRanks) <- groupedEdgesWithRank
      if !excludeIds.getOrElse(Map[InnerValLike, Boolean]()).contains(target) && !isDegreeEdge
      edgesWithRanks = edgesAndRanks.groupBy(x => x._1.srcVertex).map(_._2.head)
      id <- innerValToJsValue(target, tgtColumn.columnType)
    } yield {
      Json.obj("name" -> tgtColumn.columnName, "id" -> id,
        SCORE_FIELD_NAME -> edgesWithRanks.map(_._2).sum,
        "label" -> labelName,
        "aggr" -> Json.obj(
          "name" -> srcColumn.columnName,
          "ids" -> edgesWithRanks.flatMap { case (edge, rank) =>
            innerValToJsValue(edge.srcVertex.innerId, srcColumn.columnType)
          },
          "edges" -> edgesWithRanks.map { case (edge, rank) =>
            Json.obj("id" -> innerValToJsValue(edge.srcVertex.innerId, srcColumn.columnType),
              "props" -> propsToJson(edge),
              "score" -> rank
            )
          }
        )
      )
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

  def simple(queryResultLs: Seq[QueryResult]) = {
    val ids = resultInnerIds(queryResultLs).map(_.toString)
    val size = ids.size
    queryLogger.info(s"Result: $size")
    Json.obj("size" -> size, "results" -> ids)
    //    sortWithFormatted(ids)(false)
  }

  def resultInnerIds(queryResultLs: Seq[QueryResult], isSrcVertex: Boolean = false) = {
    for {
      queryResult <- queryResultLs
      (edge, score) <- queryResult.edgeWithScoreLs
    } yield {
      if (isSrcVertex) edge.srcVertex.innerId
      else edge.tgtVertex.innerId
    }
  }
  def summarizeWithListExcludeFormatted(exclude: Seq[QueryResult], queryResultLs: Seq[QueryResult]) = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true)
    val jsons = groupEdgeResult(queryResultLs, Some(excludeIds.toMap))
    val reverseSort = sortWithFormatted(jsons) _
    reverseSort(true)
  }

  /**
   * This method will be deprecated(because our response format will change by summarizeWithListExcludeFormatted functions' logic)
   */
  def summarizeWithListExclude(exclude: Seq[QueryResult],
                               queryResultLs: Seq[QueryResult]): JsObject = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap


    val groupedEdgesWithRank = (for {
      queryResult <- queryResultLs
      (edge, score) <- queryResult.edgeWithScoreLs if edge.props.contains(LabelMeta.degreeSeq)
    } yield {
        (edge, score)
      }).groupBy { case (edge, score) =>
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

  def summarizeWithList(edgesPerVertexWithRanks: Seq[QueryResult]) = {
    val jsons = groupEdgeResult(edgesPerVertexWithRanks)
    val reverseSort = sortWithFormatted(jsons) _
    reverseSort(true)
  }

  def summarizeWithListFormatted(edgesPerVertexWithRanks: Seq[QueryResult]) = {
    val jsons = groupEdgeResult(edgesPerVertexWithRanks)
    val reverseSort = sortWithFormatted(jsons) _
    reverseSort(true)
  }

  def noFormat(edgesPerVertex: Seq[Iterable[(Edge, Double)]]) = {
    Json.obj("edges" -> edgesPerVertex.toString)
  }


  def toSimpleVertexArrJson(queryResultLs: Seq[QueryResult]) = {
    val withScore = true
    import play.api.libs.json.Json
    val degreeJsons = ListBuffer[JsObject]()
    val degrees = ListBuffer[JsObject]()
    val edgeJsons = ListBuffer[JsObject]()

    for {
      queryResult <- queryResultLs
      (edge, score) <- queryResult.edgeWithScoreLs
    } {
      val (srcColumn, tgtColumn) = srcTgtColumn(edge, queryResult)
      val fromOpt = innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
      if (edge.propsWithTs.contains(LabelMeta.degreeSeq) && fromOpt.isDefined) {
        //          degreeJsons += edgeJson
        degrees += Json.obj(
          "from" -> fromOpt.get,
          "label" -> edge.label.label,
          LabelMeta.degree.name ->
            innerValToJsValue(edge.propsWithTs(LabelMeta.degreeSeq).innerVal, InnerVal.LONG)
        )
      } else {
        for {
          edgeJson <- edgeToJson(edge, score, queryResult)
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

  def toSiimpleVertexArrJson(exclude: Seq[QueryResult],
                             queryResultLs: Seq[QueryResult]) = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap
    val withScore = true
    import play.api.libs.json.Json
    val jsons = for {
      queryResult <- queryResultLs
      (edge, score) <- queryResult.edgeWithScoreLs
      if !excludeIds.contains(edge.tgtVertex.innerId) && !edge.props.contains(LabelMeta.degreeSeq)
      edgeJson <- edgeToJson(edge, score, queryResult)
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
  def srcTgtColumn(edge: Edge, queryResult: QueryResult) = {
    val queryParam = queryResult.queryParam
    if (edge.label.isDirected) {
      (queryParam.label.srcColumnWithDir(queryParam.labelWithDir.dir),
        queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir))
    } else {
      if (queryParam.labelWithDir.dir == GraphUtil.directions("in")) {
        (queryParam.label.tgtColumn, queryParam.label.srcColumn)
      } else {
        (queryParam.label.srcColumn, queryParam.label.tgtColumn)
      }
    }
  }
  def edgeToJson(edge: Edge, score: Double, queryResult: QueryResult): Option[JsObject] = {
    val queryParam = queryResult.queryParam
    //
    //    Logger.debug(s"edgeProps: ${edge.props} => ${props}")
    //    val shouldBeReverted = q.labelSrcTgtInvertedMap.get(edge.labelWithDir.labelId).getOrElse(false)
    //FIXME
    val (srcColumn, tgtColumn) = srcTgtColumn(edge, queryResult)
    val json = for {
      from <- innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
      to <- innerValToJsValue(edge.tgtVertex.id.innerId, tgtColumn.columnType)
    } yield {
      if (queryParam.cacheTTLInMillis > 0) {
        Json.obj(
          "cacheRemain" -> (queryParam.cacheTTLInMillis - (queryResult.timestamp - queryParam.timestamp)),
          "from" -> from,
          "to" -> to,
          "label" -> edge.label.label,
          "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
          "_timestamp" -> edge.ts,
          "props" -> propsToJson(edge),
          "score" -> score
        )
      } else {
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
