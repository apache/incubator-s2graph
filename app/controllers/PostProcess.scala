package controllers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.mysqls._

//import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.types2.{InnerVal, InnerValLike}
import play.api.Logger
import play.api.libs.json.{JsObject, JsValue, Json}

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
  val timeoutResults = Json.obj("size" -> 0, "results" -> Json.arr(), "isTimeout" -> true)

  def groupEdgeResult(queryResultLs: Seq[QueryResult], excludeIds: Map[InnerValLike, Boolean] = Map.empty) = {
    //    filterNot {case (edge, score) => edge.props.contains(LabelMeta.degreeSeq)}
    val groupedEdgesWithRank = (for {
      queryResult <- queryResultLs
      (edge, score) <- queryResult.edgeWithScoreLs
    } yield {
        (queryResult.queryParam, edge, score)
      }).groupBy {
    case (queryParam, edge, rank) if edge.labelWithDir.dir == GraphUtil.directions("in") =>
      (queryParam.label.srcColumn, queryParam.label.label, queryParam.label.tgtColumn, edge.tgtVertex.innerId, edge.propsWithTs.contains(LabelMeta.degreeSeq))
    case (queryParam, edge, rank) =>
      (queryParam.label.tgtColumn, queryParam.label.label, queryParam.label.srcColumn, edge.tgtVertex.innerId, edge.propsWithTs.contains(LabelMeta.degreeSeq))
    }
    for {
      ((tgtColumn, labelName, srcColumn, target, isDegreeEdge), edgesAndRanks) <- groupedEdgesWithRank
      if !excludeIds.contains(target) && !isDegreeEdge
      edgesWithRanks = edgesAndRanks.groupBy(x => x._2.srcVertex).map(_._2.head)
      id <- innerValToJsValue(target, tgtColumn.columnType)
    } yield {
      Json.obj("name" -> tgtColumn.columnName, "id" -> id,
        SCORE_FIELD_NAME -> edgesWithRanks.map(_._3).sum,
        "label" -> labelName,
        "aggr" -> Json.obj(
          "name" -> srcColumn.columnName,
          "ids" -> edgesWithRanks.flatMap { case (queryParam, edge, rank) =>
            innerValToJsValue(edge.srcVertex.innerId, srcColumn.columnType)
          },
          "edges" -> edgesWithRanks.map { case (queryParam, edge, rank) =>
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
    val jsons = groupEdgeResult(queryResultLs, excludeIds.toMap)
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
      (edge, score) <- queryResult.edgeWithScoreLs if edge.propsWithTs.contains(LabelMeta.degreeSeq)
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

  def toSimpleVertexArrJson(queryResultLs: Seq[QueryResult]): JsValue = {
    toSimpleVertexArrJson(queryResultLs, Seq.empty[QueryResult])
  }

  def toSimpleVertexArrJson(queryResultLs: Seq[QueryResult], exclude: Seq[QueryResult]): JsValue = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap
    val withScore = true
    import play.api.libs.json.Json
    val degreeJsons = ListBuffer[JsValue]()
    val degrees = ListBuffer[JsValue]()
    val edgeJsons = ListBuffer[JsValue]()
    if (queryResultLs.isEmpty) {
      Json.obj("size" -> 0, "degrees" -> Json.arr(), "results" -> Json.arr())
    } else {
      val q = queryResultLs.head.query
      if (q.groupByColumns.isEmpty) {
        for {
          queryResult <- queryResultLs
          (edge, score) <- queryResult.edgeWithScoreLs if !excludeIds.contains(edge.tgtVertex.innerId)
        } {
          val (srcColumn, tgtColumn) = srcTgtColumn(edge, queryResult)
          val fromOpt = innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
          if (edge.propsWithTs.contains(LabelMeta.degreeSeq) && fromOpt.isDefined) {
            //          degreeJsons += edgeJson
            degrees += Json.obj(
              "from" -> fromOpt.get,
              "label" -> queryResult.queryParam.label.label,
              "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
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

        //        val results =
        //          degreeJsons ++ edgeJsons.toList
        val results =
          if (withScore) {
            degreeJsons ++ edgeJsons.sortBy(js => ((js \ "score").asOpt[Double].getOrElse(0.0) * -1, (js \ "_timestamp").asOpt[Long].getOrElse(0L) * -1))
          } else {
            degreeJsons ++ edgeJsons.toList
          }

        queryLogger.info(s"Result: ${results.size}")
        Json.obj("size" -> results.size, "degrees" -> degrees, "results" -> results)
      } else {
        for {
          queryResult <- queryResultLs
          (edge, score) <- queryResult.edgeWithScoreLs if !excludeIds.contains(edge.tgtVertex.innerId)
        } {
          val (srcColumn, tgtColumn) = srcTgtColumn(edge, queryResult)
          val fromOpt = innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
          if (edge.propsWithTs.contains(LabelMeta.degreeSeq) && fromOpt.isDefined) {
            //          degreeJsons += edgeJson
            degrees += Json.obj(
              "from" -> fromOpt.get,
              "label" -> queryResult.queryParam.label.label,
              "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
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
            degreeJsons ++ edgeJsons.sortBy(js => ((js \ "score").asOpt[Double].getOrElse(0.0), (js \ "_timestamp").asOpt[Long].getOrElse(0L))).reverse
          } else {
            degreeJsons ++ edgeJsons.toList
          }

        val grouped = results.groupBy { jsVal =>
          for {
            column <- q.groupByColumns
            value <- (jsVal \ column).asOpt[JsValue]
          } yield {
            (column -> value)
          }
        }
        val groupedJsons = for {
          (groupByKeyVals, jsVals) <- grouped
        } yield {
            Json.obj("groupBy" -> Json.toJson(groupByKeyVals.toMap),
              "agg" -> jsVals)
          }
        Json.toJson(groupedJsons)
      }
    }
  }

  def verticesToJson(vertices: Iterable[Vertex]) = {
    Json.toJson(vertices.flatMap { v => vertexToJson(v) })
  }

  def propsToJson(edge: Edge) = {
    for {
      (seq, v) <- edge.propsWithTs if seq >= 0
      metaProp <- edge.label.metaPropsMap.get(seq)
      jsValue <- innerValToJsValue(v.innerVal, metaProp.dataType)
    } yield {
      (metaProp.name, jsValue)
    }
  }

  def propsToJson(edge: Edge, q: Query, queryParam: QueryParam): (JsObject, Boolean) = {
    var obj = Json.obj()
    var isEmpty = true
    for {
//    val ret = for {
      (seq, v) <- edge.propsWithTs if seq >= 0
      metaProp <- queryParam.label.metaPropsMap.get(seq)
      jsValue <- innerValToJsValue(v.innerVal, metaProp.dataType)
      if q.selectColumnsSet.isEmpty || q.selectColumnsSet.contains(metaProp.name)
    } yield {
      isEmpty = false
        obj += (metaProp.name -> jsValue)
//        (metaProp.name, jsValue)
      }
//    Logger.debug(s"$ret")
//    ret
    (obj, isEmpty)
  }

  def srcTgtColumn(edge: Edge, queryResult: QueryResult) = {
    val queryParam = queryResult.queryParam
    if (queryParam.label.isDirected) {
      (queryParam.srcColumnWithDir, queryParam.tgtColumnWithDir)
    } else {
      if (queryParam.labelWithDir.dir == GraphUtil.directions("in")) {
        (queryParam.label.tgtColumn, queryParam.label.srcColumn)
      } else {
        (queryParam.label.srcColumn, queryParam.label.tgtColumn)
      }
    }
  }

  def edgeToJson(edge: Edge, score: Double, queryResult: QueryResult): Option[JsValue] = {
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
        val q = queryResult.query
        val (propsMap, isEmpty) = propsToJson(edge, queryResult.query, queryResult.queryParam)
        val results = if (isEmpty) {
          Json.obj(
            "cacheRemain" -> (queryParam.cacheTTLInMillis - (queryResult.timestamp - queryParam.timestamp)),
            "from" -> from,
            "to" -> to,
            "label" -> queryParam.label.label,
            "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
            "_timestamp" -> edge.ts,
            "score" -> score
          )
        } else {
          Json.obj(
            "cacheRemain" -> (queryParam.cacheTTLInMillis - (queryResult.timestamp - queryParam.timestamp)),
            "from" -> from,
            "to" -> to,
            "label" -> queryParam.label.label,
            "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
            "_timestamp" -> edge.ts,
            "score" -> score,
            "props" -> propsMap
          )
        }

        val filterColumns = q.selectColumnsSet
        var resultJson = Json.obj()
        for {
          (k, v) <- results.fields if (k == "props" || q.selectColumnSetIsEmpty || filterColumns.contains(k))
        } {
          resultJson += (k -> v)
        }
        resultJson

        //      if (queryParam.cacheTTLInMillis > 0) {
        //        val obj = new mutable.HashMap[String, play.api.libs.json.Json.JsValueWrapper]()
        //        obj += ("cacheRemain" -> (queryParam.cacheTTLInMillis - (queryResult.timestamp - queryParam.timestamp)))
        //        if (q.selectColumnsSet.isEmpty || q.selectColumnsSet.contains(LabelMeta.from.name)) {
        //          obj += ("from" -> from)
        //        }
        //        if (q.selectColumnsSet.isEmpty || q.selectColumnsSet.contains(LabelMeta.to.name)) {
        //          obj += ("to" -> to)
        //        }
        //        if (q.selectColumnsSet.isEmpty || q.selectColumnsSet.contains("label")) {
        //          obj += ("label" -> edge.label.label)
        //        }
        //        if (q.selectColumnsSet.isEmpty || q.selectColumnsSet.contains("direction")) {
        //          obj += ("direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir))
        //        }
        //        if (q.selectColumnsSet.isEmpty || q.selectColumnsSet.contains("_timestamp")) {
        //          obj += ("_timestamp" -> edge.ts)
        //        }
        //        if (q.selectColumnsSet.isEmpty || q.selectColumnsSet.contains("score")) {
        //          obj += ("score" -> score)
        //        }
        //
        //        Json.obj(
        //          "cacheRemain" -> (queryParam.cacheTTLInMillis - (queryResult.timestamp - queryParam.timestamp)),
        //          "from" -> from,
        //          "to" -> to,
        //          "label" -> edge.label.label,
        //          "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
        //          "_timestamp" -> edge.ts,
        //          "props" -> propsToJson(edge, queryResult.query),
        //          "score" -> score
        //        )
        //      } else {
        //        Json.obj(
        //          "from" -> from,
        //          "to" -> to,
        //          "label" -> edge.label.label,
        //          "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
        //          "_timestamp" -> edge.ts,
        //          "props" -> propsToJson(edge, queryResult.query),
        //          "score" -> score
        //        )
        //      }
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
