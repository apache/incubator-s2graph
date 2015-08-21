package controllers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.mysqls._

//import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.types2.{InnerVal, InnerValLike}
import play.api.Logger
import play.api.libs.json.{JsObject, JsValue, Json}
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

  def groupEdgeResult(queryResultLs: Seq[QueryResult], exclude: Seq[QueryResult]) = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap
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

  def sortWithFormatted(jsons: Iterable[JsObject], scoreField: String = "scoreSum", queryResultLs: Seq[QueryResult], decrease: Boolean = true): JsObject = {
    val ordering = if (decrease) -1 else 1
    var sortedJsons = jsons.toList.sortBy { jsObject => (jsObject \ scoreField).as[Double] * ordering }
    queryLogger.debug(s"sortedJsons : $sortedJsons")
    if (queryResultLs.isEmpty) Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
    else Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons,
      "impressionId" -> queryResultLs.head.query.impressionId())
  }

//  def simple(queryResultLs: Seq[QueryResult]) = {
//    val ids = resultInnerIds(queryResultLs).map(_.toString)
//    val size = ids.size
//    queryLogger.info(s"Result: $size")
//    Json.obj("size" -> size, "results" -> ids)
//    //    sortWithFormatted(ids)(false)
//  }

  def resultInnerIds(queryResultLs: Seq[QueryResult], isSrcVertex: Boolean = false) = {
    for {
      queryResult <- queryResultLs
      (edge, score) <- queryResult.edgeWithScoreLs
    } yield {
      if (isSrcVertex) edge.srcVertex.innerId
      else edge.tgtVertex.innerId
    }
  }

  def summarizeWithListExcludeFormatted(queryResultLs: Seq[QueryResult], exclude: Seq[QueryResult]) = {
    val jsons = groupEdgeResult(queryResultLs, exclude)
    sortWithFormatted(jsons, queryResultLs = queryResultLs, decrease = true)
  }


  def summarizeWithList(queryResultLs: Seq[QueryResult], exclude: Seq[QueryResult]) = {
    val jsons = groupEdgeResult(queryResultLs, exclude)
    sortWithFormatted(jsons, queryResultLs = queryResultLs)
  }

  def summarizeWithListFormatted(queryResultLs: Seq[QueryResult], exclude: Seq[QueryResult]) = {
    val jsons = groupEdgeResult(queryResultLs, exclude)
    sortWithFormatted(jsons, queryResultLs = queryResultLs)
  }

//  def noFormat(edgesPerVertex: Seq[Iterable[(Edge, Double)]]) = {
//    Json.obj("edges" -> edgesPerVertex.toString)
//  }

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
        Json.obj("size" -> results.size, "degrees" -> degrees, "results" -> results, "impressionId" -> q.impressionId())
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
            val scoreSum = jsVals.map { js => (js \ "score").asOpt[Double].getOrElse(0.0) }.sum
            Json.obj("groupBy" -> Json.toJson(groupByKeyVals.toMap),
              "scoreSum" -> scoreSum,
              "agg" -> jsVals)
          }

        val groupedSortedJsons = groupedJsons.toList.sortBy { case jsVal => -1 * (jsVal \ "scoreSum").as[Double] }
        Json.obj("size" -> groupedJsons.size, "results" -> Json.toJson(groupedSortedJsons), "impressionId" -> q.impressionId())
      }
    }
  }

  def verticesToJson(vertices: Iterable[Vertex]) = {
    Json.toJson(vertices.flatMap { v => vertexToJson(v) })
  }


  def propsToJson(edge: Edge, q: Query, queryParam: QueryParam): (JsObject, Boolean) = {
    var obj = Json.obj()
    var isEmpty = true
    for {
//    val ret = for {
      (seq, labelMeta) <- queryParam.label.metaPropsMap if LabelMeta.isValidSeq(seq)
      innerVal = edge.propsWithTs.get(seq).map(_.innerVal).getOrElse {
        toInnerVal(labelMeta.defaultValue, labelMeta.dataType, queryParam.label.schemaVersion)
      }
      jsValue <- innerValToJsValue(innerVal, labelMeta.dataType)
      if q.selectColumnsSet.isEmpty || q.selectColumnsSet.contains(labelMeta.name)
    } yield {
      isEmpty = false
        obj += (labelMeta.name -> jsValue)
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
            "timestamp" -> edge.ts,
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
            "timestamp" -> edge.ts,
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

  @deprecated(message = "deprecated", since = "0.2")
  def propsToJson(edge: Edge) = {
    for {
      (seq, v) <- edge.propsWithTs if LabelMeta.isValidSeq(seq)
      metaProp <- edge.label.metaPropsMap.get(seq)
      jsValue <- innerValToJsValue(v.innerVal, metaProp.dataType)
    } yield {
      (metaProp.name, jsValue)
    }
  }

  @deprecated(message = "deprecated", since = "0.2")
  def summarizeWithListExclude(queryResultLs: Seq[QueryResult], exclude: Seq[QueryResult]): JsObject = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap


    val groupedEdgesWithRank = (for {
      queryResult <- queryResultLs
      (edge, score) <- queryResult.edgeWithScoreLs
//      if edge.propsWithTs.contains(LabelMeta.degreeSeq)
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
    if (queryResultLs.isEmpty) {
      Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
    } else {
      Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons, "impressionId" -> queryResultLs.head.query.impressionId())
    }

  }



}
