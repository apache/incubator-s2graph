package controllers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.mysqls._
import com.daumkakao.s2graph.core.types.{InnerVal, InnerValLike}
import play.api.libs.json.{Json, _}

import scala.collection.mutable.ListBuffer

object PostProcess extends JSONParser {
  /**
   * Result Entity score field name
   */
  val SCORE_FIELD_NAME = "scoreSum"
  val timeoutResults = Json.obj("size" -> 0, "results" -> Json.arr(), "isTimeout" -> true)
  val reservedColumns = Set("cacheRemain", "from", "to", "label", "direction", "_timestamp", "timestamp", "score", "props")

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

    val ret = for {
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

    ret.toList
  }

  def sortWithFormatted(jsons: Seq[JsObject], scoreField: String = "scoreSum", queryResultLs: Seq[QueryResult], decrease: Boolean = true): JsObject = {
    val ordering = if (decrease) -1 else 1
    val sortedJsons = jsons.sortBy { jsObject => (jsObject \ scoreField).as[Double] * ordering }

    if (queryResultLs.isEmpty) Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
    else Json.obj(
      "size" -> sortedJsons.size,
      "results" -> sortedJsons,
      "impressionId" -> queryResultLs.head.query.impressionId()
    )
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

  def toSimpleVertexArrJson(queryResultLs: Seq[QueryResult]): JsValue = {
    toSimpleVertexArrJson(queryResultLs, Seq.empty[QueryResult])
  }

  def toSimpleVertexArrJson(queryResultLs: Seq[QueryResult], exclude: Seq[QueryResult]): JsValue = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap
    var withScore = true
    val degrees = ListBuffer[JsValue]()
    val edgeJsons = ListBuffer[JsValue]()

    if (queryResultLs.isEmpty) {
      Json.obj("size" -> 0, "degrees" -> Json.arr(), "results" -> Json.arr())
    } else {
      val q = queryResultLs.head.query

      /** build result jsons */
      for {
        queryResult <- queryResultLs
        (edge, score) <- queryResult.edgeWithScoreLs if !excludeIds.contains(edge.tgtVertex.innerId)
      } {
        withScore = queryResult.query.withScore
        val (srcColumn, _) = srcTgtColumn(edge, queryResult.queryParam)
        val fromOpt = innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
        if (edge.propsWithTs.contains(LabelMeta.degreeSeq) && fromOpt.isDefined) {
          degrees += Json.obj(
            "from" -> fromOpt.get,
            "label" -> queryResult.queryParam.label.label,
            "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
            LabelMeta.degree.name -> innerValToJsValue(edge.propsWithTs(LabelMeta.degreeSeq).innerVal, InnerVal.LONG)
          )
        } else {
          for {
            edgeJson <- edgeToJson(edge, score, queryResult.query, queryResult.queryParam)
          } {
            edgeJsons += edgeJson
          }
        }
      }

      val edges =
        if (q.groupByColumns.isEmpty && withScore)
          edgeJsons.sortBy(js => ((js \ "score").asOpt[Double].getOrElse(0.0) * -1, (js \ "_timestamp").asOpt[Long].getOrElse(0L) * -1))
        else edgeJsons

      val resultJson =
        if (q.groupByColumns.isEmpty) {
          Json.obj("size" -> edges.size, "degrees" -> degrees, "results" -> edges, "impressionId" -> q.impressionId())
        } else {
          val grouped = edges.groupBy { jsVal =>
            for {
              column <- q.groupByColumns
              value <- (jsVal \\ column).headOption
            } yield column -> value
          }

          val groupedJsons = for {
            (groupByKeyVals, jsVals) <- grouped
          } yield {
              val scoreSum = jsVals.map { js => (js \ "score").asOpt[Double].getOrElse(0.0) }.sum
              Json.obj(
                "groupBy" -> Json.toJson(groupByKeyVals.toMap),
                "scoreSum" -> scoreSum,
                "agg" -> jsVals.sortBy(jsVal => (jsVal \ "score").asOpt[Double].getOrElse(0.0) * -1)
              )
            }

          val groupedSortedJsons = groupedJsons.toList.sortBy { jsVal => -1 * (jsVal \ "scoreSum").as[Double] }
          Json.obj(
            "size" -> groupedJsons.size,
            "degrees" -> degrees,
            "results" -> Json.toJson(groupedSortedJsons),
            "impressionId" -> q.impressionId()
          )
        }
      resultJson
    }
  }

  def verticesToJson(vertices: Iterable[Vertex]) = {
    Json.toJson(vertices.flatMap { v => vertexToJson(v) })
  }

  def propsToJson(edge: Edge, q: Query, queryParam: QueryParam): Map[String, JsValue] = {
    val kvs = for {
      (seq, labelMeta) <- queryParam.label.metaPropsMap if LabelMeta.isValidSeq(seq)
      innerVal = edge.propsWithTs.get(seq).map(_.innerVal).getOrElse {
        toInnerVal(labelMeta.defaultValue, labelMeta.dataType, queryParam.label.schemaVersion)
      }
      jsValue <- innerValToJsValue(innerVal, labelMeta.dataType) if q.selectColumnsSet.isEmpty || q.selectColumnsSet.contains(labelMeta.name)
    } yield labelMeta.name -> jsValue

    kvs
  }

  def srcTgtColumn(edge: Edge, queryParam: QueryParam) = {
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

  private def edgeParent(parentEdges: Seq[EdgeWithScore], q: Query, queryParam: QueryParam, index: Int = 0): JsValue = {
    if (q.steps.length == index+1 || parentEdges.isEmpty) {
      Json.arr()
    } else {
      val parents = for {
        parent <- parentEdges
        (parentEdge, parentScore) = (parent.edge, parent.score)
      } yield {
          val parentQueryParam = QueryParam(parentEdge.labelWithDir)
          val parents = edgeParent(parentEdge.parentEdges, q, parentQueryParam, index = index + 1)
          val edgeJson = edgeToJsonInner(parentEdge, parentScore, q, parentQueryParam).getOrElse(Map.empty[String, JsValue]) + ("parents" -> parents)

          Json.toJson(edgeJson)
        }

      Json.toJson(parents)
    }
  }

  def edgeToJsonInner(edge: Edge, score: Double, q: Query, queryParam: QueryParam): Option[Map[String, JsValue]] = {
    val (srcColumn, tgtColumn) = srcTgtColumn(edge, queryParam)

    val kvMapOpt = for {
      from <- innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
      to <- innerValToJsValue(edge.tgtVertex.id.innerId, tgtColumn.columnType)
    } yield {
        val propsMap = propsToJson(edge, q, queryParam)
        val targetColumns = if (q.selectColumnsSet.isEmpty) reservedColumns else reservedColumns & (q.selectColumnsSet) + "props"
        val kvMap = targetColumns.foldLeft(Map.empty[String, JsValue]) { (map, column) =>
          val jsValue = column match {
            case "cacheRemain" => JsNumber(queryParam.cacheTTLInMillis - (System.currentTimeMillis() - queryParam.timestamp))
            case "from" => from
            case "to" => to
            case "label" => JsString(queryParam.label.label)
            case "direction" => JsString(GraphUtil.fromDirection(edge.labelWithDir.dir))
            case "_timestamp" | "timestamp" => JsNumber(edge.ts)
            case "score" => JsNumber(score)
            case "props" if propsMap.nonEmpty => Json.toJson(propsMap)
            case _ => JsNull
          }

          if (jsValue == JsNull) map else map + (column -> jsValue)
        }
        kvMap
      }

    kvMapOpt
  }

  def edgeToJson(edge: Edge, score: Double, q: Query, queryParam: QueryParam): Option[JsValue] = {
    val kvMapOpt = edgeToJsonInner(edge, score, q, queryParam)

    for {
      kvMap <- kvMapOpt
    } yield {
      if (q.returnTree) {
        Json.toJson(kvMap + ("parents" -> Json.toJson(edgeParent(edge.parentEdges, q, queryParam))))
      } else {
        Json.toJson(kvMap)
      }
    }
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
