package com.kakao.s2graph.core

import com.kakao.s2graph.core.GraphExceptions.BadQueryException

import com.kakao.s2graph.core.mysqls.{ColumnMeta, Label, ServiceColumn, LabelMeta}
import com.kakao.s2graph.core.types.{InnerVal, InnerValLike}
import com.kakao.s2graph.core.utils.logger
import play.api.libs.json.{Json, _}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object PostProcess extends JSONParser {


  type EDGE_VALUES = Map[String, JsValue]
  type ORDER_BY_VALUES =  (Any, Any, Any, Any)
  type RAW_EDGE = (EDGE_VALUES, Double, ORDER_BY_VALUES)

  /**
   * Result Entity score field name
   */
  val emptyDegrees = Seq.empty[JsValue]
  val timeoutResults = Json.obj("size" -> 0, "degrees" -> Json.arr(), "results" -> Json.arr(), "isTimeout" -> true)
  val emptyResults = Json.obj("size" -> 0, "degrees" -> Json.arr(), "results" -> Json.arr(), "isEmpty" -> true)
  def badRequestResults(ex: => Exception) = ex match {
    case ex: BadQueryException => Json.obj("message" -> ex.msg)
    case _ => Json.obj("message" -> ex.getMessage)
  }

  val SCORE_FIELD_NAME = "scoreSum"
  val reservedColumns = Set("cacheRemain", "from", "to", "label", "direction", "_timestamp", "timestamp", "score", "props")

  def groupEdgeResult(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]) = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap
    //    filterNot {case (edge, score) => edge.props.contains(LabelMeta.degreeSeq)}
    val groupedEdgesWithRank = (for {
      queryRequestWithResult <- queryRequestWithResultLs
      (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
      if !excludeIds.contains(toHashKey(edge, queryRequest.queryParam, queryRequest.query.filterOutFields))
    } yield {
      (queryRequest.queryParam, edge, score)
    }).groupBy {
      case (queryParam, edge, rank) if edge.labelWithDir.dir == GraphUtil.directions("in") =>
        (queryParam.label.srcColumn, queryParam.label.label, queryParam.label.tgtColumn, edge.tgtVertex.innerId, edge.isDegree)
      case (queryParam, edge, rank) =>
        (queryParam.label.tgtColumn, queryParam.label.label, queryParam.label.srcColumn, edge.tgtVertex.innerId, edge.isDegree)
    }

    val ret = for {
      ((tgtColumn, labelName, srcColumn, target, isDegreeEdge), edgesAndRanks) <- groupedEdgesWithRank if !isDegreeEdge
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

  def sortWithFormatted(jsons: Seq[JsObject],
                        scoreField: String = "scoreSum",
                        queryRequestWithResultLs: Seq[QueryRequestWithResult],
                        decrease: Boolean = true): JsObject = {
    val ordering = if (decrease) -1 else 1
    val sortedJsons = jsons.sortBy { jsObject => (jsObject \ scoreField).as[Double] * ordering }

    if (queryRequestWithResultLs.isEmpty) Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
    else Json.obj(
      "size" -> sortedJsons.size,
      "results" -> sortedJsons,
      "impressionId" -> queryRequestWithResultLs.head.queryRequest.query.impressionId()
    )
  }

  private def toHashKey(edge: Edge, queryParam: QueryParam, fields: Seq[String], delimiter: String = ","): Int = {
    val ls = for {
      field <- fields
    } yield {
      field match {
        case "from" | "_from" => edge.srcVertex.innerId.toIdString()
        case "to" | "_to" => edge.tgtVertex.innerId.toIdString()
        case "label" => edge.labelWithDir.labelId
        case "direction" => JsString(GraphUtil.fromDirection(edge.labelWithDir.dir))
        case "_timestamp" | "timestamp" => edge.ts
        case _ =>
          queryParam.label.metaPropsInvMap.get(field) match {
            case None => throw new RuntimeException(s"unknow column: $field")
            case Some(labelMeta) => edge.propsWithTs.get(labelMeta.seq) match {
              case None => labelMeta.defaultValue
              case Some(propVal) => propVal
            }
          }
      }
    }
    val ret = ls.hashCode()
    ret
  }

  def resultInnerIds(queryRequestWithResultLs: Seq[QueryRequestWithResult], isSrcVertex: Boolean = false): Seq[Int] = {
    for {
      queryRequestWithResult <- queryRequestWithResultLs
      (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      q = queryRequest.query
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
    } yield  toHashKey(edge, queryRequest.queryParam, q.filterOutFields)
  }

  def summarizeWithListExcludeFormatted(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]) = {
    val jsons = groupEdgeResult(queryRequestWithResultLs, exclude)
    sortWithFormatted(jsons, queryRequestWithResultLs = queryRequestWithResultLs, decrease = true)
  }

  def summarizeWithList(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]) = {
    val jsons = groupEdgeResult(queryRequestWithResultLs, exclude)
    sortWithFormatted(jsons, queryRequestWithResultLs = queryRequestWithResultLs)
  }

  def summarizeWithListFormatted(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]) = {
    val jsons = groupEdgeResult(queryRequestWithResultLs, exclude)
    sortWithFormatted(jsons, queryRequestWithResultLs = queryRequestWithResultLs)
  }

  def toSimpleVertexArrJson(queryRequestWithResultLs: Seq[QueryRequestWithResult]): JsValue = {
    toSimpleVertexArrJson(queryRequestWithResultLs, Seq.empty[QueryRequestWithResult])
  }

  private def orderBy(queryOption: QueryOption,
                      rawEdges: ListBuffer[(EDGE_VALUES, Double, ORDER_BY_VALUES)]): ListBuffer[(EDGE_VALUES, Double, ORDER_BY_VALUES)] = {
    import com.kakao.s2graph.core.OrderingUtil._

    if (queryOption.withScore && queryOption.orderByColumns.nonEmpty) {
      val ascendingLs = queryOption.orderByColumns.map(_._2)
      rawEdges.sortBy(_._3)(TupleMultiOrdering[Any](ascendingLs))
    } else {
      rawEdges
    }
  }

  private def getColumnValue(keyWithJs: Map[String, JsValue], score: Double, edge: Edge, column: String): Any = {
    column match {
      case "score" => score
      case "timestamp" | "_timestamp" => edge.ts
      case _ =>
        keyWithJs.get(column) match {
          case None => keyWithJs.get("props").map { js => (js \ column).as[JsValue] }.get
          case Some(x) => x
        }
    }
  }

  private def buildReplaceJson(jsValue: JsValue)(mapper: JsValue => JsValue): JsValue = {
    def traverse(js: JsValue): JsValue = js match {
      case JsNull => mapper(JsNull)
      case JsUndefined() => mapper(JsUndefined(""))
      case JsNumber(v) => mapper(js)
      case JsString(v) => mapper(js)
      case JsBoolean(v) => mapper(js)
      case JsArray(elements) => JsArray(elements.map { t => traverse(mapper(t)) })
      case JsObject(values) => JsObject(values.map { case (k, v) => k -> traverse(mapper(v)) })
    }

    traverse(jsValue)
  }

  /** test query with filterOut is not working since it can not diffrentate filterOut */
  private def buildNextQuery(jsonQuery: JsValue, _cursors: Seq[Seq[String]]): JsValue = {
    val cursors = _cursors.flatten.iterator

    buildReplaceJson(jsonQuery) {
      case js@JsObject(fields) =>
        val isStep = fields.find { case (k, _) => k == "label" } // find label group
        if (isStep.isEmpty) js
        else {
          // TODO: Order not ensured
          val withCursor = js.fieldSet | Set("cursor" -> JsString(cursors.next))
          JsObject(withCursor.toSeq)
        }
      case js => js
    }
  }

  private def buildRawEdges(queryOption: QueryOption,
                            queryRequestWithResultLs: Seq[QueryRequestWithResult],
                            excludeIds: Map[Int, Boolean],
                            scoreWeight: Double = 1.0): (ListBuffer[JsValue], ListBuffer[RAW_EDGE]) = {
    val degrees = ListBuffer[JsValue]()
    val rawEdges = ListBuffer[(EDGE_VALUES, Double, ORDER_BY_VALUES)]()
    val metaPropNamesMap = scala.collection.mutable.Map[String, Int]()
    for {
      queryRequestWithResult <- queryRequestWithResultLs
    } yield {
      val (queryRequest, _) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      queryRequest.queryParam.label.metaPropNames.foreach { metaPropName =>
        metaPropNamesMap.put(metaPropName, metaPropNamesMap.getOrElse(metaPropName, 0) + 1)
      }
    }
    val propsExistInAll = metaPropNamesMap.filter(kv => kv._2 == queryRequestWithResultLs.length)
    val orderByColumns = queryOption.orderByColumns.filter { case (column, _) =>
      column match {
        case "from" | "to" | "label" | "score" | "timestamp" | "_timestamp" => true
        case _ =>
          propsExistInAll.contains(column)
//          //TODO??
//          false
//          queryParam.label.metaPropNames.contains(column)
      }
    }

    /** build result jsons */
    for {
      queryRequestWithResult <- queryRequestWithResultLs
      (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      queryParam = queryRequest.queryParam
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
      hashKey = toHashKey(edge, queryRequest.queryParam, queryOption.filterOutFields)
      if !excludeIds.contains(hashKey)
    } {

      // edge to json
      val (srcColumn, _) = queryParam.label.srcTgtColumn(edge.labelWithDir.dir)
      val fromOpt = innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
      if (edge.isDegree && fromOpt.isDefined) {
        if (queryOption.limitOpt.isEmpty) {
          degrees += Json.obj(
            "from" -> fromOpt.get,
            "label" -> queryRequest.queryParam.label.label,
            "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
            LabelMeta.degree.name -> innerValToJsValue(edge.propsWithTs(LabelMeta.degreeSeq).innerVal, InnerVal.LONG)
          )
        }
      } else {
        val keyWithJs = edgeToJson(edge, score, queryRequest.query, queryRequest.queryParam)
        val orderByValues: (Any, Any, Any, Any) = orderByColumns.length match {
          case 0 =>
            (None, None, None, None)
          case 1 =>
            val it = orderByColumns.iterator
            val v1 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            (v1, None, None, None)
          case 2 =>
            val it = orderByColumns.iterator
            val v1 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            val v2 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            (v1, v2, None, None)
          case 3 =>
            val it = orderByColumns.iterator
            val v1 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            val v2 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            val v3 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            (v1, v2, v3, None)
          case _ =>
            val it = orderByColumns.iterator
            val v1 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            val v2 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            val v3 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            val v4 = getColumnValue(keyWithJs, score, edge, it.next()._1)
            (v1, v2, v3, v4)
        }

        val currentEdge = (keyWithJs, score * scoreWeight, orderByValues)
        rawEdges += currentEdge
      }
    }
    (degrees, rawEdges)
  }

  private def buildResultJsValue(queryOption: QueryOption,
                                 degrees: ListBuffer[JsValue],
                                 rawEdges: ListBuffer[RAW_EDGE]): JsValue = {
    if (queryOption.groupByColumns.isEmpty) {
      // ordering
      val filteredEdges = rawEdges.filter(t => t._2 >= queryOption.scoreThreshold)

      val edges = queryOption.limitOpt match {
        case None => orderBy(queryOption, filteredEdges).map(_._1)
        case Some(limit) => orderBy(queryOption, filteredEdges).map(_._1).take(limit)
      }
      val resultDegrees = if (queryOption.returnDegree) degrees else emptyDegrees

      Json.obj(
        "size" -> edges.size,
        "degrees" -> resultDegrees,
//        "queryNext" -> buildNextQuery(q.jsonQuery, q.cursorStrings()),
        "results" -> edges
      )
    } else {
      val grouped = rawEdges.groupBy { case (keyWithJs, _, _) =>
        val props = keyWithJs.get("props")

        for {
          column <- queryOption.groupByColumns
          value <- keyWithJs.get(column) match {
            case None => props.flatMap { js => (js \ column).asOpt[JsValue] }
            case Some(x) => Some(x)
          }
        } yield column -> value
      }

      val groupedEdgesWithScoreSum =
        for {
          (groupByKeyVals, groupedRawEdges) <- grouped
          scoreSum = groupedRawEdges.map(x => x._2).sum if scoreSum >= queryOption.scoreThreshold
        } yield {
          // ordering
          val edges = orderBy(queryOption, groupedRawEdges).map(_._1)

          //TODO: refactor this
          val js = if (queryOption.returnAgg)
            Json.obj(
              "groupBy" -> Json.toJson(groupByKeyVals.toMap),
              "scoreSum" -> scoreSum,
              "agg" -> edges
            )
          else
            Json.obj(
              "groupBy" -> Json.toJson(groupByKeyVals.toMap),
              "scoreSum" -> scoreSum,
              "agg" -> Json.arr()
            )
          (js, scoreSum)
        }

      val groupedSortedJsons = queryOption.limitOpt match {
        case None =>
          groupedEdgesWithScoreSum.toList.sortBy { case (jsVal, scoreSum) => scoreSum * -1 }.map(_._1)
        case Some(limit) =>
          groupedEdgesWithScoreSum.toList.sortBy { case (jsVal, scoreSum) => scoreSum * -1 }.map(_._1).take(limit)
      }
      val resultDegrees = if (queryOption.returnDegree) degrees else emptyDegrees
      Json.obj(
        "size" -> groupedSortedJsons.size,
        "degrees" -> resultDegrees,
//        "queryNext" -> buildNextQuery(q.jsonQuery, q.cursorStrings()),
        "results" -> groupedSortedJsons
      )
    }
  }

  def toSimpleVertexArrJsonMulti(queryOption: QueryOption,
                                 resultWithExcludeLs: Seq[(Seq[QueryRequestWithResult], Seq[QueryRequestWithResult])],
                                 excludes: Seq[QueryRequestWithResult]): JsValue = {
    val excludeIds = (Seq((Seq.empty, excludes)) ++ resultWithExcludeLs).foldLeft(Map.empty[Int, Boolean]) { case (acc, (result, excludes)) =>
      acc ++ resultInnerIds(excludes).map(hashKey => hashKey -> true).toMap
    }

    val (degrees, rawEdges) = (ListBuffer.empty[JsValue], ListBuffer.empty[RAW_EDGE])
    for {
      (result, localExclude) <- resultWithExcludeLs
    } {
      val newResult = result.map { queryRequestWithResult =>
        val (queryRequest, _) = QueryRequestWithResult.unapply(queryRequestWithResult).get
        val newQuery = queryRequest.query.copy(queryOption = queryOption)
        queryRequestWithResult.copy(queryRequest = queryRequest.copy(query = newQuery))
      }
      val (_degrees, _rawEdges) = buildRawEdges(queryOption, newResult, excludeIds)
      degrees ++= _degrees
      rawEdges ++= _rawEdges
    }
    buildResultJsValue(queryOption, degrees, rawEdges)
  }

  def toSimpleVertexArrJson(queryOption: QueryOption,
                            queryRequestWithResultLs: Seq[QueryRequestWithResult],
                            exclude: Seq[QueryRequestWithResult]): JsValue = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap
    val (degrees, rawEdges) = buildRawEdges(queryOption, queryRequestWithResultLs, excludeIds)
    buildResultJsValue(queryOption, degrees, rawEdges)
  }


  def toSimpleVertexArrJson(queryRequestWithResultLs: Seq[QueryRequestWithResult],
                            exclude: Seq[QueryRequestWithResult]): JsValue = {

    queryRequestWithResultLs.headOption.map { queryRequestWithResult =>
      val (queryRequest, _) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      val query = queryRequest.query
      val queryOption = query.queryOption
      val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap
      val (degrees, rawEdges) = buildRawEdges(queryOption, queryRequestWithResultLs, excludeIds)
      buildResultJsValue(queryOption, degrees, rawEdges)
    } getOrElse emptyResults
  }

  def verticesToJson(vertices: Iterable[Vertex]) = {
    Json.toJson(vertices.flatMap { v => vertexToJson(v) })
  }

  def propsToJson(edge: Edge, q: Query, queryParam: QueryParam): Map[String, JsValue] = {
    for {
      (seq, innerValWithTs) <- edge.propsWithTs if LabelMeta.isValidSeq(seq)
      labelMeta <- queryParam.label.metaPropsMap.get(seq)
      jsValue <- innerValToJsValue(innerValWithTs.innerVal, labelMeta.dataType)
    } yield labelMeta.name -> jsValue
  }

  private def edgeParent(parentEdges: Seq[EdgeWithScore], q: Query, queryParam: QueryParam): JsValue = {
    if (parentEdges.isEmpty) {
      JsNull
    } else {
      val parents = for {
        parent <- parentEdges
        (parentEdge, parentScore) = EdgeWithScore.unapply(parent).get
        parentQueryParam = QueryParam(parentEdge.labelWithDir)
        parents = edgeParent(parentEdge.parentEdges, q, parentQueryParam) if parents != JsNull
      } yield {
        val originalEdge = parentEdge.originalEdgeOpt.getOrElse(parentEdge)
        val edgeJson = edgeToJsonInner(originalEdge, parentScore, q, parentQueryParam) + ("parents" -> parents)
        Json.toJson(edgeJson)
      }

      Json.toJson(parents)
    }
  }

  /** TODO */
  def edgeToJsonInner(edge: Edge, score: Double, q: Query, queryParam: QueryParam): Map[String, JsValue] = {
    val (srcColumn, tgtColumn) = queryParam.label.srcTgtColumn(edge.labelWithDir.dir)

    val kvMapOpt = for {
      from <- innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
      to <- innerValToJsValue(edge.tgtVertex.id.innerId, tgtColumn.columnType)
    } yield {
      val targetColumns = if (q.selectColumnsSet.isEmpty) reservedColumns else (reservedColumns & q.selectColumnsSet) + "props"
      val _propsMap = queryParam.label.metaPropsDefaultMapInner ++ propsToJson(edge, q, queryParam)
      val propsMap = if (q.selectColumnsSet.nonEmpty) _propsMap.filterKeys(q.selectColumnsSet) else _propsMap

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

    kvMapOpt.getOrElse(Map.empty)
  }

  def edgeToJson(edge: Edge, score: Double, q: Query, queryParam: QueryParam): Map[String, JsValue] = {
    val kvs = edgeToJsonInner(edge, score, q, queryParam)
    if (kvs.nonEmpty && q.returnTree) kvs + ("parents" -> Json.toJson(edgeParent(edge.parentEdges, q, queryParam)))
    else kvs
  }

  def vertexToJson(vertex: Vertex): Option[JsObject] = {
    val serviceColumn = ServiceColumn.findById(vertex.id.colId)

    for {
      id <- innerValToJsValue(vertex.innerId, serviceColumn.columnType)
    } yield {
      Json.obj("serviceName" -> serviceColumn.service.serviceName,
        "columnName" -> serviceColumn.columnName,
        "id" -> id, "props" -> propsToJson(vertex),
        "timestamp" -> vertex.ts,
        //        "belongsTo" -> vertex.belongLabelIds)
        "belongsTo" -> vertex.belongLabelIds.flatMap(Label.findByIdOpt(_).map(_.label)))
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
  def summarizeWithListExclude(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]): JsObject = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap


    val groupedEdgesWithRank = (for {
      queryRequestWithResult <- queryRequestWithResultLs
      (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
      if !excludeIds.contains(toHashKey(edge, queryRequest.queryParam, queryRequest.query.filterOutFields))
    } yield {
      (edge, score)
    }).groupBy { case (edge, score) =>
      (edge.label.tgtColumn, edge.label.srcColumn, edge.tgtVertex.innerId)
    }

    val jsons = for {
      ((tgtColumn, srcColumn, target), edgesAndRanks) <- groupedEdgesWithRank
      (edges, ranks) = edgesAndRanks.groupBy(x => x._1.srcVertex).map(_._2.head).unzip
      tgtId <- innerValToJsValue(target, tgtColumn.columnType)
    } yield {
      Json.obj(tgtColumn.columnName -> tgtId,
        s"${srcColumn.columnName}s" ->
          edges.flatMap(edge => innerValToJsValue(edge.srcVertex.innerId, srcColumn.columnType)), "scoreSum" -> ranks.sum)
    }
    val sortedJsons = jsons.toList.sortBy { jsObj => (jsObj \ "scoreSum").as[Double] }.reverse
    if (queryRequestWithResultLs.isEmpty) {
      Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
    } else {
      Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons,
        "impressionId" -> queryRequestWithResultLs.head.queryRequest.query.impressionId())
    }

  }


}
