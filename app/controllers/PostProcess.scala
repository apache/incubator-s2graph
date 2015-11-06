package controllers

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.types.{InnerVal, InnerValLike}
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
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
      if !excludeIds.contains(toHashKey(edge, queryResult.queryParam, queryResult.query.filterOutFields))
    } yield {
        (queryResult.queryParam, edge, score)
      }).groupBy {
      case (queryParam, edge, rank) if edge.labelWithDir.dir == GraphUtil.directions("in") =>
        (queryParam.label.srcColumn, queryParam.label.label, queryParam.label.tgtColumn, edge.tgtVertex.innerId, edge.propsWithTs.contains(LabelMeta.degreeSeq))
      case (queryParam, edge, rank) =>
        (queryParam.label.tgtColumn, queryParam.label.label, queryParam.label.srcColumn, edge.tgtVertex.innerId, edge.propsWithTs.contains(LabelMeta.degreeSeq))
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

  private def toHashKey(edge: Edge, queryParam: QueryParam, fields: Seq[String], delimiter: String = ","): Int = {
    val ls = for {
      field <- fields
    } yield {
        field match {
          case "from" | "_from" => edge.srcVertex.innerId
          case "to" | "_to" => edge.tgtVertex.innerId
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

  def resultInnerIds(queryResultLs: Seq[QueryResult], isSrcVertex: Boolean = false): Seq[Int] = {
    for {
      queryResult <- queryResultLs
      q = queryResult.query
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
    } yield toHashKey(edge, queryResult.queryParam, q.filterOutFields)
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
    val rawEdges = ListBuffer[(Map[String, JsValue], Double, Long)]()

    if (queryResultLs.isEmpty) {
      Json.obj("size" -> 0, "degrees" -> Json.arr(), "results" -> Json.arr())
    } else {
      val q = queryResultLs.head.query

      /** build result jsons */
      for {
        queryResult <- queryResultLs
        queryParam = queryResult.queryParam
        edgeWithScore <- queryResult.edgeWithScoreLs
        (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
        if !excludeIds.contains(toHashKey(edge, queryResult.queryParam, q.filterOutFields))
      } {
        withScore = queryResult.query.withScore
        val (srcColumn, _) = queryParam.label.srcTgtColumn(edge.labelWithDir.dir)
        val fromOpt = innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
        if (edge.propsWithTs.contains(LabelMeta.degreeSeq) && fromOpt.isDefined) {
          degrees += Json.obj(
            "from" -> fromOpt.get,
            "label" -> queryResult.queryParam.label.label,
            "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
            LabelMeta.degree.name -> innerValToJsValue(edge.propsWithTs(LabelMeta.degreeSeq).innerVal, InnerVal.LONG)
          )
        } else {
          val currentEdge = (edgeToJson(edge, score, queryResult.query, queryResult.queryParam), score, edge.ts)
          rawEdges += currentEdge
        }
      }

      if (q.groupByColumns.isEmpty) {
        val edges = {
          if (withScore) {
            rawEdges.sortBy { case (kvs, score, ts) =>
              val firstOrder = score * -1
              val secondOrder = ts * -1
              (firstOrder, secondOrder)
            }
          } else {
            rawEdges
          }
        }.map(_._1)

        Json.obj(
          "size" -> edges.size,
          "degrees" -> degrees,
          "results" -> edges,
          "impressionId" -> q.impressionId()
        )
      } else {
        val grouped = rawEdges.groupBy { case (keyWithJs, score, ts) =>
          val props = keyWithJs.get("props")

          for {
            column <- q.groupByColumns
            value <- keyWithJs.get(column) match {
              case None => props.flatMap { js => (js \ column).asOpt[JsValue] }
              case Some(x) => Some(x)
            }
          } yield column -> value
        }

        val groupedEdges = {
          for {
            (groupByKeyVals, edges) <- grouped
          } yield {
            val scoreSum = edges.map(x => x._2).sum
            Json.obj(
              "groupBy" -> Json.toJson(groupByKeyVals.toMap),
              "scoreSum" -> scoreSum,
              "agg" -> edges.sortBy { case (kvs, score, ts) =>
                val firstOrder = score * -1
                val secondOrder = ts * -1
                (firstOrder, secondOrder)
              }.map(_._1)
            )
          }
        }

        val groupedSortedJsons = groupedEdges.toList.sortBy { jsVal => -1 * (jsVal \ "scoreSum").as[Double] }
        Json.obj(
          "size" -> groupedEdges.size,
          "degrees" -> degrees,
          "results" -> groupedSortedJsons,
          "impressionId" -> q.impressionId()
        )
      }
    }
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
  def summarizeWithListExclude(queryResultLs: Seq[QueryResult], exclude: Seq[QueryResult]): JsObject = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap


    val groupedEdgesWithRank = (for {
      queryResult <- queryResultLs
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
      if !excludeIds.contains(toHashKey(edge, queryResult.queryParam, queryResult.query.filterOutFields))
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
    if (queryResultLs.isEmpty) {
      Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
    } else {
      Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons, "impressionId" -> queryResultLs.head.query.impressionId())
    }

  }


}
