package com.kakao.s2graph.core.rest

import com.kakao.s2graph.core.GraphExceptions.{BadQueryException, ModelNotFoundException}
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.parsers.WhereParser
import com.kakao.s2graph.core.types._
import com.typesafe.config.Config

import play.api.libs.json._

import scala.util.{Failure, Success, Try}

class RequestParser(config: Config) extends JSONParser {

  import Management.JsonModel._

  val hardLimit = 100000
  val defaultLimit = 100
  val DefaultRpcTimeout = config.getInt("hbase.rpc.timeout")
  val DefaultMaxAttempt = config.getInt("hbase.client.retries.number")
  val DefaultCluster = config.getString("hbase.zookeeper.quorum")
  val DefaultCompressionAlgorithm = config.getString("hbase.table.compression.algorithm")
  val DefaultPhase = config.getString("phase")

  private def extractScoring(labelId: Int, value: JsValue) = {
    val ret = for {
      js <- parse[Option[JsObject]](value, "scoring")
    } yield {
      for {
        (k, v) <- js.fields
        labelOrderType <- LabelMeta.findByName(labelId, k)
      } yield {
        val value = v match {
          case n: JsNumber => n.as[Double]
          case _ => throw new Exception("scoring weight should be double.")
        }
        (labelOrderType.seq, value)
      }
    }
    ret
  }

  def extractInterval(label: Label, jsValue: JsValue) = {
    def extractKv(js: JsValue) = js match {
      case JsObject(obj) => obj
      case JsArray(arr) => arr.flatMap {
        case JsObject(obj) => obj
        case _ => throw new RuntimeException(s"cannot support json type $js")
      }
      case _ => throw new RuntimeException(s"cannot support json type: $js")
    }

    val ret = for {
      js <- parse[Option[JsObject]](jsValue, "interval")
      fromJs <- (js \ "from").asOpt[JsValue]
      toJs <- (js \ "to").asOpt[JsValue]
    } yield {
      val from = Management.toProps(label, extractKv(fromJs))
      val to = Management.toProps(label, extractKv(toJs))
      (from, to)
    }

    ret
  }

  def extractDuration(label: Label, jsValue: JsValue) = {
    for {
      js <- parse[Option[JsObject]](jsValue, "duration")
    } yield {
      val minTs = parse[Option[Long]](js, "from").getOrElse(Long.MaxValue)
      val maxTs = parse[Option[Long]](js, "to").getOrElse(Long.MinValue)

      (minTs, maxTs)
    }
  }

  def extractHas(label: Label, jsValue: JsValue) = {
    val ret = for {
      js <- parse[Option[JsObject]](jsValue, "has")
    } yield {
      for {
        (k, v) <- js.fields
        labelMeta <- LabelMeta.findByName(label.id.get, k)
        value <- jsValueToInnerVal(v, labelMeta.dataType, label.schemaVersion)
      } yield {
        labelMeta.seq -> value
      }
    }
    ret.map(_.toMap).getOrElse(Map.empty[Byte, InnerValLike])
  }

  def extractWhere(labelMap: Map[String, Label], jsValue: JsValue) = {
    (jsValue \ "where").asOpt[String] match {
      case None => Success(WhereParser.success)
      case Some(where) =>
        WhereParser(labelMap).parse(where) match {
          case s@Success(_) => s
          case Failure(ex) => throw BadQueryException(ex.getMessage, ex)
        }
    }
  }

  def toVertices(labelName: String, direction: String, ids: Seq[JsValue]): Seq[Vertex] = {
    val vertices = for {
      label <- Label.findByName(labelName).toSeq
      serviceColumn = if (direction == "out") label.srcColumn else label.tgtColumn
      id <- ids
      innerId <- jsValueToInnerVal(id, serviceColumn.columnType, label.schemaVersion)
    } yield {
      Vertex(SourceVertexId(serviceColumn.id.get, innerId), System.currentTimeMillis())
    }
    vertices.toSeq
  }

  def toQuery(jsValue: JsValue, isEdgeQuery: Boolean = true): Query = {
    try {
      val vertices =
        (for {
          value <- parse[List[JsValue]](jsValue, "srcVertices")
          serviceName = parse[String](value, "serviceName")
          column = parse[String](value, "columnName")
        } yield {
          val service = Service.findByName(serviceName).getOrElse(throw BadQueryException("service not found"))
          val col = ServiceColumn.find(service.id.get, column).getOrElse(throw BadQueryException("bad column name"))
          val (idOpt, idsOpt) = ((value \ "id").asOpt[JsValue], (value \ "ids").asOpt[List[JsValue]])
          for {
            idVal <- idOpt ++ idsOpt.toSeq.flatten

            /** bug, need to use labels schemaVersion  */
            innerVal <- jsValueToInnerVal(idVal, col.columnType, col.schemaVersion)
          } yield {
            Vertex(SourceVertexId(col.id.get, innerVal), System.currentTimeMillis())
          }
        }).flatten

      if (vertices.isEmpty) throw BadQueryException("srcVertices`s id is empty")

      val filterOutFields = (jsValue \ "filterOutFields").asOpt[List[String]].getOrElse(List(LabelMeta.to.name))
      val filterOutQuery = (jsValue \ "filterOut").asOpt[JsValue].map { v => toQuery(v) }.map { q => q.copy(filterOutFields = filterOutFields) }
      val steps = parse[Vector[JsValue]](jsValue, "steps")
      val removeCycle = (jsValue \ "removeCycle").asOpt[Boolean].getOrElse(true)
      val selectColumns = (jsValue \ "select").asOpt[List[String]].getOrElse(List.empty)
      val groupByColumns = (jsValue \ "groupBy").asOpt[List[String]].getOrElse(List.empty)
      val orderByColumns: List[(String, Boolean)] = (jsValue \ "orderBy").asOpt[List[JsObject]].map { jsLs =>
        for {
          js <- jsLs
          (column, orderJs) <- js.fields
        } yield {
          val ascending = orderJs.as[String].toUpperCase match {
            case "ASC" => true
            case "DESC" => false
          }
          column -> ascending
        }
      }.getOrElse(List("score" -> false, "timestamp" -> false))
      val withScore = (jsValue \ "withScore").asOpt[Boolean].getOrElse(true)
      val returnTree = (jsValue \ "returnTree").asOpt[Boolean].getOrElse(false)

      // TODO: throw exception, when label dosn't exist
      val labelMap = (for {
        js <- jsValue \\ "label"
        labelName <- js.asOpt[String]
        label <- Label.findByName(labelName)
      } yield (labelName, label)).toMap

      val querySteps =
        steps.zipWithIndex.map { case (step, stepIdx) =>
          val labelWeights = step match {
            case obj: JsObject =>
              val converted = for {
                (k, v) <- (obj \ "weights").asOpt[JsObject].getOrElse(Json.obj()).fields
                l <- Label.findByName(k)
              } yield {
                l.id.get -> v.toString().toDouble
              }
              converted.toMap
            case _ => Map.empty[Int, Double]
          }
          val queryParamJsVals = step match {
            case arr: JsArray => arr.as[List[JsValue]]
            case obj: JsObject => (obj \ "step").as[List[JsValue]]
            case _ => List.empty[JsValue]
          }
          val nextStepScoreThreshold = step match {
            case obj: JsObject => (obj \ "nextStepThreshold").asOpt[Double].getOrElse(QueryParam.DefaultThreshold)
            case _ => QueryParam.DefaultThreshold
          }
          val nextStepLimit = step match {
            case obj: JsObject => (obj \ "nextStepLimit").asOpt[Int].getOrElse(-1)
            case _ => -1
          }
          val cacheTTL = step match {
            case obj: JsObject => (obj \ "cacheTTL").asOpt[Int].getOrElse(-1)
            case _ => -1
          }
          val queryParams =
            for {
              labelGroup <- queryParamJsVals
              queryParam <- parseQueryParam(labelMap, labelGroup)
            } yield {
              val (_, columnName) =
                if (queryParam.labelWithDir.dir == GraphUtil.directions("out")) {
                  (queryParam.label.srcService.serviceName, queryParam.label.srcColumnName)
                } else {
                  (queryParam.label.tgtService.serviceName, queryParam.label.tgtColumnName)
                }
              //FIXME:
              if (stepIdx == 0 && vertices.nonEmpty && !vertices.exists(v => v.serviceColumn.columnName == columnName)) {
                throw BadQueryException("srcVertices contains incompatiable serviceName or columnName with first step.")
              }

              queryParam
            }
          Step(queryParams.toList, labelWeights = labelWeights,
            //            scoreThreshold = stepThreshold,
            nextStepScoreThreshold = nextStepScoreThreshold,
            nextStepLimit = nextStepLimit,
            cacheTTL = cacheTTL)

        }

      val ret = Query(
        vertices,
        querySteps,
        removeCycle = removeCycle,
        selectColumns = selectColumns,
        groupByColumns = groupByColumns,
        orderByColumns = orderByColumns,
        filterOutQuery = filterOutQuery,
        filterOutFields = filterOutFields,
        withScore = withScore,
        returnTree = returnTree
      )
      //      logger.debug(ret.toString)
      ret
    } catch {
      case e: BadQueryException =>
        throw e
      case e: ModelNotFoundException =>
        throw BadQueryException(e.getMessage, e)
      case e: Exception =>
        throw BadQueryException(s"$jsValue, $e", e)
    }
  }

  private def parseQueryParam(labelMap: Map[String, Label], labelGroup: JsValue): Option[QueryParam] = {
    for {
      labelName <- parse[Option[String]](labelGroup, "label")
    } yield {
      val label = Label.findByName(labelName).getOrElse(throw BadQueryException(s"$labelName not found"))
      val direction = parse[Option[String]](labelGroup, "direction").map(GraphUtil.toDirection(_)).getOrElse(0)
      val limit = {
        parse[Option[Int]](labelGroup, "limit") match {
          case None => defaultLimit
          case Some(l) if l < 0 => Int.MaxValue
          case Some(l) if l >= 0 =>
            val default = hardLimit
            Math.min(l, default)
        }
      }
      val offset = parse[Option[Int]](labelGroup, "offset").getOrElse(0)
      val interval = extractInterval(label, labelGroup)
      val duration = extractDuration(label, labelGroup)
      val scoring = extractScoring(label.id.get, labelGroup).getOrElse(List.empty[(Byte, Double)]).toList
      val exclude = parse[Option[Boolean]](labelGroup, "exclude").getOrElse(false)
      val include = parse[Option[Boolean]](labelGroup, "include").getOrElse(false)
      val hasFilter = extractHas(label, labelGroup)
      val labelWithDir = LabelWithDirection(label.id.get, direction)
      val indexNameOpt = (labelGroup \ "index").asOpt[String]
      val indexSeq = indexNameOpt match {
        case None => label.indexSeqsMap.get(scoring.map(kv => kv._1)).map(_.seq).getOrElse(LabelIndex.DefaultSeq)
        case Some(indexName) => label.indexNameMap.get(indexName).map(_.seq).getOrElse(throw new RuntimeException("cannot find index"))
      }
      val where = extractWhere(labelMap, labelGroup)
      val includeDegree = (labelGroup \ "includeDegree").asOpt[Boolean].getOrElse(true)
      val rpcTimeout = (labelGroup \ "rpcTimeout").asOpt[Int].getOrElse(DefaultRpcTimeout)
      val maxAttempt = (labelGroup \ "maxAttempt").asOpt[Int].getOrElse(DefaultMaxAttempt)
      val tgtVertexInnerIdOpt = (labelGroup \ "_to").asOpt[JsValue].flatMap { jsVal =>
        jsValueToInnerVal(jsVal, label.tgtColumnWithDir(direction).columnType, label.schemaVersion)
      }
      val cacheTTL = (labelGroup \ "cacheTTL").asOpt[Long].getOrElse(-1L)
      val timeDecayFactor = (labelGroup \ "timeDecay").asOpt[JsObject].map { jsVal =>
        val initial = (jsVal \ "initial").asOpt[Double].getOrElse(1.0)
        val decayRate = (jsVal \ "decayRate").asOpt[Double].getOrElse(0.1)
        if (decayRate >= 1.0 || decayRate <= 0.0) throw new BadQueryException("decay rate should be 0.0 ~ 1.0")
        val timeUnit = (jsVal \ "timeUnit").asOpt[Double].getOrElse(60 * 60 * 24.0)
        TimeDecay(initial, decayRate, timeUnit)
      }
      val threshold = (labelGroup \ "threshold").asOpt[Double].getOrElse(QueryParam.DefaultThreshold)
      // TODO: refactor this. dirty
      val duplicate = parse[Option[String]](labelGroup, "duplicate").map(s => Query.DuplicatePolicy(s))

      val outputField = (labelGroup \ "outputField").asOpt[String].map(s => Json.arr(Json.arr(s)))
      val transformer = if (outputField.isDefined) outputField else (labelGroup \ "transform").asOpt[JsValue]
      val scorePropagateOp = (labelGroup \ "scorePropagateOp").asOpt[String].getOrElse("multiply")
      val sample = (labelGroup \ "sample").asOpt[Int].getOrElse(-1)

      // FIXME: Order of command matter
      QueryParam(labelWithDir)
        .sample(sample)
        .limit(offset, limit)
        .rank(RankParam(label.id.get, scoring))
        .exclude(exclude)
        .include(include)
        .interval(interval)
        .duration(duration)
        .has(hasFilter)
        .labelOrderSeq(indexSeq)
        .interval(interval)
        .where(where)
        .duplicatePolicy(duplicate)
        .includeDegree(includeDegree)
        .rpcTimeout(rpcTimeout)
        .maxAttempt(maxAttempt)
        .tgtVertexInnerIdOpt(tgtVertexInnerIdOpt)
        .cacheTTLInMillis(cacheTTL)
        .timeDecay(timeDecayFactor)
        .threshold(threshold)
        .transformer(transformer)
        .scorePropagateOp(scorePropagateOp)
    }
  }

  private def parse[R](js: JsValue, key: String)(implicit read: Reads[R]): R = {
    (js \ key).validate[R]
      .fold(
        errors => {
          val msg = (JsError.toFlatJson(errors) \ "obj").as[List[JsValue]].map(x => x \ "msg")
          val e = Json.obj("args" -> key, "error" -> msg)
          throw new GraphExceptions.JsonParseException(Json.obj("error" -> key).toString)
        },
        r => {
          r
        })
  }

  def toJsValues(jsValue: JsValue): List[JsValue] = {
    jsValue match {
      case obj: JsObject => List(obj)
      case arr: JsArray => arr.as[List[JsValue]]
      case _ => List.empty[JsValue]
    }

  }

  def toEdges(jsValue: JsValue, operation: String): List[Edge] = {
    toJsValues(jsValue).map(toEdge(_, operation))
  }

  def toEdge(jsValue: JsValue, operation: String) = {

    val srcId = parse[JsValue](jsValue, "from") match {
      case s: JsString => s.as[String]
      case o@_ => s"${o}"
    }
    val tgtId = parse[JsValue](jsValue, "to") match {
      case s: JsString => s.as[String]
      case o@_ => s"${o}"
    }
    val label = parse[String](jsValue, "label")
    val timestamp = parse[Long](jsValue, "timestamp")
    val direction = parse[Option[String]](jsValue, "direction").getOrElse("")
    val props = (jsValue \ "props").asOpt[JsValue].getOrElse("{}")
    Management.toEdge(timestamp, operation, srcId, tgtId, label, direction, props.toString)

  }

  def toVertices(jsValue: JsValue, operation: String, serviceName: Option[String] = None, columnName: Option[String] = None) = {
    toJsValues(jsValue).map(toVertex(_, operation, serviceName, columnName))
  }

  def toVertex(jsValue: JsValue, operation: String, serviceName: Option[String] = None, columnName: Option[String] = None): Vertex = {
    val id = parse[JsValue](jsValue, "id")
    val ts = parse[Option[Long]](jsValue, "timestamp").getOrElse(System.currentTimeMillis())
    val sName = if (serviceName.isEmpty) parse[String](jsValue, "serviceName") else serviceName.get
    val cName = if (columnName.isEmpty) parse[String](jsValue, "columnName") else columnName.get
    val props = (jsValue \ "props").asOpt[JsObject].getOrElse(Json.obj())
    Management.toVertex(ts, operation, id.toString, sName, cName, props.toString)
  }

  def toPropElements(jsObj: JsValue) = Try {
    val propName = (jsObj \ "name").as[String]
    val dataType = InnerVal.toInnerDataType((jsObj \ "dataType").as[String])
    val defaultValue = (jsObj \ "defaultValue").as[JsValue] match {
      case JsString(s) => s
      case _@js => js.toString
    }
    Prop(propName, defaultValue, dataType)
  }

  def toPropsElements(jsValue: JsValue): Seq[Prop] = for {
    jsObj <- jsValue.asOpt[Seq[JsValue]].getOrElse(Nil)
  } yield {
    val propName = (jsObj \ "name").as[String]
    val dataType = InnerVal.toInnerDataType((jsObj \ "dataType").as[String])
    val defaultValue = (jsObj \ "defaultValue").as[JsValue] match {
      case JsString(s) => s
      case _@js => js.toString
    }
    Prop(propName, defaultValue, dataType)
  }

  def toIndicesElements(jsValue: JsValue): Seq[Index] = for {
    jsObj <- jsValue.as[Seq[JsValue]]
    indexName = (jsObj \ "name").as[String]
    propNames = (jsObj \ "propNames").as[Seq[String]]
  } yield Index(indexName, propNames)

  def toLabelElements(jsValue: JsValue) = Try {
    val labelName = parse[String](jsValue, "label")
    val srcServiceName = parse[String](jsValue, "srcServiceName")
    val tgtServiceName = parse[String](jsValue, "tgtServiceName")
    val srcColumnName = parse[String](jsValue, "srcColumnName")
    val tgtColumnName = parse[String](jsValue, "tgtColumnName")
    val srcColumnType = parse[String](jsValue, "srcColumnType")
    val tgtColumnType = parse[String](jsValue, "tgtColumnType")
    val serviceName = (jsValue \ "serviceName").asOpt[String].getOrElse(tgtServiceName)
    val isDirected = (jsValue \ "isDirected").asOpt[Boolean].getOrElse(true)

    val allProps = toPropsElements(jsValue \ "props")
    val indices = toIndicesElements(jsValue \ "indices")

    val consistencyLevel = (jsValue \ "consistencyLevel").asOpt[String].getOrElse("weak")

    // expect new label don`t provide hTableName
    val hTableName = (jsValue \ "hTableName").asOpt[String]
    val hTableTTL = (jsValue \ "hTableTTL").asOpt[Int]
    val schemaVersion = (jsValue \ "schemaVersion").asOpt[String].getOrElse(HBaseType.DEFAULT_VERSION)
    val isAsync = (jsValue \ "isAsync").asOpt[Boolean].getOrElse(false)
    val compressionAlgorithm = (jsValue \ "compressionAlgorithm").asOpt[String].getOrElse(DefaultCompressionAlgorithm)

    (labelName, srcServiceName, srcColumnName, srcColumnType,
      tgtServiceName, tgtColumnName, tgtColumnType, isDirected, serviceName,
      indices, allProps, consistencyLevel, hTableName, hTableTTL, schemaVersion, isAsync, compressionAlgorithm)
  }

  def toIndexElements(jsValue: JsValue) = Try {
    val labelName = parse[String](jsValue, "label")
    val indices = toIndicesElements(jsValue \ "indices")
    (labelName, indices)
  }

  def toServiceElements(jsValue: JsValue) = {
    val serviceName = parse[String](jsValue, "serviceName")
    val cluster = (jsValue \ "cluster").asOpt[String].getOrElse(DefaultCluster)
    val hTableName = (jsValue \ "hTableName").asOpt[String].getOrElse(s"${serviceName}-${DefaultPhase}")
    val preSplitSize = (jsValue \ "preSplitSize").asOpt[Int].getOrElse(1)
    val hTableTTL = (jsValue \ "hTableTTL").asOpt[Int]
    val compressionAlgorithm = (jsValue \ "compressionAlgorithm").asOpt[String].getOrElse(DefaultCompressionAlgorithm)
    (serviceName, cluster, hTableName, preSplitSize, hTableTTL, compressionAlgorithm)
  }

  def toServiceColumnElements(jsValue: JsValue) = Try {
    val serviceName = parse[String](jsValue, "serviceName")
    val columnName = parse[String](jsValue, "columnName")
    val columnType = parse[String](jsValue, "columnType")
    val props = toPropsElements(jsValue \ "props")
    (serviceName, columnName, columnType, props)
  }

  def toCheckEdgeParam(jsValue: JsValue) = {
    val params = jsValue.as[List[JsValue]]
    var isReverted = false
    val labelWithDirs = scala.collection.mutable.HashSet[LabelWithDirection]()
    val quads = for {
      param <- params
      labelName <- (param \ "label").asOpt[String]
      direction <- GraphUtil.toDir((param \ "direction").asOpt[String].getOrElse("out"))
      label <- Label.findByName(labelName)
      srcId <- jsValueToInnerVal((param \ "from").as[JsValue], label.srcColumnWithDir(direction.toInt).columnType, label.schemaVersion)
      tgtId <- jsValueToInnerVal((param \ "to").as[JsValue], label.tgtColumnWithDir(direction.toInt).columnType, label.schemaVersion)
    } yield {
      val labelWithDir = LabelWithDirection(label.id.get, direction)
      labelWithDirs += labelWithDir
      val (src, tgt, dir) = if (direction == 1) {
        isReverted = true
        (Vertex(VertexId(label.tgtColumnWithDir(direction.toInt).id.get, tgtId)),
          Vertex(VertexId(label.srcColumnWithDir(direction.toInt).id.get, srcId)), 0)
      } else {
        (Vertex(VertexId(label.srcColumnWithDir(direction.toInt).id.get, srcId)),
          Vertex(VertexId(label.tgtColumnWithDir(direction.toInt).id.get, tgtId)), 0)
      }
      (src, tgt, QueryParam(LabelWithDirection(label.id.get, dir)))
    }
    (quads, isReverted)
  }

  def toGraphElements(str: String): Seq[GraphElement] = {
    val edgeStrs = str.split("\\n")

    for {
      edgeStr <- edgeStrs
      str <- GraphUtil.parseString(edgeStr)
      element <- Graph.toGraphElement(str)
    } yield element
  }

  def toDeleteParam(json: JsValue) = {
    val labelName = (json \ "label").as[String]
    val labels = Label.findByName(labelName).map { l => Seq(l) }.getOrElse(Nil)
    val direction = (json \ "direction").asOpt[String].getOrElse("out")

    val ids = (json \ "ids").asOpt[List[JsValue]].getOrElse(Nil)
    val ts = (json \ "timestamp").asOpt[Long].getOrElse(System.currentTimeMillis())
    val vertices = toVertices(labelName, direction, ids)
    (labels, direction, ids, ts, vertices)
  }
}
