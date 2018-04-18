/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.rest


import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.cache.CacheBuilder
import org.apache.s2graph.core.GraphExceptions.{BadQueryException, ModelNotFoundException}
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.parsers.{Where, WhereParser}
import org.apache.s2graph.core.types._
import play.api.libs.json._


import scala.util.{Random, Failure, Success, Try}

object TemplateHelper {
  val findVar = """\"?\$\{(.*?)\}\"?""".r
  val num = """(next_minute|next_day|next_hour|next_week|now)?\s*(-?\s*[0-9]+)?\s*(minute|hour|day|week)?""".r
  val randIntRegex = """randint\((.*,.*)\)""".r

  val minute: Long = 60 * 1000L
  val hour = 60 * minute
  val day = 24 * hour
  val week = 7 * day

  def calculate(now: Long, n: Int, unit: String): Long = {
    val duration = unit match {
      case "minute" | "MINUTE" => n * minute
      case "hour" | "HOUR" => n * hour
      case "day" | "DAY" => n * day
      case "week" | "WEEK" => n * week
      case _ => n
    }

    duration + now
  }

  def randInt(s: String): Long = {
    val tokens = s.split(",").map(_.trim)
    if (tokens.length != 2) throw new RuntimeException(s"TemplateHelper.randint has wrong format. $s")
    val (from, to) = try {
      (tokens.head.toInt, tokens.last.toInt)
    } catch {
      case e: Exception => throw new RuntimeException(s"TemplateHelper.randint has wrong format. $s")
    }
    if (from > to) throw new RuntimeException(s"TemplateHelper.randint has wrong format. $s")
    val diff = to - from
    val r = Random.nextInt(diff + 1)
    assert(diff >= 0 && diff < Int.MaxValue && from + r < Int.MaxValue)
    from + r
  }

  def replaceVariable(now: Long, body: String): String = {
    findVar.replaceAllIn(body, m => {
      val matched = m group 1
      randIntRegex.findFirstMatchIn(matched) match {
        case None =>
          num.replaceSomeIn(matched, m => {
            val (_pivot, n, unit) = (m.group(1), m.group(2), m.group(3))
            val ts = _pivot match {
              case null => now
              case "now" | "NOW" => now
              case "next_minute" | "NEXT_MINUTE" => now / minute * minute + minute
              case "next_week" | "NEXT_WEEK" => now / week * week + week
              case "next_day" | "NEXT_DAY" => now / day * day + day
              case "next_hour" | "NEXT_HOUR" => now / hour * hour + hour
            }

            if (_pivot == null && n == null && unit == null) None
            else if (n == null || unit == null) Option(ts.toString)
            else Option(calculate(ts, n.replaceAll(" ", "").toInt, unit).toString)
          })
        case Some(m) =>
          val range = m group 1
          randInt(range).toString
      }
    })
  }
}

object RequestParser {
  type ExperimentParam = (JsObject, String, String, String, Option[String])
  val defaultLimit = 100

  def toJsValues(jsValue: JsValue): List[JsValue] = {
    jsValue match {
      case obj: JsObject => List(obj)
      case arr: JsArray => arr.as[List[JsValue]]
      case _ => List.empty[JsValue]
    }
  }

  def jsToStr(js: JsValue): String = js match {
    case JsString(s) => s
    case _ => js.toString()
  }

}

class RequestParser(graph: S2GraphLike) {

  import Management.JsonModel._
  import RequestParser._

  val config = graph.config
  val hardLimit = config.getInt("query.hardlimit")
  val maxLimit = Int.MaxValue - 1
  val DefaultRpcTimeout = config.getInt("hbase.rpc.timeout")
  val DefaultMaxAttempt = config.getInt("hbase.client.retries.number")
  val DefaultCluster = config.getString("hbase.zookeeper.quorum")
  val DefaultCompressionAlgorithm = config.getString("hbase.table.compression.algorithm")
  val DefaultPhase = config.getString("phase")
  val parserCache = CacheBuilder.newBuilder()
      .expireAfterAccess(10000, TimeUnit.MILLISECONDS)
      .expireAfterWrite(10000, TimeUnit.MILLISECONDS)
      .maximumSize(10000)
      .initialCapacity(1000)
      .build[String, Try[Where]]

  private def extractScoring(label: Label, value: JsValue): Option[Seq[(LabelMeta, Double)]] = {
    val ret = for {
      js <- parseOption[JsObject](value, "scoring")
    } yield {
      for {
        (k, v) <- js.fields
        labelMata <- label.metaPropsInvMap.get(k)
      } yield {
        val value = v match {
          case n: JsNumber => n.as[Double]
          case _ => throw new Exception("scoring weight should be double.")
        }
        (labelMata, value)
      }
    }

    ret
  }

  def extractInterval(label: Label, jsValue: JsValue): Option[(Seq[(String, JsValue)], Seq[(String, JsValue)])] = {
    def extractKv(js: JsValue): Seq[(String, JsValue)] = js match {
      case JsObject(obj) => obj.toSeq
      case JsArray(arr) => arr.flatMap {
        case JsObject(obj) => obj.toSeq
        case _ => throw new RuntimeException(s"cannot support json type $js")
      }
      case _ => throw new RuntimeException(s"cannot support json type: $js")
    }

    val ret = for {
      js <- parseOption[JsObject](jsValue, "interval")
      fromJs <- (js \ "from").asOpt[JsValue]
      toJs <- (js \ "to").asOpt[JsValue]
    } yield {
      val from = extractKv(fromJs)
      val to = extractKv(toJs)
      (from, to)
    }

    ret
  }

  def extractDuration(label: Label, jsValue: JsValue) = {
    for {
      js <- parseOption[JsObject](jsValue, "duration")
    } yield {
      val minTs = (js \ "from").get match {
        case JsString(s) => TemplateHelper.replaceVariable(System.currentTimeMillis(), s).toLong
        case JsNumber(n) => n.toLong
        case _ => Long.MinValue
      }

      val maxTs = (js \ "to").get match {
        case JsString(s) => TemplateHelper.replaceVariable(System.currentTimeMillis(), s).toLong
        case JsNumber(n) => n.toLong
        case _ => Long.MaxValue
      }

      if (minTs > maxTs) {
        throw new BadQueryException("Duration error. Timestamp of From cannot be larger than To.")
      }

      (minTs, maxTs)
    }
  }

  def extractHas(label: Label, jsValue: JsValue) = {
    val ret = for {
      js <- parseOption[JsObject](jsValue, "has")
    } yield {
      for {
        (k, v) <- js.fields
        labelMeta <- LabelMeta.findByName(label.id.get, k)
        value <- jsValueToInnerVal(v, labelMeta.dataType, label.schemaVersion)
      } yield {
        labelMeta.name -> value
      }
    }
    ret.map(_.toMap).getOrElse(Map.empty[String, InnerValLike])
  }

  def extractWhere(label: Label, whereClauseOpt: Option[String]): Try[Where] = {
    whereClauseOpt match {
      case None => Success(WhereParser.success)
      case Some(where) =>
        val whereParserKey = s"${label.label}_${where}"

        parserCache.get(whereParserKey, new Callable[Try[Where]] {
          override def call(): Try[Where] = {
            val _where = TemplateHelper.replaceVariable(System.currentTimeMillis(), where)

            WhereParser().parse(_where) match {
              case s@Success(_) => s
              case Failure(ex) => throw BadQueryException(ex.getMessage, ex)
            }
          }
        })

    }
  }

  def extractGroupBy(value: Option[JsValue]): GroupBy = value.map {
    case obj: JsObject =>
      val keys = (obj \ "keys").asOpt[Seq[String]].getOrElse(Nil)
      val groupByLimit = (obj \ "limit").asOpt[Int].getOrElse(hardLimit)
      val minShouldMatchOpt = (obj \ "minimumShouldMatch").asOpt[JsObject].map { o =>
        val prop = (o \ "prop").asOpt[String].getOrElse("to")
        val count = (o \ "count").asOpt[Int].getOrElse(0)
        val terms = (o \ "terms").asOpt[Set[JsValue]].getOrElse(Set.empty).map {
          case JsString(s) => s
          case JsNumber(n) => n
          case _ => throw new RuntimeException("not supported data type")
        }.map(_.asInstanceOf[Any])

        MinShouldMatchParam(prop, count, terms)
      }

      GroupBy(keys, groupByLimit, minShouldMatch = minShouldMatchOpt)
    case arr: JsArray =>
      val keys = arr.asOpt[Seq[String]].getOrElse(Nil)
      GroupBy(keys)
    case _ => GroupBy.Empty
  }.getOrElse(GroupBy.Empty)

  def toVertices(labelName: String, direction: String, ids: Seq[JsValue]): Seq[S2VertexLike] = {
    val vertices = for {
      label <- Label.findByName(labelName).toSeq
      serviceColumn = if (direction == "out") label.srcColumn else label.tgtColumn
      id <- ids
      innerId <- jsValueToInnerVal(id, serviceColumn.columnType, label.schemaVersion)
    } yield {
      graph.elementBuilder.newVertex(SourceVertexId(serviceColumn, innerId), System.currentTimeMillis())
    }

    vertices
  }

  def toMultiQuery(jsValue: JsValue, impIdOpt: Option[String]): MultiQuery = {
    val globalQueryOption = toQueryOption(jsValue, impIdOpt)
    val queries = for {
      queryJson <- (jsValue \ "queries").asOpt[Seq[JsValue]].getOrElse(Seq.empty)
    } yield {
      val innerQuery = toQuery(queryJson, impIdOpt = impIdOpt)
      val queryOption = innerQuery.queryOption

      if (queryOption.groupBy.keys.nonEmpty) throw new BadQueryException("Group by option is not allowed in multiple queries.")
      if (queryOption.orderByKeys.nonEmpty) throw new BadQueryException("Order by option is not allowed in multiple queries.")

      if (globalQueryOption.withScore) innerQuery.copy(queryOption = innerQuery.queryOption.copy(withScore = false))
      else innerQuery
      //        val innerQuery3 =
      //          if (globalQueryOption.groupBy.keys.nonEmpty) innerQuery2.copy(queryOption = innerQuery2.queryOption.copy(groupBy = GroupBy.Empty))
      //          else innerQuery2

    }
    val weights = (jsValue \ "weights").asOpt[Seq[Double]].getOrElse(queries.map(_ => 1.0))
    MultiQuery(queries = queries, weights = weights, queryOption = globalQueryOption)
  }


  def toQueryOption(jsValue: JsValue, impIdOpt: Option[String]): QueryOption = {
    val filterOutFields = (jsValue \ "filterOutFields").asOpt[List[String]].getOrElse(List(LabelMeta.to.name))
    val filterOutQuery = (jsValue \ "filterOut").asOpt[JsValue].map { v =>
      toQuery(v, impIdOpt = impIdOpt)
    }.map { q =>
      q.copy(queryOption = q.queryOption.copy(filterOutFields = filterOutFields, selectColumns = filterOutFields))
    }
    val removeCycle = (jsValue \ "removeCycle").asOpt[Boolean].getOrElse(true)
    val selectColumns = (jsValue \ "select").asOpt[List[String]].getOrElse(List.empty)

    val groupBy = extractGroupBy((jsValue \ "groupBy").asOpt[JsValue])

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
    }.getOrElse(Nil)
    val withScore = (jsValue \ "withScore").asOpt[Boolean].getOrElse(true)
    val returnTree = (jsValue \ "returnTree").asOpt[Boolean].getOrElse(false)
    //TODO: Refactor this
    val limitOpt = (jsValue \ "limit").asOpt[Int]
    val returnAgg = (jsValue \ "returnAgg").asOpt[Boolean].getOrElse(true)
    val scoreThreshold = (jsValue \ "scoreThreshold").asOpt[Double].getOrElse(Double.MinValue)
    val returnDegree = (jsValue \ "returnDegree").asOpt[Boolean].getOrElse(true)
    val ignorePrevStepCache = (jsValue \ "ignorePrevStepCache").asOpt[Boolean].getOrElse(false)
    val shouldPropagateScore = (jsValue \ "shouldPropagateScore").asOpt[Boolean].getOrElse(true)

    QueryOption(removeCycle = removeCycle,
      selectColumns = selectColumns,
      groupBy = groupBy,
      orderByColumns = orderByColumns,
      filterOutQuery = filterOutQuery,
      filterOutFields = filterOutFields,
      withScore = withScore,
      returnTree = returnTree,
      limitOpt = limitOpt,
      returnAgg = returnAgg,
      scoreThreshold = scoreThreshold,
      returnDegree = returnDegree,
      impIdOpt = impIdOpt,
      ignorePrevStepCache,
      shouldPropagateScore
    )
  }

  def toQuery(jsValue: JsValue, impIdOpt: Option[String]): Query = {
    try {
      val vertices = for {
        value <- (jsValue \ "srcVertices").asOpt[Seq[JsValue]].getOrElse(Nil)
        vertex <- toVertex(value)
      } yield vertex

      if (vertices.isEmpty) throw BadQueryException("srcVertices`s id is empty")
      val steps = parse[Vector[JsValue]](jsValue, "steps")
      val queryOption = toQueryOption(jsValue, impIdOpt)

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
              queryParam <- parseQueryParam(labelGroup, queryOption)
            } yield {
              val (_, columnName) =
                if (queryParam.dir == GraphUtil.directions("out")) {
                  (queryParam.label.srcService.serviceName, queryParam.label.srcColumnName)
                } else {
                  (queryParam.label.tgtService.serviceName, queryParam.label.tgtColumnName)
                }
              //FIXME:
              if (stepIdx == 0 && vertices.nonEmpty && !vertices.exists(v => v.columnName == columnName)) {
                throw BadQueryException("srcVertices contains incompatiable serviceName or columnName with first step.")
              }

              queryParam
            }


          val groupBy = extractGroupBy((step \ "groupBy").asOpt[JsValue])

          Step(queryParams = queryParams,
            labelWeights = labelWeights,
            nextStepScoreThreshold = nextStepScoreThreshold,
            nextStepLimit = nextStepLimit,
            cacheTTL = cacheTTL,
            groupBy = groupBy)

        }

      Query(vertices, querySteps, queryOption)
    } catch {
      case e: BadQueryException =>
        throw e
      case e: ModelNotFoundException =>
        throw BadQueryException(e.getMessage, e)
      case e: Exception =>
        throw BadQueryException(s"$jsValue, $e", e)
    }
  }

  private def parseQueryParam(labelGroup: JsValue, queryOption: QueryOption): Option[QueryParam] = {
    for {
      labelName <- parseOption[String](labelGroup, "label")
    } yield {
      val label = Label.findByName(labelName).getOrElse(throw BadQueryException(s"$labelName not found"))
      val direction = parseOption[String](labelGroup, "direction").getOrElse("out")
      val limit = {
        parseOption[Int](labelGroup, "limit") match {
          case None => defaultLimit
          case Some(l) if l < 0 => hardLimit
          case Some(l) if l >= 0 => Math.min(l, hardLimit)
        }
      }
      val offset = parseOption[Int](labelGroup, "offset").getOrElse(0)
      val interval = extractInterval(label, labelGroup)
      val duration = extractDuration(label, labelGroup)
      val scoring = extractScoring(label, labelGroup).getOrElse(Nil).toList
      val exclude = parseOption[Boolean](labelGroup, "exclude").getOrElse(false)
      val include = parseOption[Boolean](labelGroup, "include").getOrElse(false)
      val hasFilter = extractHas(label, labelGroup)

      val indexName = (labelGroup \ "index").asOpt[String].getOrElse(LabelIndex.DefaultName)
      val whereClauseOpt = (labelGroup \ "where").asOpt[String]
      val where = extractWhere(label, whereClauseOpt)
      val includeDegree = (labelGroup \ "includeDegree").asOpt[Boolean].getOrElse(true)
      val rpcTimeout = (labelGroup \ "rpcTimeout").asOpt[Int].getOrElse(DefaultRpcTimeout)
      val maxAttempt = (labelGroup \ "maxAttempt").asOpt[Int].getOrElse(DefaultMaxAttempt)

      val tgtVertexInnerIdOpt = (labelGroup \ "_to").asOpt[JsValue].filterNot(_ == JsNull).flatMap(jsValueToAny)

      val cacheTTL = (labelGroup \ "cacheTTL").asOpt[Long].getOrElse(-1L)
      val timeDecayFactor = (labelGroup \ "timeDecay").asOpt[JsObject].map { jsVal =>
        val propName = (jsVal \ "propName").asOpt[String].getOrElse(LabelMeta.timestamp.name)
        val propNameSeq = label.metaPropsInvMap.get(propName).getOrElse(LabelMeta.timestamp)
        val initial = (jsVal \ "initial").asOpt[Double].getOrElse(1.0)
        val decayRate = (jsVal \ "decayRate").asOpt[Double].getOrElse(0.1)
        if (decayRate >= 1.0 || decayRate <= 0.0) throw new BadQueryException("decay rate should be 0.0 ~ 1.0")
        val timeUnit = (jsVal \ "timeUnit").asOpt[Double].getOrElse(60 * 60 * 24.0)
        TimeDecay(initial, decayRate, timeUnit, propNameSeq)
      }
      val threshold = (labelGroup \ "threshold").asOpt[Double].getOrElse(QueryParam.DefaultThreshold)
      // TODO: refactor this. dirty
      val duplicate = parseOption[String](labelGroup, "duplicate").map(s => DuplicatePolicy(s)).getOrElse(DuplicatePolicy.First)

      val outputField = (labelGroup \ "outputField").asOpt[String].map(s => Json.arr(Json.arr(s)))
      val transformer = (if (outputField.isDefined) outputField else (labelGroup \ "transform").asOpt[JsValue]) match {
        case None => EdgeTransformer(EdgeTransformer.DefaultJson)
        case Some(json) => EdgeTransformer(json)
      }
      val scorePropagateOp = (labelGroup \ "scorePropagateOp").asOpt[String].getOrElse("multiply")
      val scorePropagateShrinkage = (labelGroup \ "scorePropagateShrinkage").asOpt[Long].getOrElse(500l)
      val sample = (labelGroup \ "sample").asOpt[Int].getOrElse(-1)
      val shouldNormalize = (labelGroup \ "normalize").asOpt[Boolean].getOrElse(false)
      val cursorOpt = (labelGroup \ "cursor").asOpt[String]
      // FIXME: Order of command matter
      QueryParam(labelName = labelName,
        direction = direction,
        offset = offset,
        limit = limit,
        sample = sample,
        maxAttempt = maxAttempt,
        rpcTimeout = rpcTimeout,
        cacheTTLInMillis = cacheTTL,
        indexName = indexName, where = where, threshold = threshold,
        rank = RankParam(scoring), intervalOpt = interval, durationOpt = duration,
        exclude = exclude, include = include, has = hasFilter, duplicatePolicy = duplicate,
        includeDegree = includeDegree, scorePropagateShrinkage = scorePropagateShrinkage,
        scorePropagateOp = scorePropagateOp, shouldNormalize = shouldNormalize,
        whereRawOpt = whereClauseOpt, cursorOpt = cursorOpt,
        tgtVertexIdOpt = tgtVertexInnerIdOpt,
        edgeTransformer = transformer, timeDecay = timeDecayFactor
      )
    }
  }

  private def parse[R](js: JsValue, key: String)(implicit read: Reads[R]): R = {
    (js \ key).validate[R] match {
      case JsError(errors) =>
        val msg = (JsError.toJson(errors) \ "obj").as[List[JsValue]].flatMap(x => (x \ "msg").toOption)
        val e = Json.obj("args" -> key, "error" -> msg)
        throw new GraphExceptions.JsonParseException(Json.obj("error" -> key).toString)
      case JsSuccess(result, _) => result
    }
  }

  private def parseOption[R](js: JsValue, key: String)(implicit read: Reads[R]): Option[R] = {
    (js \ key).validateOpt[R] match {
      case JsError(errors) =>
        val msg = (JsError.toJson(errors) \ "obj").as[List[JsValue]].flatMap(x => (x \ "msg").toOption)
        val e = Json.obj("args" -> key, "error" -> msg)
        throw new GraphExceptions.JsonParseException(Json.obj("error" -> key).toString)
      case JsSuccess(result, _) => result
    }
  }

  def jsToStr(js: JsValue): String = js match {
    case JsString(s) => s
    case _ => js.toString()
  }

  def parseBulkFormat(str: String): Seq[(GraphElement, String)] = {
    val edgeStrs = str.split("\\n").filterNot(_.isEmpty)
    val elementsWithTsv = for {
      edgeStr <- edgeStrs
      str <- GraphUtil.parseString(edgeStr)
      element <- graph.toGraphElement(str)
    } yield (element, str)

    elementsWithTsv
  }

  def parseJsonFormat(jsValue: JsValue, operation: String): Seq[(S2EdgeLike, String)] = {
    val jsValues = toJsValues(jsValue)
    jsValues.flatMap(toEdgeWithTsv(_, operation))
  }

  private def toEdgeWithTsv(jsValue: JsValue, operation: String): Seq[(S2EdgeLike, String)] = {
    val srcIds = (jsValue \ "from").asOpt[JsValue].map(Seq(_)).getOrElse(Nil) ++ (jsValue \ "froms").asOpt[Seq[JsValue]].getOrElse(Nil)
    val tgtIds = (jsValue \ "to").asOpt[JsValue].map(Seq(_)).getOrElse(Nil) ++ (jsValue \ "tos").asOpt[Seq[JsValue]].getOrElse(Nil)

    val label = parse[String](jsValue, "label")
    val timestamp = parse[Long](jsValue, "timestamp")
    val direction = parseOption[String](jsValue, "direction").getOrElse("out")
    val propsJson = (jsValue \ "props").asOpt[JsObject].getOrElse(Json.obj())

    for {
      srcId <- srcIds.flatMap(jsValueToAny(_).toSeq)
      tgtId <- tgtIds.flatMap(jsValueToAny(_).toSeq)
    } yield {
      //      val edge = Management.toEdge(graph, timestamp, operation, srcId, tgtId, label, direction, fromJsonToProperties(propsJson))
      val edge = graph.toEdge(srcId, tgtId, label, direction, fromJsonToProperties(propsJson), ts = timestamp, operation = operation)
      val tsv = (jsValue \ "direction").asOpt[String] match {
        case None => Seq(timestamp, operation, "e", srcId, tgtId, label, propsJson.toString).mkString("\t")
        case Some(dir) => Seq(timestamp, operation, "e", srcId, tgtId, label, propsJson.toString, dir).mkString("\t")
      }

      (edge, tsv)
    }
  }

  def toVertices(jsValue: JsValue, operation: String, serviceName: Option[String] = None, columnName: Option[String] = None) = {
    toJsValues(jsValue).flatMap(toVertex(_, operation, serviceName, columnName))
  }

  def toVertex(jsValue: JsValue, operation: String = "insert", serviceName: Option[String] = None, columnName: Option[String] = None): Seq[S2VertexLike] = {
    ((jsValue \ "id").asOpt[JsValue].map(Seq(_)).getOrElse(Nil) ++
      (jsValue \ "ids").asOpt[Seq[JsValue]].getOrElse(Nil)).flatMap(JSONParser.jsValueToAny).map { id =>
      val ts = parseOption[Long](jsValue, "timestamp").getOrElse(System.currentTimeMillis())
      val sName = if (serviceName.isEmpty) parse[String](jsValue, "serviceName") else serviceName.get
      val cName = if (columnName.isEmpty) parse[String](jsValue, "columnName") else columnName.get
      val props = fromJsonToProperties((jsValue \ "props").asOpt[JsObject].getOrElse(Json.obj()))
      graph.toVertex(sName, cName, id, props, ts, operation)
    }
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

  def toPropsElements(jsValue: JsLookupResult): Seq[Prop] = for {
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

  def toIndicesElements(jsValue: JsLookupResult): Seq[Index] = {
    val indices = for {
      jsObj <- jsValue.as[Seq[JsValue]]
      indexName = (jsObj \ "name").as[String]
      propNames = (jsObj \ "propNames").as[Seq[String]]
      direction = (jsObj \ "direction").asOpt[String].map(GraphUtil.toDirection)
      options = (jsObj \ "options").asOpt[JsValue].map(_.toString)
    } yield {
      Index(indexName, propNames, direction, options)
    }

    val (pk, others) = indices.partition(index => index.name == LabelIndex.DefaultName)
    val (both, inOut) = others.partition(index => index.direction.isEmpty)
    val (in, out) = inOut.partition(index => index.direction.get == GraphUtil.directions("in"))

    pk ++ both ++ in ++ out
  }

  def toLabelElements(jsValue: JsValue): Try[Label] = {
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
    val options = (jsValue \ "options").asOpt[JsValue].map(_.toString())

    graph.management.createLabel(labelName, srcServiceName, srcColumnName, srcColumnType,
        tgtServiceName, tgtColumnName, tgtColumnType, isDirected, serviceName,
        indices, allProps, consistencyLevel, hTableName, hTableTTL, schemaVersion, isAsync, compressionAlgorithm, options)
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
    for {
      json <- jsValue.asOpt[Seq[JsValue]].getOrElse(Nil)
      from <- (json \ "from").asOpt[JsValue].flatMap(jsValueToAny(_))
      to <- (json \ "to").asOpt[JsValue].flatMap(jsValueToAny(_))
      labelName <- (json \ "label").asOpt[String]
      direction = (json \ "direction").asOpt[String].getOrElse("out")
    } yield {
      graph.toEdge(from, to, labelName, direction, Map.empty)
    }
  }

  def toGraphElements(str: String): Seq[GraphElement] = {
    val edgeStrs = str.split("\\n")

    for {
      edgeStr <- edgeStrs
      str <- GraphUtil.parseString(edgeStr)
      element <- graph.toGraphElement(str)
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

  def toFetchAndDeleteParam(json: JsValue) = {
    val labelName = (json \ "label").as[String]
    val fromOpt = (json \ "from").asOpt[JsValue]
    val toOpt = (json \ "to").asOpt[JsValue]
    val direction = (json \ "direction").asOpt[String].getOrElse("out")
    val indexOpt = (json \ "index").asOpt[String]
    val propsOpt = (json \ "props").asOpt[JsObject]
    (labelName, fromOpt, toOpt, direction, indexOpt, propsOpt)
  }

  def parseExperiment(jsQuery: JsValue): Seq[ExperimentParam] = jsQuery.as[Seq[JsObject]].map { obj =>
    def _require(field: String) = throw new RuntimeException(s"${field} not found")

    val accessToken = (obj \ "accessToken").asOpt[String].getOrElse(_require("accessToken"))
    val experimentName = (obj \ "experiment").asOpt[String].getOrElse(_require("experiment"))
    val uuid = (obj \ "#uuid").get match {
      case JsString(s) => s
      case JsNumber(n) => n.toString
      case _ => _require("#uuid")
    }
    val body = (obj \ "params").asOpt[JsObject].getOrElse(Json.obj())
    val impKeyOpt = (obj \ Experiment.ImpressionKey).asOpt[String]

    (body, accessToken, experimentName, uuid, impKeyOpt)
  }
}
