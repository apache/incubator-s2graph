package controllers

import com.daumkakao.s2graph.core._
//import com.daumkakao.s2graph.core.mysqls._

import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.parsers.WhereParser
import com.daumkakao.s2graph.core.types2._
import play.api.Logger
import play.api.libs.json._
import com.daumkakao.s2graph.rest.config.Config

trait RequestParser extends JSONParser {

  val hardLimit = 10000
  val defaultLimit = 100

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
    val ret = for {
      js <- parse[Option[JsObject]](jsValue, "interval")
      fromJs <- parse[Option[JsObject]](js, "from")
      toJs <- parse[Option[JsObject]](js, "to")
    } yield {
        val from = Management.toProps(label, fromJs)
        val to = Management.toProps(label, toJs)
        (from, to)
      }
    //    Logger.debug(s"extractInterval: $ret")
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
          (labelMeta.seq -> value)
        }
      }
    ret.map(_.toMap).getOrElse(Map.empty[Byte, InnerValLike])
  }

  //  def hasOrWhere(label: HLabel, jsValue: JsValue): Set[InnerVal] = {
  //    for {
  //      (propName, v) <- jsValue.as[JsObject].fields
  //      propMeta <- HLabelMeta.findByName(label.id.get, propName)
  ////      innerVal <- jsValueToInnerVal(v, propMeta.dataType)
  //    }  yield {
  //      propName -> (v match {
  //        case arr: JsArray => // set
  //          Set(arr.as[List[JsValue]].flatMap { e =>
  //            jsValueToInnerVal(e, propMeta.dataType)
  //          })
  //        case value: JsValue => // exact
  //          Set(List(v).flatMap { jsValue =>
  //            jsValueToInnerVal(jsValue, propMeta.dataType)
  //          })
  //        case obj: JsObject => // from, to
  //          val (fromJsVal, toJsVal) = ((obj \ "from").as[JsValue], (obj \ "to").as[JsValue])
  //          val (from, to) = (toInnerVal(fromJsVal), toInnerVal(toJsVal))
  //          (from, to)
  //      })
  //    }
  //  }
  def extractWhere(label: Label, jsValue: JsValue) = {
    (jsValue \ "where").asOpt[String].flatMap { where =>
      WhereParser(label).parse(where)
    }
  }

  def toQuery(jsValue: JsValue, isEdgeQuery: Boolean = true): Query = {
    try {
      val vertices =
        (for {
          value <- parse[List[JsValue]](jsValue, "srcVertices")
          serviceName = parse[String](value, "serviceName")
          service <- Service.findByName(serviceName)
          column <- parse[Option[String]](value, "columnName")
          col <- ServiceColumn.find(service.id.get, column)
        } yield {
            val (idOpt, idsOpt) = ((value \ "id").asOpt[JsValue], (value \ "ids").asOpt[List[JsValue]])
            val idVals = (idOpt, idsOpt) match {
              case (Some(id), None) => List(id)
              case (None, Some(ids)) => ids
              case (Some(id), Some(ids)) => id :: ids
              case (None, None) => List.empty[JsValue]
            }

            for {
              idVal <- idVals

              /** bug, need to use labels schemaVersion  */
              innerVal <- jsValueToInnerVal(idVal, col.columnType, col.schemaVersion)
            } yield {
              Vertex(SourceVertexId(col.id.get, innerVal), System.currentTimeMillis())
            }
          }).flatten

      val steps = parse[List[JsValue]](jsValue, "steps")
      val stepLength = steps.size
      val removeCycle = (jsValue \ "removeCycle").asOpt[Boolean].getOrElse(true)
      val querySteps =
        steps.map { step =>
          val queryParams =
            for {
              labelGroup <- step.as[List[JsValue]]
              labelName <- parse[Option[String]](labelGroup, "label")
              label <- Label.findByName(labelName)
            } yield {
              val direction = parse[Option[String]](labelGroup, "direction").map(GraphUtil.toDirection(_)).getOrElse(0)
              val limit = {
                parse[Option[Int]](labelGroup, "limit") match {
                  case None => Math.min(defaultLimit, Math.pow(hardLimit, stepLength).toInt)
                  case Some(l) if l < 0 => Int.MaxValue
                  case Some(l) if l >= 0 => Math.min(l, Math.pow(hardLimit, stepLength).toInt)
                }
                //              Math.min(parse[Option[Int]](labelGroup, "limit").getOrElse(10), Math.pow(hardLimit, stepLength).toInt)
              }
              val offset = parse[Option[Int]](labelGroup, "offset").getOrElse(0)
              val interval = extractInterval(label, labelGroup)
              val duration = extractDuration(label, labelGroup)
              val scorings = extractScoring(label.id.get, labelGroup).getOrElse(List.empty[(Byte, Double)]).toList
              val labelOrderSeq = label.findLabelIndexSeq(scorings)
              val exclude = parse[Option[Boolean]](labelGroup, "exclude").getOrElse(false)
              val include = parse[Option[Boolean]](labelGroup, "include").getOrElse(false)
              val hasFilter = extractHas(label, labelGroup)
              val outputField = for (of <- (labelGroup \ "outputField").asOpt[String]; labelMeta <- LabelMeta.findByName(label.id.get, of)) yield labelMeta.seq
              val labelWithDir = LabelWithDirection(label.id.get, direction)
              val indexSeq = label.indexSeqsMap.get(scorings.map(kv => kv._1).toList).map(x => x.seq).getOrElse(LabelIndex.defaultSeq)
              val where = extractWhere(label, labelGroup)
              val includeDegree = (labelGroup \ "includeDegree").asOpt[Boolean].getOrElse(true)
              val rpcTimeout = (labelGroup \ "rpcTimeout").asOpt[Int].getOrElse(1000)
              val maxAttempt = (labelGroup \ "maxAttempt").asOpt[Int].getOrElse(2)
              val tgtVertexInnerIdOpt = (labelGroup \ "_to").asOpt[JsValue].flatMap { jsVal =>
                jsValueToInnerVal(jsVal, label.tgtColumnWithDir(direction).columnType, label.schemaVersion)
              }
              val cacheTTL = (labelGroup \ "cacheTTL").asOpt[Long].getOrElse(-1L)
              // TODO: refactor this. dirty
              val duplicate = parse[Option[String]](labelGroup, "duplicate").map(s => Query.DuplicatePolicy(s))
              QueryParam(labelWithDir).labelOrderSeq(labelOrderSeq)
                .limit(offset, limit)
                .rank(RankParam(label.id.get, scorings))
                .exclude(exclude)
                .include(include)
                .interval(interval)
                .duration(duration)
                .has(hasFilter)
                .labelOrderSeq(indexSeq)
                .outputField(outputField)
                .where(where)
                .duplicatePolicy(duplicate)
                .includeDegree(includeDegree)
                .rpcTimeout(rpcTimeout)
                .maxAttempt(maxAttempt)
                .tgtVertexInnerIdOpt(tgtVertexInnerIdOpt)
                .cacheTTLInMillis(cacheTTL)
            }
          Step(queryParams.toList)
        }

      val ret = Query(vertices, querySteps, removeCycle = removeCycle)
      //          Logger.debug(ret.toString)
      ret
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        throw new KGraphExceptions.BadQueryException(s"$jsValue")
    }
  }

  private def parse[R](js: JsValue, key: String)(implicit read: Reads[R]): R = {
    (js \ key).validate[R]
      .fold(
        errors => {
          val msg = (JsError.toFlatJson(errors) \ "obj").as[List[JsValue]].map(x => x \ "msg")
          val e = Json.obj("args" -> key, "error" -> msg)
          throw new KGraphExceptions.JsonParseException(Json.obj("error" -> key).toString)
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

  private[RequestParser] def jsObjDuplicateKeyCheck(jsObj: JsObject) = {
    assert(jsObj != null)
    if (jsObj.fields.map(_._1).groupBy(_.toString).map(r => r match {
      case (k, v) => v
    }).filter(_.length > 1).isEmpty == false)
      throw new KGraphExceptions.JsonParseException(Json.obj("error" -> s"$jsObj --> some key is duplicated").toString)
  }

  def parsePropsElements(jsValue: JsValue) = {
    for {
      jsObj <- jsValue.asOpt[List[JsValue]].getOrElse(List.empty)
    } yield {
      val propName = (jsObj \ "name").as[String]
      val dataType = InnerVal.toInnerDataType((jsObj \ "dataType").as[String])
      val defaultValue = (jsObj \ "defaultValue")
      (propName, defaultValue, dataType)
    }
  }

  def toLabelElements(jsValue: JsValue) = {
    val labelName = parse[String](jsValue, "label")
    val srcServiceName = parse[String](jsValue, "srcServiceName")
    val tgtServiceName = parse[String](jsValue, "tgtServiceName")
    val srcColumnName = parse[String](jsValue, "srcColumnName")
    val tgtColumnName = parse[String](jsValue, "tgtColumnName")
    val srcColumnType = parse[String](jsValue, "srcColumnType")
    val tgtColumnType = parse[String](jsValue, "tgtColumnType")
    val serviceName = (jsValue \ "serviceName").asOpt[String].getOrElse(tgtServiceName)
    //    parse[String](jsValue, "serviceName")
    val isDirected = (jsValue \ "isDirected").asOpt[Boolean].getOrElse(true)
    val idxProps = parsePropsElements((jsValue \ "indexProps"))
    val metaProps = parsePropsElements((jsValue \ "props"))
    val consistencyLevel = (jsValue \ "consistencyLevel").asOpt[String].getOrElse("weak")
    // expect new label don`t provide hTableName
    val hTableName = (jsValue \ "hTableName").asOpt[String]
    val hTableTTL = (jsValue \ "hTableTTL").asOpt[Int]
    val schemaVersion = (jsValue \ "schemaVersion").asOpt[String].getOrElse(InnerVal.DEFAULT_VERSION)
    val isAsync = (jsValue \ "isAsync").asOpt[Boolean].getOrElse(false)
    val t = (labelName, srcServiceName, srcColumnName, srcColumnType,
      tgtServiceName, tgtColumnName, tgtColumnType, isDirected, serviceName,
      idxProps, metaProps, consistencyLevel, hTableName, hTableTTL, schemaVersion, isAsync)
    Logger.info(s"createLabel $t")
    t
  }

  def toIndexElements(jsValue: JsValue) = {
    val labelName = parse[String](jsValue, "label")
    val idxProps = parsePropsElements((jsValue \ "indexProps"))
    //    val js = (jsValue \ "indexProps").asOpt[JsObject].getOrElse(Json.parse("{}").as[JsObject])
    //    val props = for ((k, v) <- js.fields) yield (k, v)
    val t = (labelName, idxProps)
    t
  }

  def toServiceElements(jsValue: JsValue) = {
    val serviceName = parse[String](jsValue, "serviceName")
    val cluster = (jsValue \ "cluster").asOpt[String].getOrElse(Graph.config.getString("hbase.zookeeper.quorum"))
    val hTableName = (jsValue \ "hTableName").asOpt[String].getOrElse(s"${serviceName}-${Config.PHASE}")
    val preSplitSize = (jsValue \ "preSplitSize").asOpt[Int].getOrElse(0)
    val hTableTTL = (jsValue \ "hTableTTL").asOpt[Int]
    (serviceName, cluster, hTableName, preSplitSize, hTableTTL)
  }

  def toVertexElements(jsValue: JsValue) = {
    val serviceName = parse[String](jsValue, "serviceName")
    val columnName = parse[String](jsValue, "columnName")
    val columnType = parse[String](jsValue, "columnType")
    val props = parsePropsElements(jsValue \ "props")
    (serviceName, columnName, columnType, props)
  }

  def toPropElements(jsValue: JsValue) = {
    val propName = parse[String](jsValue, "name")
    val defaultValue = parse[JsValue](jsValue, "defaultValue")
    val dataType = parse[String](jsValue, "dataType")
    val usedInIndex = parse[Option[Boolean]](jsValue, "usedInIndex").getOrElse(false)
    (propName, defaultValue, dataType, usedInIndex)
  }
}
