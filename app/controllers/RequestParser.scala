package controllers

import com.daumkakao.s2graph.core.HBaseElement._
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.models._
import play.api.Logger
import play.api.libs.json._
import com.daumkakao.s2graph.rest.config.Config
import scala.util.parsing.combinator.JavaTokenParsers

trait RequestParser extends JSONParser {

  val hardLimit = 10000
  val defaultLimit = 100

  private def extractScoring(labelId: Int, value: JsValue) = {
    val ret = for {
      js <- parse[Option[JsObject]](value, "scoring")
    } yield {
      for {
        (k, v) <- js.fields
        labelOrderType <- HLabelMeta.findByName(labelId, k)
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
  def extractInterval(label: HLabel, jsValue: JsValue) = {
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
  def extractDuration(label: HLabel, jsValue: JsValue) = {
    for {
      js <- parse[Option[JsObject]](jsValue, "duration")
    } yield {
      val minTs = parse[Option[Long]](js, "from").getOrElse(Long.MaxValue)
      val maxTs = parse[Option[Long]](js, "to").getOrElse(Long.MinValue)
      (minTs, maxTs)
    }
  }
  def extractHas(label: HLabel, jsValue: JsValue) = {
    val ret = for {
      js <- parse[Option[JsObject]](jsValue, "has")
    } yield {
      for {
        (k, v) <- js.fields
        labelMeta <- HLabelMeta.findByName(label.id.get, k)
        value <- jsValueToInnerVal(v, labelMeta.dataType)
      } yield {
        (labelMeta.seq -> value)
      }
    }
    ret.map(_.toMap).getOrElse(Map.empty[Byte, InnerVal])
  }
  def hasOrWhere(jsValue: JsValue) = {
    for ((k, v) <- jsValue.as[JsObject].fields) yield {
      k -> (v match {
        case arr: JsArray => // set
          Set(arr.as[List[JsValue]].map { toInnerVal(_) })
        case value: JsValue => // exact
          Set(toInnerVal(value))
        case obj: JsObject => // from, to
          val (fromJsVal, toJsVal) = ((obj \ "from").as[JsValue], (obj \ "to").as[JsValue])
          val (from, to) = (toInnerVal(fromJsVal), toInnerVal(toJsVal))
          (from, to)
      })
    }
  }
  def extractWhere(label: HLabel, jsValue: JsValue) = {
    (jsValue \ "where").asOpt[String].flatMap { where =>
      WhereParser(label).parse(where)
    }
  }
  case class WhereParser(label: HLabel) extends JavaTokenParsers with JSONParser {

    val metaProps = label.metaPropsInvMap ++ Map(HLabelMeta.from.name -> HLabelMeta.from, HLabelMeta.to.name -> HLabelMeta.to)

    def where: Parser[Where] = rep(clause) ^^ (Where(_))

    def clause: Parser[Clause] = (predicate | parens) * (
      "and" ^^^ { (a: Clause, b: Clause) => And(a, b) } |
      "or" ^^^ { (a: Clause, b: Clause) => Or(a, b) })

    def parens: Parser[Clause] = "(" ~> clause <~ ")"

    def boolean = ("true" ^^^ (true) | "false" ^^^ (false))

    /** floating point is not supported yet **/
    def predicate = (
      (ident ~ "=" ~ ident | ident ~ "=" ~ decimalNumber | ident ~ "=" ~ stringLiteral) ^^ {
        case f ~ "=" ~ s =>
          metaProps.get(f) match {
            case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
            case Some(metaProp) =>
              Equal(metaProp.seq, toInnerVal(s, metaProp.dataType))
          }

      }
      | (ident ~ "between" ~ ident ~ "and" ~ ident | ident ~ "between" ~ decimalNumber ~ "and" ~ decimalNumber
        | ident ~ "between" ~ stringLiteral ~ "and" ~ stringLiteral) ^^ {
          case f ~ "between" ~ minV ~ "and" ~ maxV =>
            metaProps.get(f) match {
              case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
              case Some(metaProp) =>
                Between(metaProp.seq, toInnerVal(minV, metaProp.dataType), toInnerVal(maxV, metaProp.dataType))
            }
        }
        | (ident ~ "in" ~ "(" ~ rep(ident | decimalNumber | stringLiteral | "true" | "false" | ",") ~ ")") ^^ {
          case f ~ "in" ~ "(" ~ vals ~ ")" =>
            metaProps.get(f) match {
              case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
              case Some(metaProp) =>
                val values = vals.filter(v => v != ",").map { v =>
                  toInnerVal(v, metaProp.dataType)
                }
                IN(metaProp.seq, values.toSet)
            }
        })

    def parse(sql: String): Option[Where] = {
      parseAll(where, sql) match {
        case Success(r, q) => Some(r)
        case x => println(x); None
      }
    }
  }

  def toQuery(jsValue: JsValue, isEdgeQuery: Boolean = true): Query = {
    try {
      val vertices =
        (for {
          value <- parse[List[JsValue]](jsValue, "srcVertices")
          serviceName = parse[String](value, "serviceName")
          service <- HService.findByName(serviceName)
          column <- parse[Option[String]](value, "columnName")
          col <- HServiceColumn.find(service.id.get, column)
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
            innerVal <- jsValueToInnerVal(idVal, col.columnType)
          } yield {
            Vertex(CompositeId(col.id.get, innerVal, isEdgeQuery, true), System.currentTimeMillis())
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
              label <- parse[Option[String]](labelGroup, "label")
              label <- HLabel.findByName(label)
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
              val outputField = for (of <- (labelGroup \ "outputField").asOpt[String]; labelMeta <- HLabelMeta.findByName(label.id.get, of)) yield labelMeta.seq
              val labelWithDir = LabelWithDirection(label.id.get, direction)
              val indexSeq = label.indexSeqsMap.get(scorings.map(kv => kv._1).toList).map(x => x.seq).getOrElse(HLabelIndex.defaultSeq)
              val where = extractWhere(label, labelGroup)
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
            }
          Step(queryParams.toList)
        }

      val ret = Query(vertices, querySteps, removeCycle = removeCycle)
      //    Logger.debug(ret.toString)
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
      case o @ _ => s"${o}"
    }
    val tgtId = parse[JsValue](jsValue, "to") match {
      case s: JsString => s.as[String]
      case o @ _ => s"${o}"
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
    val ts = parse[Option[Long]](jsValue, "timestamp").getOrElse(System.currentTimeMillis() / 1000)
    val sName = if (serviceName.isEmpty) parse[String](jsValue, "serviceName") else serviceName.get
    val cName = if (columnName.isEmpty) parse[String](jsValue, "columnName") else columnName.get
    val props = (jsValue \ "props").asOpt[JsObject].getOrElse(Json.obj())
    Management.toVertex(ts, operation, id.toString, sName, cName, props.toString)
  }

  private[RequestParser] def jsObjDuplicateKeyCheck(jsObj: JsObject) = {
    assert(jsObj != null)
    if (jsObj.fields.map(_._1).groupBy(_.toString).map(r => r match { case (k, v) => v }).filter(_.length > 1).isEmpty == false)
      throw new KGraphExceptions.JsonParseException(Json.obj("error" -> s"$jsObj --> some key is duplicated").toString)
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
    val idxJs = (jsValue \ "indexProps").asOpt[JsObject].getOrElse(Json.parse("{}").as[JsObject])
    jsObjDuplicateKeyCheck(idxJs)
    val metaJs = (jsValue \ "props").asOpt[JsObject].getOrElse(Json.parse("{}").as[JsObject])
    jsObjDuplicateKeyCheck(metaJs)
    val idxProps = for ((k, v) <- idxJs.fields) yield (k, v)
    val metaProps = for ((k, v) <- metaJs.fields) yield (k, v)
    val consistencyLevel = (jsValue \ "consistencyLevel").asOpt[String].getOrElse("week")
    // expect new label don`t provide hTableName
    val hTableName = (jsValue \ "hTableName").asOpt[String]
    val hTableTTL = (jsValue \ "hTableTTL").asOpt[Int]
    val t = (labelName, srcServiceName, srcColumnName, srcColumnType,
      tgtServiceName, tgtColumnName, tgtColumnType, isDirected, serviceName,
      idxProps, metaProps, consistencyLevel, hTableName, hTableTTL)
    t
  }
  def toIndexElements(jsValue: JsValue) = {
    val labelName = parse[String](jsValue, "label")
    val js = (jsValue \ "indexProps").asOpt[JsObject].getOrElse(Json.parse("{}").as[JsObject])
    val props = for ((k, v) <- js.fields) yield (k, v)
    val t = (labelName, props)
    t
  }
  def toServiceElements(jsValue: JsValue) = {
    val serviceName = parse[String](jsValue, "serviceName")
    val cluster = (jsValue \ "cluster").asOpt[String].getOrElse(GraphConnection.defaultConfigs("hbase.zookeeper.quorum"))
    val hTableName = (jsValue \ "hTableName").asOpt[String].getOrElse(s"${serviceName}-${Config.PHASE}")
    val preSplitSize = (jsValue \ "preSplitSize").asOpt[Int].getOrElse(0)
    val hTableTTL = (jsValue \ "hTableTTL").asOpt[Int]
    (serviceName, cluster, hTableName, preSplitSize, hTableTTL)
  }
  def toPropElements(jsValue: JsValue) = {
    val propName = parse[String](jsValue, "name")
    val defaultValue = parse[JsValue](jsValue, "defaultValue")
    val dataType = parse[String](jsValue, "dataType")
    (propName, defaultValue, dataType)
  }
}