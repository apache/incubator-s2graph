package com.kakao.s2graph.core

import com.kakao.s2graph.core.Graph.edgeCf
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.parsers.{Where, WhereParser}
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.logger
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{ColumnRangeFilter, GetRequest}
import play.api.libs.json.{JsNumber, JsValue, Json}

import scala.util.hashing.MurmurHash3
import scala.util.{Success, Try}

object Query {
  val initialScore = 1.0
  lazy val empty = Query()

  def toQuery(srcVertices: Seq[Vertex], queryParam: QueryParam) = Query(srcVertices, Vector(Step(List(queryParam))))

  object DuplicatePolicy extends Enumeration {
    type DuplicatePolicy = Value
    val First, Sum, CountSum, Raw = Value

    def apply(policy: String): Value = {
      policy match {
        case "sum" => Query.DuplicatePolicy.Sum
        case "countSum" => Query.DuplicatePolicy.CountSum
        case "raw" => Query.DuplicatePolicy.Raw
        case _ => DuplicatePolicy.First
      }
    }
  }

}

case class Query(vertices: Seq[Vertex] = Seq.empty[Vertex],
                 steps: IndexedSeq[Step] = Vector.empty[Step],
                 unique: Boolean = true,
                 removeCycle: Boolean = false,
                 selectColumns: Seq[String] = Seq.empty[String],
                 groupByColumns: Seq[String] = Seq.empty[String],
                 filterOutQuery: Option[Query] = None,
                 filterOutFields: Seq[String] = Seq(LabelMeta.to.name),
                 withScore: Boolean = true,
                 returnTree: Boolean = false) {
  lazy val selectColumnsSet = selectColumns.map { c =>
    if (c == "_from") "from"
    else if (c == "_to") "to"
    else c
  }.toSet

  /** return logical query id without considering parameter values */
  def templateId(): JsValue = {
    Json.toJson(for {
      step <- steps
      queryParam <- step.queryParams.sortBy(_.labelWithDir.labelId)
    } yield {
        Json.obj("label" -> queryParam.label.label, "direction" -> GraphUtil.fromDirection(queryParam.labelWithDir.dir))
      })
  }

  def impressionId(): JsNumber = {
    val hash = MurmurHash3.stringHash(templateId().toString())
    JsNumber(hash)
  }
}

object EdgeTransformer {
  val DefaultTransformField = Json.arr("_to")
  val DefaultTransformFieldAsList = Json.arr("_to").as[List[String]]
  val DefaultJson = Json.arr(DefaultTransformField)
}

/**
 * TODO: step wise outputFields should be used with nextStepLimit, nextStepThreshold.
 * @param jsValue
 */
case class EdgeTransformer(queryParam: QueryParam, jsValue: JsValue) {
  val Delimiter = "\\$"
  val targets = jsValue.asOpt[List[Vector[String]]].toList
  val fieldsLs = for {
    target <- targets
    fields <- target
  } yield fields
  val isDefault = fieldsLs.size == 1 && fieldsLs.head.size == 1 && (fieldsLs.head.head == "_to" || fieldsLs.head.head == "to")

  def replace(fmt: String,
              values: Seq[InnerValLike],
              nextStepOpt: Option[Step]): Seq[InnerValLike] = {

    val tokens = fmt.split(Delimiter)
    val mergedStr = tokens.zip(values).map { case (prefix, innerVal) => prefix + innerVal.toString }.mkString
    //    logger.error(s"${tokens.toList}, ${values}, $mergedStr")
    //    println(s"${tokens.toList}, ${values}, $mergedStr")
    nextStepOpt match {
      case None =>
        val columnType =
          if (queryParam.labelWithDir.dir == GraphUtil.directions("out")) queryParam.label.tgtColumnType
          else queryParam.label.srcColumnType

        if (columnType == InnerVal.STRING) Seq(InnerVal.withStr(mergedStr, queryParam.label.schemaVersion))
        else Nil
      case Some(nextStep) =>
        val nextQueryParamsValid = nextStep.queryParams.filter { qParam =>
          if (qParam.labelWithDir.dir == GraphUtil.directions("out")) qParam.label.srcColumnType == "string"
          else qParam.label.tgtColumnType == "string"
        }
        for {
          nextQueryParam <- nextQueryParamsValid
        } yield {
          InnerVal.withStr(mergedStr, nextQueryParam.label.schemaVersion)
        }
    }
  }

  def toInnerValOpt(edge: Edge, fieldName: String): Option[InnerValLike] = {
    fieldName match {
      case LabelMeta.to.name => Option(edge.tgtVertex.innerId)
      case LabelMeta.from.name => Option(edge.srcVertex.innerId)
      case _ =>
        for {
          labelMeta <- queryParam.label.metaPropsInvMap.get(fieldName)
          value <- edge.propsWithTs.get(labelMeta.seq)
        } yield value.innerVal
    }
  }

  def transform(edge: Edge, nextStepOpt: Option[Step]): Seq[Edge] = {
    if (isDefault) Seq(edge)
    else {
      val edges = for {
        fields <- fieldsLs
        innerVal <- {
          if (fields.size == 1) {
            val fieldName = fields.head
            toInnerValOpt(edge, fieldName).toSeq
          } else {
            val fmt +: fieldNames = fields
            replace(fmt, fieldNames.flatMap(fieldName => toInnerValOpt(edge, fieldName)), nextStepOpt)
          }
        }
      } yield edge.updateTgtVertex(innerVal).copy(originalEdgeOpt = Option(edge))


      edges
    }
  }
}

object Step {
  val Delimiter = "|"
}
case class Step(queryParams: List[QueryParam],
                labelWeights: Map[Int, Double] = Map.empty,
                //                scoreThreshold: Double = 0.0,
                nextStepScoreThreshold: Double = 0.0,
                nextStepLimit: Int = -1,
                cacheTTL: Long = -1) {

  lazy val excludes = queryParams.filter(_.exclude)
  lazy val includes = queryParams.filterNot(_.exclude)
  lazy val excludeIds = excludes.map(x => x.labelWithDir.labelId -> true).toMap

  logger.debug(s"Step: $queryParams, $labelWeights, $nextStepScoreThreshold, $nextStepLimit")

  def toCacheKey(lss: Iterable[(GetRequest, QueryParam)]): Int = {
    val s = "step" + Step.Delimiter +
      lss.map { case (getRequest, param) => param.toCacheKey(getRequest) } mkString(Step.Delimiter)
    MurmurHash3.stringHash(s)
  }
}

case class VertexParam(vertices: Seq[Vertex]) {
  var filters: Option[Map[Byte, InnerValLike]] = None

  def has(what: Option[Map[Byte, InnerValLike]]): VertexParam = {
    what match {
      case None => this
      case Some(w) => has(w)
    }
  }

  def has(what: Map[Byte, InnerValLike]): VertexParam = {
    this.filters = Some(what)
    this
  }

}

object RankParam {
  def apply(labelId: Int, keyAndWeights: Seq[(Byte, Double)]) = {
    new RankParam(labelId, keyAndWeights)
  }
}

class RankParam(val labelId: Int, var keySeqAndWeights: Seq[(Byte, Double)] = Seq.empty[(Byte, Double)]) {
  // empty => Count
  lazy val rankKeysWeightsMap = keySeqAndWeights.toMap

  def defaultKey() = {
    this.keySeqAndWeights = List((LabelMeta.countSeq, 1.0))
    this
  }

  //
  //  def singleKey(key: String) = {
  //    this.keySeqAndWeights =
  //      LabelMeta.findByName(labelId, key) match {
  //        case None => List.empty[(Byte, Double)]
  //        case Some(ktype) => List((ktype.seq, 1.0))
  //      }
  //    this
  //  }
  //
  //  def multipleKey(keyAndWeights: Seq[(String, Double)]) = {
  //    this.keySeqAndWeights =
  //      for ((key, weight) <- keyAndWeights; row <- LabelMeta.findByName(labelId, key)) yield (row.seq, weight)
  //    this
  //  }
}

object QueryParam {
  lazy val Empty = QueryParam(LabelWithDirection(0, 0))
  lazy val DefaultThreshold = Double.MinValue
  val Delimiter = ","
}

case class QueryParam(labelWithDir: LabelWithDirection, timestamp: Long = System.currentTimeMillis()) {

  import HBaseSerializable._
  import Query.DuplicatePolicy
  import Query.DuplicatePolicy._

  lazy val label = Label.findById(labelWithDir.labelId)
  val DefaultKey = LabelIndex.DefaultSeq
  val fullKey = DefaultKey

  var labelOrderSeq = fullKey

  var limit = 10
  var offset = 0
  var rank = new RankParam(labelWithDir.labelId, List(LabelMeta.countSeq -> 1))

  var duration: Option[(Long, Long)] = None
  var isInverted: Boolean = false

  var columnRangeFilter: ColumnRangeFilter = null


  var hasFilters: Map[Byte, InnerValLike] = Map.empty[Byte, InnerValLike]
  var where: Try[Where] = Success(WhereParser.success)
  var duplicatePolicy = DuplicatePolicy.First
  var rpcTimeoutInMillis = 1000
  var maxAttempt = 2
  var includeDegree = false
  var tgtVertexInnerIdOpt: Option[InnerValLike] = None
  var cacheTTLInMillis: Long = -1L
  var threshold = QueryParam.DefaultThreshold
  var timeDecay: Option[TimeDecay] = None
  var transformer: EdgeTransformer = EdgeTransformer(this, EdgeTransformer.DefaultJson)
  var scorePropagateOp: String = "multiply"
  var exclude = false
  var include = false

  lazy val srcColumnWithDir = label.srcColumnWithDir(labelWithDir.dir)
  lazy val tgtColumnWithDir = label.tgtColumnWithDir(labelWithDir.dir)

  /**
   * consider only I/O specific parameters.
   * properties that is used on Graph.filterEdges should not be considered.
   * @param getRequest
   * @return
   */
  def toCacheKey(getRequest: GetRequest): Int = {
    val s = Seq(getRequest, labelWithDir, labelOrderSeq, offset, limit, rank,
//      duration,
      isInverted,
      columnRangeFilter).mkString(QueryParam.Delimiter)
    MurmurHash3.stringHash(s)
  }

  def isInverted(isInverted: Boolean): QueryParam = {
    this.isInverted = isInverted
    this
  }

  def labelOrderSeq(labelOrderSeq: Byte): QueryParam = {
    this.labelOrderSeq = labelOrderSeq
    this
  }

  def limit(offset: Int, limit: Int): QueryParam = {
    /** since degree info is located on first always */
    if (offset == 0) {
      this.limit = limit + 1
      this.offset = offset
    } else {
      this.limit = limit
      this.offset = offset + 1
    }
    //    this.columnPaginationFilter = new ColumnPaginationFilter(this.limit, this.offset)
    this
  }

  def interval(fromTo: Option[(Seq[(Byte, InnerValLike)], Seq[(Byte, InnerValLike)])]): QueryParam = {
    fromTo match {
      case Some((from, to)) => interval(from, to)
      case _ => this
    }
  }

  def interval(from: Seq[(Byte, InnerValLike)], to: Seq[(Byte, InnerValLike)]): QueryParam = {
    //    val len = label.orderTypes.size.toByte
    //    val len = label.extraIndicesMap(labelOrderSeq).sortKeyTypes.size.toByte
    //    logger.error(s"indicesMap: ${label.indicesMap(labelOrderSeq)}")
    val len = label.indicesMap(labelOrderSeq).sortKeyTypes.size.toByte

    val minMetaByte = InnerVal.minMetaByte
    //    val maxMetaByte = InnerVal.maxMetaByte
    val maxMetaByte = -1.toByte
    val toVal = Bytes.add(propsToBytes(to), Array.fill(1)(minMetaByte))
    //FIXME
    val fromVal = Bytes.add(propsToBytes(from), Array.fill(10)(maxMetaByte))
    toVal(0) = len
    fromVal(0) = len
    val maxBytes = fromVal
    val minBytes = toVal
    val rangeFilter = new ColumnRangeFilter(minBytes, true, maxBytes, true)
    this.columnRangeFilter = rangeFilter
    this
  }

  def duration(minMaxTs: Option[(Long, Long)]): QueryParam = {
    minMaxTs match {
      case Some((minTs, maxTs)) => duration(minTs, maxTs)
      case _ => this
    }
  }

  def duration(minTs: Long, maxTs: Long): QueryParam = {
    this.duration = Some((minTs, maxTs))
    this
  }

  def rank(r: RankParam): QueryParam = {
    this.rank = r
    this
  }

  def exclude(filterOut: Boolean): QueryParam = {
    this.exclude = filterOut
    this
  }

  def include(filterIn: Boolean): QueryParam = {
    this.include = filterIn
    this
  }

  def has(hasFilters: Map[Byte, InnerValLike]): QueryParam = {
    this.hasFilters = hasFilters
    this
  }

  def where(whereTry: Try[Where]): QueryParam = {
    this.where = whereTry
    this
  }

  def duplicatePolicy(policy: Option[DuplicatePolicy]): QueryParam = {
    this.duplicatePolicy = policy.getOrElse(DuplicatePolicy.First)
    this
  }

  def rpcTimeout(millis: Int): QueryParam = {
    this.rpcTimeoutInMillis = millis
    this
  }

  def maxAttempt(attempt: Int): QueryParam = {
    this.maxAttempt = attempt
    this
  }

  def includeDegree(includeDegree: Boolean): QueryParam = {
    this.includeDegree = includeDegree
    this
  }

  def tgtVertexInnerIdOpt(other: Option[InnerValLike]): QueryParam = {
    this.tgtVertexInnerIdOpt = other
    this
  }

  def cacheTTLInMillis(other: Long): QueryParam = {
    this.cacheTTLInMillis = other
    this
  }

  def timeDecay(other: Option[TimeDecay]): QueryParam = {
    this.timeDecay = other
    this
  }

  def threshold(other: Double): QueryParam = {
    this.threshold = other
    this
  }

  def transformer(other: Option[JsValue]): QueryParam = {
    other match {
      case Some(js) => this.transformer = EdgeTransformer(this, js)
      case None =>
    }
    this
  }

  def scorePropagateOp(scorePropagateOp: String): QueryParam = {
    this.scorePropagateOp = scorePropagateOp
    this
  }

  def isSnapshotEdge = tgtVertexInnerIdOpt.isDefined

  override def toString = {
    List(label.label, labelOrderSeq, offset, limit, rank,
      duration, isInverted, exclude, include, hasFilters).mkString("\t")
    //      duration, isInverted, exclude, include, hasFilters, outputFields).mkString("\t")
  }

  def buildGetRequest(srcVertex: Vertex) = {
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val (srcInnerId, tgtInnerId) = tgtVertexInnerIdOpt match {
      case Some(tgtVertexInnerId) => // _to is given.
        /** we use toInvertedEdgeHashLike so dont need to swap src, tgt */
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        val tgt = InnerVal.convertVersion(tgtVertexInnerId, tgtColumn.columnType, label.schemaVersion)
        (src, tgt)
      case None =>
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        (src, src)
    }

    val (srcVId, tgtVId) = (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val edge = Edge(srcV, tgtV, labelWithDir)

    val get = if (tgtVertexInnerIdOpt.isDefined) {
      val snapshotEdge = edge.toInvertedEdgeHashLike
      val kv = snapshotEdge.kvs.head
      new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf, kv.qualifier)
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)
      val indexedEdge = indexedEdgeOpt.get
      val kv = indexedEdge.kvs.head
      val table = label.hbaseTableName.getBytes
        //kv.table //
      val rowKey = kv.row // indexedEdge.rowKey.bytes
      val cf = edgeCf
      new GetRequest(table, rowKey, cf)
    }

    val (minTs, maxTs) = duration.getOrElse((0L, Long.MaxValue))

    get.maxVersions(1)
    get.setFailfast(true)
    get.setMaxResultsPerColumnFamily(limit)
    get.setRowOffsetPerColumnFamily(offset)
    get.setMinTimestamp(minTs)
    get.setMaxTimestamp(maxTs)
    get.setTimeout(rpcTimeoutInMillis)
    if (columnRangeFilter != null) get.setFilter(columnRangeFilter)
    //    get.setMaxAttempt(maxAttempt.toByte)
    //    get.setRpcTimeout(rpcTimeoutInMillis)

    //    if (columnRangeFilter != null) get.filter(columnRangeFilter)
    //    logger.debug(s"Get: $get, $offset, $limit")

    get
  }
}

case class TimeDecay(initial: Double = 1.0, lambda: Double = 0.1, timeUnit: Double = 60 * 60 * 24) {
  def decay(diff: Double): Double = {
    //FIXME
    val ret = initial * Math.pow(1.0 - lambda, diff / timeUnit)
    //    logger.debug(s"$initial, $lambda, $timeUnit, $diff, ${diff / timeUnit}, $ret")
    ret
  }
}
