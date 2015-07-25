package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.mysqls._
import play.api.libs.json.{JsValue, Json}

import scala.util.hashing.MurmurHash3

//import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.parsers.Where
import com.daumkakao.s2graph.core.types2._
import play.api.Logger
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.util.Bytes
import GraphConstant._
import org.hbase.async.{GetRequest, ScanFilter, ColumnRangeFilter}

object Query {
  val initialScore = 1.0
  lazy val empty = Query()

  def toQuery(srcVertices: Seq[Vertex], queryParam: QueryParam) = {
    Query(srcVertices, List(Step(List(queryParam))))
  }

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
                 steps: List[Step] = List.empty[Step],
                 unique: Boolean = true,
                 removeCycle: Boolean = false,
                 selectColumns: Seq[String] = Seq.empty[String],
                 groupByColumns: Seq[String] = Seq.empty[String]) {
  lazy val selectColumnsSet = selectColumns.map{ c =>
    if (c == "_from") "from"
    else if (c == "_to") "to"
    else c
  }.toSet
  lazy val selectColumnSetIsEmpty = selectColumnsSet.isEmpty

  lazy val labelSrcTgtInvertedMap = if (vertices.isEmpty) {
    Map.empty[Int, Boolean]
  } else {
    (for {
      step <- steps
      param <- step.queryParams
    } yield {
        param.label.id.get -> {
          param.label.srcColumn.columnName != vertices.head.serviceColumn.columnName
        }

      }).toMap
  }
  /** return logical query id without considering parameter values */
  def templateId(): JsValue = {
    Json.toJson(for {
      step <- steps
      queryParam <- step.queryParams.sortBy(_.labelWithDir.labelId)
    } yield {
      Json.obj("label" -> queryParam.label.label, "direction" -> GraphUtil.fromDirection(queryParam.labelWithDir.dir))
    })
  }
  def impressionId(): Int = {
    val hash = MurmurHash3.stringHash(templateId().toString())

    hash
  }
}

case class Step(queryParams: List[QueryParam],
                labelWeights: Map[Int, Double] = Map.empty,
//                scoreThreshold: Double = 0.0,
                nextStepScoreThreshold: Double = 0.0,
                nextStepLimit: Int = -1) {
  lazy val excludes = queryParams.filter(qp => qp.exclude)
  lazy val includes = queryParams.filterNot(qp => qp.exclude)
  lazy val excludeIds = excludes.map(x => x.labelWithDir.labelId -> true).toMap
  Logger.debug(s"Step: $queryParams, $labelWeights, $nextStepScoreThreshold, $nextStepLimit")
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

  def singleKey(key: String) = {
    this.keySeqAndWeights =
      LabelMeta.findByName(labelId, key) match {
        case None => List.empty[(Byte, Double)]
        case Some(ktype) => List((ktype.seq, 1.0))
      }
    this
  }

  def multipleKey(keyAndWeights: Seq[(String, Double)]) = {
    this.keySeqAndWeights =
      for ((key, weight) <- keyAndWeights; row <- LabelMeta.findByName(labelId, key)) yield (row.seq, weight)
    this
  }
}

object QueryParam {
  lazy val empty = QueryParam(LabelWithDirection(0, 0))
}
case class QueryParam(labelWithDir: LabelWithDirection, timestamp: Long = System.currentTimeMillis()) {

  import Query.DuplicatePolicy._
  import Query.DuplicatePolicy
  import HBaseSerializable._

  val label = Label.findById(labelWithDir.labelId)
  val defaultKey = LabelIndex.defaultSeq
  val fullKey = defaultKey

  var labelOrderSeq = fullKey

  var outputField: Option[Byte] = None
  //  var start = OrderProps.empty
  //  var end = OrderProps.empty
  var limit = 10
  var offset = 0
  var rank = new RankParam(labelWithDir.labelId, List(LabelMeta.countSeq -> 1))
  var isRowKeyOnly = false
  var duration: Option[(Long, Long)] = None

  //  var direction = 0
  //  var props = OrderProps.empty
  var isInverted: Boolean = false

  //  var filters = new FilterList(FilterList.Operator.MUST_PASS_ALL)
  //  val scanFilters = ListBuffer.empty[ScanFilter]
  //  var filters = new FilterList(List.empty[ScanFilter], FilterList.Operator.MUST_PASS_ALL)
  var columnRangeFilter: ColumnRangeFilter = null
  //  var columnPaginationFilter: ColumnPaginationFilter = null
  var exclude = false
  var include = false

  var hasFilters: Map[Byte, InnerValLike] = Map.empty[Byte, InnerValLike]
  //  var propsFilters: PropsFilter = PropsFilter()
  var where: Option[Where] = None
  var duplicatePolicy = DuplicatePolicy.First
  var rpcTimeoutInMillis = 1000
  var maxAttempt = 2
  var includeDegree = false
  var tgtVertexInnerIdOpt: Option[InnerValLike] = None
  var cacheTTLInMillis: Long = -1L
  var threshold = 0.0
  var timeDecay: Option[TimeDecay] = None
  var excludeBy: Option[String] = None

  val srcColumnWithDir = label.srcColumnWithDir(labelWithDir.dir)
  val tgtColumnWithDir = label.tgtColumnWithDir(labelWithDir.dir)

  def isRowKeyOnly(isRowKeyOnly: Boolean): QueryParam = {
    this.isRowKeyOnly = isRowKeyOnly
    this
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
    this.limit = if (offset == 0) limit + 1 else limit
    this.offset = offset
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
    import types2.HBaseDeserializable._
    //    val len = label.orderTypes.size.toByte
    //    val len = label.extraIndicesMap(labelOrderSeq).sortKeyTypes.size.toByte
    //    Logger.error(s"indicesMap: ${label.indicesMap(labelOrderSeq)}")
    val len = label.indicesMap(labelOrderSeq).sortKeyTypes.size.toByte

    val minMetaByte = InnerVal.minMetaByte
    val maxMetaByte = InnerVal.maxMetaByte
    val toVal = Bytes.add(propsToBytes(to), Array.fill(1)(minMetaByte))
    val fromVal = Bytes.add(propsToBytes(from), Array.fill(1)(maxMetaByte))
    toVal(0) = len
    fromVal(0) = len
    val maxBytes = fromVal
    val minBytes = toVal
    val rangeFilter = new ColumnRangeFilter(minBytes, true, maxBytes, true)
    Logger.debug(s"index length: $len, min: ${minBytes.toList}, max: ${maxBytes.toList}")
    //    queryLogger.info(s"Interval: ${rangeFilter.getMinColumn().toList} ~ ${rangeFilter.getMaxColumn().toList}: ${Bytes.compareTo(minBytes, maxBytes)}")
    //    this.filters.(rangeFilter)
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

  def outputField(ofOpt: Option[Byte]): QueryParam = {
    this.outputField = ofOpt
    this
  }

  def has(hasFilters: Map[Byte, InnerValLike]): QueryParam = {
    this.hasFilters = hasFilters
    this
  }

  def where(whereOpt: Option[Where]): QueryParam = {
    this.where = whereOpt
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
    this.maxAttempt = attempt;
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
  def excludeBy(other: Option[String]): QueryParam = {
    this.excludeBy = other
    this
  }
  override def toString(): String = {
    List(label.label, labelOrderSeq, offset, limit, rank, isRowKeyOnly,
      duration, isInverted, exclude, include, hasFilters, outputField).mkString("\t")
  }


  def buildGetRequest(srcVertex: Vertex) = {
    val (srcColumn, tgtColumn) =
      if (labelWithDir.dir == GraphUtil.directions("in") && label.isDirected) (label.tgtColumn, label.srcColumn)
      else (label.srcColumn, label.tgtColumn)
    val (srcInnerId, tgtInnerId) =
      //FIXME
      if (labelWithDir.dir == GraphUtil.directions("in") && tgtVertexInnerIdOpt.isDefined && label.isDirected) {
        // need to be swap src, tgt
        val tgtVertexInnerId = tgtVertexInnerIdOpt.get
        (InnerVal.convertVersion(tgtVertexInnerId, srcColumn.columnType, label.schemaVersion),
          InnerVal.convertVersion(srcVertex.innerId, tgtColumn.columnType, label.schemaVersion))
      } else {
        val tgtVertexInnerId = tgtVertexInnerIdOpt.getOrElse(srcVertex.innerId)
        (InnerVal.convertVersion(srcVertex.innerId, tgtColumn.columnType, label.schemaVersion),
          InnerVal.convertVersion(tgtVertexInnerId, srcColumn.columnType, label.schemaVersion))
      }
    val (srcVId, tgtVId) =
      (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val edge = Edge(srcV, tgtV, labelWithDir)

    val get =  if (tgtVertexInnerIdOpt.isDefined) {
      val snapshotEdge = edge.toInvertedEdgeHashLike()
      new GetRequest(label.hbaseTableName.getBytes, snapshotEdge.rowKey.bytes, edgeCf, snapshotEdge.qualifier.bytes)
    } else {
      val indexedEdgeOpt =  edge.edgesWithIndex.find(e => e.labelIndexSeq == labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)
      val indexedEdge = indexedEdgeOpt.get
      new GetRequest(label.hbaseTableName.getBytes, indexedEdge.rowKey.bytes, edgeCf)
    }

    val (minTs, maxTs) = duration.getOrElse((0L, Long.MaxValue))
    val client = Graph.getClient(label.hbaseZkAddr)
    val filters = ListBuffer.empty[ScanFilter]


    get.maxVersions(1)
    get.setFailfast(true)
    get.setMaxResultsPerColumnFamily(limit)
    get.setRowOffsetPerColumnFamily(offset)
    get.setMinTimestamp(minTs)
    get.setMaxTimestamp(maxTs)
    get.setMaxAttempt(maxAttempt.toByte)
    get.setRpcTimeout(rpcTimeoutInMillis)
    if (columnRangeFilter != null) get.filter(columnRangeFilter)
    Logger.debug(s"$get")
    get
  }
}

case class TimeDecay(initial: Double = 1.0, lambda: Double = 0.1, timeUnit: Double = 60 * 60 * 24) {
  def decay(diff: Double): Double = {
    //FIXME
    val ret = initial * Math.pow((1.0 - lambda), diff / timeUnit)
//    Logger.debug(s"$initial, $lambda, $timeUnit, $diff, ${diff / timeUnit}, $ret")
    ret
  }
}