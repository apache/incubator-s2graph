package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.mysqls._
//import com.daumkakao.s2graph.core.models._
import com.daumkakao.s2graph.core.parsers.Where
import com.daumkakao.s2graph.core.types2._
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.util.Bytes
import GraphConstant._
import org.hbase.async.{ScanFilter, ColumnRangeFilter}

object Query {
  val initialScore = 1.0
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
case class Query(vertices: Seq[Vertex], steps: List[Step],
  unique: Boolean = true, removeCycle: Boolean = false) {
}
case class Step(queryParams: List[QueryParam]) {
  lazy val excludes = queryParams.filter(qp => qp.exclude)
  lazy val includes = queryParams.filterNot(qp => qp.exclude)
  lazy val excludeIds = excludes.map(x => x.labelWithDir.labelId -> true).toMap
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
class RankParam(val labelId: Int, var keySeqAndWeights: Seq[(Byte, Double)] = Seq.empty[(Byte, Double)]) { // empty => Count
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


case class QueryParam(labelWithDir: LabelWithDirection) {

  import Query.DuplicatePolicy._
  import Query.DuplicatePolicy

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
  var rpcTimeoutInMillis = 100
  var maxAttempt = 2
  var includeDegree = false

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
    //    Logger.error(s"index length: $len")
    val minMetaByte = InnerVal.minMetaByte
    val maxMetaByte = InnerVal.maxMetaByte
    val toVal = Bytes.add(propsToBytes(to), Array.fill(1)(minMetaByte))
    val fromVal = Bytes.add(propsToBytes(from), Array.fill(1)(maxMetaByte))
    toVal(0) = len
    fromVal(0) = len
    val maxBytes = fromVal
    val minBytes = toVal
    val rangeFilter = new ColumnRangeFilter(minBytes, true, maxBytes, true)
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

  override def toString(): String = {
    List(label.label, labelOrderSeq, offset, limit, rank, isRowKeyOnly,
      duration, isInverted, exclude, include, hasFilters, outputField).mkString("\t")
  }


  def buildGetRequest(srcVertex: Vertex) = {
    val id = InnerVal.convertVersion(srcVertex.innerId, srcVertex.serviceColumn.columnType, label.schemaVersion)
    val vId = SourceVertexId(srcVertex.id.colId, id)
    val sourceVertexId = VertexId.toSourceVertexId(vId)
    val rowKey = EdgeRowKey(sourceVertexId, labelWithDir, labelOrderSeq, isInverted)(label.schemaVersion)
    val (minTs, maxTs) = duration.getOrElse((0L, Long.MaxValue))
    val client = Graph.getClient(label.hbaseZkAddr)
    val filters = ListBuffer.empty[ScanFilter]
    Graph.singleGet(label.hbaseTableName.getBytes, rowKey.bytes, edgeCf, offset, limit, minTs, maxTs, maxAttempt, rpcTimeoutInMillis, columnRangeFilter)
  }
}
