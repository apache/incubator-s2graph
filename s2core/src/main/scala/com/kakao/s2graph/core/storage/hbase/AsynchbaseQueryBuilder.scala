package com.kakao.s2graph.core.storage.hbase

import java.util

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.utils.{logger, Extensions}
import com.kakao.s2graph.core.storage.{Storage, SKeyValue, CanSKeyValue, QueryBuilder}
import com.kakao.s2graph.core.types.{TargetVertexId, SourceVertexId, InnerVal}
import com.stumbleupon.async.{Callback, Deferred}
import org.hbase.async.{KeyValue, GetRequest}
import scala.collection.JavaConversions._

class AsynchbaseQueryBuilder(storage: AsynchbaseStorage) extends QueryBuilder[GetRequest, Deferred[QueryResult]] {

  override def buildRequest(queryRequest: QueryRequest): GetRequest = {
    val srcVertex = queryRequest.vertex
    //    val tgtVertexOpt = queryRequest.tgtVertexOpt
    val edgeCf = HSerializable.edgeCf

    val queryParam = queryRequest.queryParam
    val tgtVertexIdOpt = queryParam.tgtVertexInnerIdOpt
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val (srcInnerId, tgtInnerId) = tgtVertexIdOpt match {
      case Some(tgtVertexId) => // _to is given.
        /** we use toInvertedEdgeHashLike so dont need to swap src, tgt */
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        val tgt = InnerVal.convertVersion(tgtVertexId, tgtColumn.columnType, label.schemaVersion)
        (src, tgt)
      case None =>
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        (src, src)
    }

    val (srcVId, tgtVId) = (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val edge = Edge(srcV, tgtV, labelWithDir)

    val get = if (tgtVertexIdOpt.isDefined) {
      val snapshotEdge = edge.toSnapshotEdge
      val kv = storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues.head
      new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf, kv.qualifier)
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == queryParam.labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)

      val indexedEdge = indexedEdgeOpt.get
      val kv = storage.indexEdgeSerializer(indexedEdge).toKeyValues.head
      val table = label.hbaseTableName.getBytes
      val rowKey = kv.row
      val cf = edgeCf
      new GetRequest(table, rowKey, cf)
    }

    val (minTs, maxTs) = queryParam.duration.getOrElse((0L, Long.MaxValue))

    get.maxVersions(1)
    get.setFailfast(true)
    get.setMaxResultsPerColumnFamily(queryParam.limit)
    get.setRowOffsetPerColumnFamily(queryParam.offset)
    get.setMinTimestamp(minTs)
    get.setMaxTimestamp(maxTs)
    get.setTimeout(queryParam.rpcTimeoutInMillis)

    if (queryParam.columnRangeFilter != null) get.setFilter(queryParam.columnRangeFilter)

    get
  }

  override def fetch(queryRequest: QueryRequest): Deferred[QueryResult] = {
    val request = buildRequest(queryRequest)
    val successCallback = (kvs: util.ArrayList[KeyValue]) => {
      val edgeWithScores = storage.toEdges(kvs.toSeq, queryRequest.queryParam, queryRequest.prevStepScore, queryRequest.isInnerCall, queryRequest.parentEdges)
      QueryResult(queryRequest.query, queryRequest.stepIdx, queryRequest.queryParam, edgeWithScores)
    }
    val fallback = (ex: Exception) => {
      logger.error(s"fetchQueryParam failed. fallback return.", ex)
      QueryResult(queryRequest.query, queryRequest.stepIdx, queryRequest.queryParam)
    }
    storage.client.get(buildRequest(queryRequest)).withCallback { successCallback } recoverWith { fallback }
  }
}
