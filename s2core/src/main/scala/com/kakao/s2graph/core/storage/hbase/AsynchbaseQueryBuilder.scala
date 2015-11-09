package com.kakao.s2graph.core.storage.hbase

import java.util


import com.kakao.s2graph.core._
import com.kakao.s2graph.core.utils.{logger, Extensions}
import com.kakao.s2graph.core.storage.{QueryBuilder}
import com.kakao.s2graph.core.types.{VertexId, TargetVertexId, SourceVertexId, InnerVal}
import com.stumbleupon.async.{Deferred}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{GetRequest}
import scala.collection.JavaConversions._
import scala.collection.{Map, Seq}
import scala.concurrent.{Future, ExecutionContext}

class AsynchbaseQueryBuilder(storage: AsynchbaseStorage)(implicit ec: ExecutionContext)
  extends QueryBuilder[GetRequest, Deferred[QueryResult]](storage) {

  import Extensions.DeferOps

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

    def fetchInner: Deferred[QueryResult] = {
      val request = buildRequest(queryRequest)
      storage.client.get(request) withCallback { kvs =>
        val edgeWithScores = storage.toEdges(kvs.toSeq, queryRequest.queryParam, queryRequest.prevStepScore, queryRequest.isInnerCall, queryRequest.parentEdges)
        QueryResult(queryRequest.query, queryRequest.stepIdx, queryRequest.queryParam, edgeWithScores)
      } recoverWith { ex =>
        logger.error(s"fetchQueryParam failed. fallback return.", ex)
        QueryResult(queryRequest.query, queryRequest.stepIdx, queryRequest.queryParam)
      }
    }

    storage.cacheOpt match {
      case None => fetchInner
      case Some(cache) =>
        val queryParam = queryRequest.queryParam
        val request = buildRequest(queryRequest)
        val cacheKey = queryParam.toCacheKey(toCacheKeyBytes(request))

        def setCacheAfterFetch: Deferred[QueryResult] =
          fetchInner withCallback { queryResult: QueryResult =>
            cache.put(cacheKey, Seq(queryResult))
            queryResult
          }
        if (queryParam.cacheTTLInMillis > 0) {
          val cacheTTL = queryParam.cacheTTLInMillis
          if (cache.asMap().containsKey(cacheKey)) {
            val cachedVal = cache.asMap().get(cacheKey)
            if (cachedVal != null && cachedVal.nonEmpty && queryParam.timestamp - cachedVal.head.timestamp < cacheTTL)
              Deferred.fromResult(cachedVal.head)
            else
              setCacheAfterFetch
          } else
            setCacheAfterFetch
        } else
          setCacheAfterFetch
    }
  }

  override def toCacheKeyBytes(getRequest: GetRequest): Array[Byte] = {
    var bytes = getRequest.key()
    if (getRequest.family() != null) bytes = Bytes.add(bytes, getRequest.family())
    if (getRequest.qualifiers() != null) getRequest.qualifiers().filter(_ != null).foreach(q => bytes = Bytes.add(bytes, q))
    bytes
  }

  override def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): Deferred[QueryResult] = {
    //TODO:
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(tgtVertex.innerId))
    val q = Query.toQuery(Seq(srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, srcVertex, _queryParam, 1.0, Option(tgtVertex), isInnerCall = true)
    val fallback = QueryResult(q, 0, queryParam)
    fetch(queryRequest)
  }

  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryResult]] = {
    val defers: Seq[Deferred[QueryResult]] = for {
      queryRequest <- queryRequests
    } yield {
        val prevStepEdgesOpt = prevStepEdges.get(queryRequest.vertex.id)
        if (prevStepEdgesOpt.isEmpty) throw new RuntimeException("miss match on prevStepEdge and current GetRequest")

        val parentEdges = for {
          parentEdge <- prevStepEdgesOpt.get
        } yield parentEdge

        val newQueryRequest = queryRequest.copy(parentEdges = parentEdges)
        fetch(newQueryRequest)
      }
    val grouped: Deferred[util.ArrayList[QueryResult]] = Deferred.group(defers)
    grouped withCallback { queryResults: util.ArrayList[QueryResult] =>
      queryResults.toIndexedSeq
    } toFuture
  }
}
