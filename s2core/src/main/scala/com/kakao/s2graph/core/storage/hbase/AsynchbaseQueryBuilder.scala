package com.kakao.s2graph.core.storage.hbase

import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.QueryBuilder
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.{Extensions, logger}
import com.stumbleupon.async.Deferred
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.GetRequest

import scala.collection.JavaConversions._
import scala.collection.{Map, Seq}
import scala.concurrent.{Promise, ExecutionContext, Future}

class AsynchbaseQueryBuilder(storage: AsynchbaseStorage)(implicit ec: ExecutionContext)
  extends QueryBuilder[GetRequest, Deferred[QueryRequestWithResult]](storage) {

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
    val currentTs = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs)).toMap
    val edge = Edge(srcV, tgtV, labelWithDir, propsWithTs = propsWithTs)

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

  override def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): Deferred[QueryRequestWithResult] = {
    //TODO:
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(tgtVertex.innerId))
    val q = Query.toQuery(Seq(srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, srcVertex, _queryParam)
    fetch(queryRequest, 1.0, isInnerCall = true, parentEdges = Nil)
  }

  val maxSize = storage.config.getInt("future.cache.max.size")
  val futureCacheTTL = storage.config.getInt("future.cache.max.idle.ttl")
  val futureCache = CacheBuilder.newBuilder()
  .initialCapacity(maxSize)
  .concurrencyLevel(Runtime.getRuntime.availableProcessors())
  .expireAfterAccess(futureCacheTTL, TimeUnit.MILLISECONDS)
  .maximumSize(maxSize).build[java.lang.Long, (Long, Deferred[QueryRequestWithResult])]()

  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Deferred[QueryRequestWithResult] = {

    def fetchInner(request: GetRequest) = {
      storage.client.get(request) withCallback { kvs =>
        val edgeWithScores = storage.toEdges(kvs.toSeq, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
        QueryRequestWithResult(queryRequest, QueryResult(edgeWithScores))
      } recoverWith { ex =>
        logger.error(s"fetchQueryParam failed. fallback return.", ex)
        QueryRequestWithResult(queryRequest, QueryResult(isFailure = true))
      }
    }
    def checkAndExpire(request: GetRequest, cacheKey: Long, cacheTTL: Long, cachedAt: Long, defer: Deferred[QueryRequestWithResult]) = {
      if (System.currentTimeMillis() >= cachedAt + cacheTTL) {
        futureCache.asMap().remove(cacheKey)
        val oldVal = futureCache.asMap().get(cacheKey)
        oldVal match {
          case null =>
            val newPromise = new Deferred[QueryRequestWithResult]()
            fetchInner(request) withCallback { queryRequestWithResult =>
              newPromise.callback(queryRequestWithResult)
              queryRequestWithResult
            }
            futureCache.put(cacheKey, (System.currentTimeMillis(), newPromise))
        }
        val (_, oldDefer) = futureCache.asMap().get(cacheKey)
        oldDefer
      } else defer
    }

    val queryParam = queryRequest.queryParam
    val cacheTTL = queryParam.cacheTTLInMillis
    val request = buildRequest(queryRequest)
    if (cacheTTL <= 0) fetchInner(request)
    else {
      val cacheKeyBytes = Bytes.add(queryRequest.query.cacheKeyBytes, toCacheKeyBytes(request))
      val cacheKey = queryParam.toCacheKey(cacheKeyBytes)

      val cacheVal = futureCache.asMap().get(cacheKey)
      cacheVal match {
        case null =>
          // here there is no promise set up for this cacheKey so we need to set promise on future cache.
          val promise = new Deferred[QueryRequestWithResult]()
          fetchInner(request) withCallback { queryRequestWithResult =>
            promise.callback(queryRequestWithResult)
            queryRequestWithResult
          }
          futureCache.put(cacheKey, (System.currentTimeMillis(), promise))

          // we are sure that at least one thread have set promise.
          val (cachedAt, defer) = futureCache.asMap().get(cacheKey)
          checkAndExpire(request, cacheKey, cacheTTL, cachedAt, defer)
        case (cachedAt, defer) =>
          checkAndExpire(request, cacheKey, cacheTTL, cachedAt, defer)
      }
    }
  }


  override def toCacheKeyBytes(getRequest: GetRequest): Array[Byte] = {
    var bytes = getRequest.key()
    Option(getRequest.family()).foreach(family => bytes = Bytes.add(bytes, family))
    Option(getRequest.qualifiers()).foreach {
      qualifiers =>
        qualifiers.filter(q => Option(q).isDefined).foreach {
          qualifier =>
            bytes = Bytes.add(bytes, qualifier)
        }
    }
    //    if (getRequest.family() != null) bytes = Bytes.add(bytes, getRequest.family())
    //    if (getRequest.qualifiers() != null) getRequest.qualifiers().filter(_ != null).foreach(q => bytes = Bytes.add(bytes, q))
    bytes
  }


  override def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = {
    val defers: Seq[Deferred[QueryRequestWithResult]] = for {
      (queryRequest, prevStepScore) <- queryRequestWithScoreLs
    } yield {
        val prevStepEdgesOpt = prevStepEdges.get(queryRequest.vertex.id)
        if (prevStepEdgesOpt.isEmpty) throw new RuntimeException("miss match on prevStepEdge and current GetRequest")

        val parentEdges = for {
          parentEdge <- prevStepEdgesOpt.get
        } yield parentEdge

        fetch(queryRequest, prevStepScore, isInnerCall = true, parentEdges)
      }

    val grouped: Deferred[util.ArrayList[QueryRequestWithResult]] = Deferred.group(defers)
    grouped withCallback {
      queryResults: util.ArrayList[QueryRequestWithResult] =>
        queryResults.toIndexedSeq
    } toFuture
  }
}
