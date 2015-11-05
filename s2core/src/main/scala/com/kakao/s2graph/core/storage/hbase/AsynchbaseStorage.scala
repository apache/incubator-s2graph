package com.kakao.s2graph.core.storage.hbase

import java.util
import java.util.ArrayList

import com.google.common.cache.Cache
import com.kakao.s2graph.core.GraphExceptions.FetchTimeoutException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{Label, LabelMeta}
import com.kakao.s2graph.core.storage.{SKeyValue, Storage}
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.DeferOps._
import com.kakao.s2graph.core.utils.{Extensions, logger}
import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async._

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, Seq}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, duration}
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Random, Success, Try}


object AsynchbaseStorage {
  val vertexCf = HSerializable.vertexCf
  val edgeCf = HSerializable.edgeCf
  val emptyKVs = new util.ArrayList[KeyValue]()
  private val maxValidEdgeListSize = 10000
  private val MaxBackOff = 10

  def makeClient(config: Config, overrideKv: (String, String)*) = {
    val asyncConfig: org.hbase.async.Config = new org.hbase.async.Config()

    for (entry <- config.entrySet() if entry.getKey.contains("hbase")) {
      asyncConfig.overrideConfig(entry.getKey, entry.getValue.unwrapped().toString)
    }

    for ((k, v) <- overrideKv) {
      asyncConfig.overrideConfig(k, v)
    }

    val client = new HBaseClient(asyncConfig)
    logger.info(s"Asynchbase: ${client.getConfig.dumpConfiguration()}")
    client
  }
}

class AsynchbaseStorage(config: Config, cache: Cache[Integer, Seq[QueryResult]], vertexCache: Cache[Integer, Option[Vertex]])
                       (implicit ec: ExecutionContext) extends Storage {

  import AsynchbaseStorage._
  import Extensions.FutureOps

  private val client = AsynchbaseStorage.makeClient(config)
  private val clientWithFlush = AsynchbaseStorage.makeClient(config, "hbase.rpcs.buffered_flush_interval" -> "0")
  private val clients = Seq(client, clientWithFlush)

  private val clientFlushInterval = config.getInt("hbase.rpcs.buffered_flush_interval").toString().toShort
  private val MaxRetryNum = config.getInt("max.retry.number")

  /**
   * Serializer/Deserializer
   */
  def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge) = new SnapshotEdgeSerializable(snapshotEdge)

  def indexEdgeSerializer(indexedEdge: IndexEdge) = new IndexEdgeSerializable(indexedEdge)

  def vertexSerializer(vertex: Vertex) = new VertexSerializable(vertex)

  val snapshotEdgeDeserializer = new SnapshotEdgeDeserializable
  val indexEdgeDeserializer = new IndexEdgeDeserializable
  val vertexDeserializer = new VertexDeserializable

  private def fetchStepFuture(queryResultLsFuture: Future[Seq[QueryResult]], q: Query, stepIdx: Int): Future[Seq[QueryResult]] = {
    for {
      queryResultLs <- queryResultLsFuture
      ret <- fetchStepWithFilter(queryResultLs, q, stepIdx)
    } yield {
      ret
    }
  }

  private def put(kvs: Seq[SKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }

  private def increment(kvs: Seq[SKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value)) }

  private def delete(kvs: Seq[SKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv =>
      if (kv.qualifier == null) new DeleteRequest(kv.table, kv.row, kv.cf, kv.timestamp)
      else new DeleteRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.timestamp)
    }

  def getEdges(q: Query): Future[Seq[QueryResult]] = {
    Try {
      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        Future.successful(q.vertices.map(v => QueryResult(query = q, stepIdx = 0, queryParam = QueryParam.Empty)))
      } else {
        val startQueryResultLs = QueryResult.fromVertices(q)
        q.steps.zipWithIndex.foldLeft(Future.successful(startQueryResultLs)) { case (acc, (_, idx)) =>
          fetchStepFuture(acc, q, idx)
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        Future.successful(q.vertices.map(v => QueryResult(query = q, stepIdx = 0, queryParam = QueryParam.Empty)))
    } get
  }

  def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryResult]] = {
    val futures = for {
      (srcVertex, tgtVertex, queryParam) <- params
    } yield getEdge(srcVertex, tgtVertex, queryParam, false)

    Future.sequence(futures)
  }

  def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = {
    def fromResult(queryParam: QueryParam,
                   kvs: Seq[org.hbase.async.KeyValue],
                   version: String): Option[Vertex] = {

      if (kvs.isEmpty) None
      else {
        val newKVs = kvs
        Option(vertexDeserializer.fromKeyValues(queryParam, newKVs, version, None))
      }
    }

    val futures = vertices.map { vertex =>
      val kvs = vertexSerializer(vertex).toKeyValues
      val get = new GetRequest(vertex.hbaseTableName.getBytes, kvs.head.row, vertexCf)
      //      get.setTimeout(this.singleGetTimeout.toShort)
      get.setFailfast(true)
      get.maxVersions(1)

      val cacheKey = MurmurHash3.stringHash(get.toString)
      val cacheVal = vertexCache.getIfPresent(cacheKey)
      if (cacheVal == null)
        deferredToFuture(client.get(get))(emptyKVs).map { kvs =>
          fromResult(QueryParam.Empty, kvs, vertex.serviceColumn.schemaVersion)
        }

      else Future.successful(cacheVal)
    }

    Future.sequence(futures).map { result => result.toList.flatten }
  }

  def deleteAllAdjacentEdges(srcVertices: List[Vertex],
                             labels: Seq[Label],
                             dir: Int,
                             ts: Long): Future[Boolean] = {
    val requestTs = ts
    val queryParams = for {
      label <- labels
    } yield {
        val labelWithDir = LabelWithDirection(label.id.get, dir)
        QueryParam(labelWithDir).limit(0, maxValidEdgeListSize * 5).duplicatePolicy(Option(Query.DuplicatePolicy.Raw))
      }

    val step = Step(queryParams.toList)
    val q = Query(srcVertices, Vector(step))

    for {
      queryResultLs <- getEdges(q)
      ret <- deleteAllFetchedEdgesLs(queryResultLs, requestTs, 0)
    } yield ret
  }

  def mutateElements(elements: Seq[GraphElement], withWait: Boolean): Future[Seq[Boolean]] = {
    val futures = elements.map {
      case edge: Edge => mutateEdge(edge, withWait)
      case vertex: Vertex => mutateVertex(vertex, withWait)
      case element => throw new RuntimeException(s"$element is not edge/vertex")
    }

    Future.sequence(futures)
  }

  private def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = mutateEdgeWithOp(edge, withWait)

  def mutateEdges(edges: Seq[Edge], withWait: Boolean): Future[Seq[Boolean]] = {
    val edgeGrouped = edges.groupBy { edge => (edge.label, edge.srcVertex.innerId, edge.tgtVertex.innerId) } toSeq

    val ret = edgeGrouped.map { case ((label, srcId, tgtId), edges) =>
      if (edges.isEmpty) Future.successful(true)
      else {
        val head = edges.head
        val strongConsistency = head.label.consistencyLevel == "strong"

        if (strongConsistency) {
          val edgeFuture = mutateEdgesInner(edges, edges.head.label.consistencyLevel == "strong", withWait)(Edge.buildOperation)
          //TODO: decide what we will do on failure on vertex put
          val vertexFuture = writeAsyncSimple(head.label.hbaseZkAddr, buildVertexPutsAsync(head))
          Future.sequence(Seq(edgeFuture, vertexFuture)).map { rets => rets.forall(identity) }
        } else {
          Future.sequence(edges.map { edge => mutateEdge(edge, withWait = withWait) }).map { rets => rets.forall(identity) }
        }
      }
    }
    Future.sequence(ret)
  }

  private def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = {
    if (vertex.op == GraphUtil.operations("delete")) {
      deleteVertex(vertex, withWait)
    } else if (vertex.op == GraphUtil.operations("deleteAll")) {
      logger.info(s"deleteAll for vertex is truncated. $vertex")
      Future.successful(true) // Ignore withWait parameter, because deleteAll operation may takes long time
    } else {
      if (withWait)
        writeAsyncWithWaitSimple(vertex.hbaseZkAddr, buildPutsAll(vertex))
      else
        writeAsyncSimple(vertex.hbaseZkAddr, buildPutsAll(vertex))
    }
  }

  def mutateVertices(vertices: Seq[Vertex], withWait: Boolean): Future[Seq[Boolean]] = {
    val futures = vertices.map { vertex => mutateVertex(vertex, withWait) }
    Future.sequence(futures)
  }

  def incrementCounts(edges: Seq[Edge]): Future[Seq[(Boolean, Long)]] = {
    val defers: Seq[Deferred[(Boolean, Long)]] = for {
      edge <- edges
    } yield {
        Try {
          val edgeWithIndex = edge.edgesWithIndex.head
          val countWithTs = edge.propsWithTs(LabelMeta.countSeq)
          val countVal = countWithTs.innerVal.toString().toLong
          val incr = buildIncrementsCountAsync(edgeWithIndex, countVal).head
          val request = incr.asInstanceOf[AtomicIncrementRequest]
          val defered = deferredCallbackWithFallback[java.lang.Long, (Boolean, Long)](client.bufferAtomicIncrement(request))({
            (resultCount: java.lang.Long) => (true, resultCount)
          }, (false, -1L))
          defered
        } match {
          case Success(r) => r
          case Failure(ex) => Deferred.fromResult((false, -1L))
        }
      }

    val grouped: Deferred[util.ArrayList[(Boolean, Long)]] = Deferred.groupInOrder(defers)
    deferredToFutureWithoutFallback(grouped).map(_.toSeq)
  }

  private def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): Future[QueryResult] = {
    //TODO:
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(tgtVertex.innerId))
    val q = Query.toQuery(Seq(srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, srcVertex, _queryParam, 1.0, Option(tgtVertex), isInnerCall = true)
    val fallback = QueryResult(q, 0, queryParam)

    deferredToFuture(fetchQueryParam(queryRequest))(fallback)
  }

  private def buildRequest(queryRequest: QueryRequest): GetRequest = {
    val srcVertex = queryRequest.vertex
    //    val tgtVertexOpt = queryRequest.tgtVertexOpt

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
      val kv = snapshotEdgeSerializer(snapshotEdge).toKeyValues.head
      new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf, kv.qualifier)
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == queryParam.labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)

      val indexedEdge = indexedEdgeOpt.get
      val kv = indexEdgeSerializer(indexedEdge).toKeyValues.head
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

  private def fetchhQueryParamWithCache(queryRequest: QueryRequest): Deferred[QueryResult] = {
    val queryParam = queryRequest.queryParam
    val cacheKey = queryParam.toCacheKey(buildRequest(queryRequest))

    def queryResultCallback(cacheKey: Int) = new Callback[QueryResult, QueryResult] {
      def call(arg: QueryResult): QueryResult = {
        cache.put(cacheKey, Seq(arg))
        arg
      }
    }
    if (queryParam.cacheTTLInMillis > 0) {
      val cacheTTL = queryParam.cacheTTLInMillis
      if (cache.asMap().containsKey(cacheKey)) {
        val cachedVal = cache.asMap().get(cacheKey)
        if (cachedVal != null && cachedVal.nonEmpty && queryParam.timestamp - cachedVal.head.timestamp < cacheTTL) {
          Deferred.fromResult(cachedVal.head)
        }
        else {
          fetchQueryParam(queryRequest).addBoth(queryResultCallback(cacheKey))
        }
      } else {
        fetchQueryParam(queryRequest).addBoth(queryResultCallback(cacheKey))
      }
    } else {
      fetchQueryParam(queryRequest).addBoth(queryResultCallback(cacheKey))
    }
  }

  private def toEdges(kvs: Seq[KeyValue], queryParam: QueryParam,
                      prevScore: Double = 1.0,
                      isInnerCall: Boolean,
                      parentEdges: Seq[EdgeWithScore]): Seq[(Edge, Double)] = {

    if (kvs.isEmpty) Seq.empty
    else {
      val first = kvs.head
      val kv = first
      val cacheElementOpt =
        if (queryParam.isSnapshotEdge) None
        else Option(indexEdgeDeserializer.fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion))

      for {
        kv <- kvs
        edge <-
        if (queryParam.isSnapshotEdge) toSnapshotEdge(kv, queryParam, None, isInnerCall, parentEdges)
        else toEdge(kv, queryParam, cacheElementOpt, parentEdges)
      } yield {
        //TODO: Refactor this.
        val currentScore =
          queryParam.scorePropagateOp match {
            case "plus" => edge.rank(queryParam.rank) + prevScore
            case _ => edge.rank(queryParam.rank) * prevScore
          }
        (edge, currentScore)
      }
    }
  }

  private def toSnapshotEdge(kv: KeyValue, param: QueryParam,
                             cacheElementOpt: Option[SnapshotEdge] = None,
                             isInnerCall: Boolean,
                             parentEdges: Seq[EdgeWithScore]): Option[Edge] = {
    val kvs = Seq(kv)
    val snapshotEdge = snapshotEdgeDeserializer.fromKeyValues(param, kvs, param.label.schemaVersion, cacheElementOpt)

    if (isInnerCall) {
      val edge = snapshotEdgeDeserializer.toEdge(snapshotEdge).copy(parentEdges = parentEdges)
      val ret = if (param.where.map(_.filter(edge)).getOrElse(true)) {
        Some(edge)
      } else {
        None
      }

      ret
    } else {
      if (Edge.allPropsDeleted(snapshotEdge.props)) None
      else {
        val edge = snapshotEdgeDeserializer.toEdge(snapshotEdge).copy(parentEdges = parentEdges)
        val ret = if (param.where.map(_.filter(edge)).getOrElse(true)) {
          logger.debug(s"fetchedEdge: $edge")
          Some(edge)
        } else {
          None
        }
        ret
      }
    }
  }

  private def toEdge(kv: KeyValue, param: QueryParam,
                     cacheElementOpt: Option[IndexEdge] = None,
                     parentEdges: Seq[EdgeWithScore]): Option[Edge] = {

    // logger.error(s"kv: => $kv")

    val kvs = Seq(kv)
    val edgeWithIndex = indexEdgeDeserializer.fromKeyValues(param, kvs, param.label.schemaVersion, cacheElementOpt)
    Option(indexEdgeDeserializer.toEdge(edgeWithIndex).copy(parentEdges = parentEdges))
  }

  private def fetchQueryParam(queryRequest: QueryRequest): Deferred[QueryResult] = {
    val successCallback = (kvs: util.ArrayList[KeyValue]) => {
      val edgeWithScores = toEdges(kvs.toSeq, queryRequest.queryParam, queryRequest.prevStepScore, queryRequest.isInnerCall, queryRequest.parentEdges)
      QueryResult(queryRequest.query, queryRequest.stepIdx, queryRequest.queryParam, edgeWithScores)
    }
    val fallback = QueryResult(queryRequest.query, queryRequest.stepIdx, queryRequest.queryParam)

    deferredCallbackWithFallback(client.get(buildRequest(queryRequest)))(successCallback, fallback)
  }

  private def fetchStep(queryRequests: Seq[QueryRequest],
                        prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Deferred[util.ArrayList[QueryResult]] = {
    val defers = for {
      queryRequest <- queryRequests
    } yield {
        val prevStepEdgesOpt = prevStepEdges.get(queryRequest.vertex.id)
        if (prevStepEdgesOpt.isEmpty) throw new RuntimeException("miss match on prevStepEdge and current GetRequest")

        val parentEdges = for {
          parentEdge <- prevStepEdgesOpt.get
        } yield parentEdge

        val newQueryRequest = queryRequest.copy(parentEdges = parentEdges)
        fetchhQueryParamWithCache(newQueryRequest)
      }
    Deferred.group(defers)
  }

  private def fetchStepWithFilter(queryResultsLs: Seq[QueryResult],
                                  q: Query,
                                  stepIdx: Int): Future[Seq[QueryResult]] = {

    val prevStepOpt = if (stepIdx > 0) Option(q.steps(stepIdx - 1)) else None
    val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.DefaultThreshold)
    val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
    val step = q.steps(stepIdx)
    val alreadyVisited =
      if (stepIdx == 0) Map.empty[(LabelWithDirection, Vertex), Boolean]
      else Graph.alreadyVisitedVertices(queryResultsLs)

    //TODO:
    val groupedBy = queryResultsLs.flatMap { queryResult =>
      queryResult.edgeWithScoreLs.map { case (edge, score) =>
        edge.tgtVertex -> (edge -> score)
      }
    }.groupBy { case (vertex, (edge, score)) => vertex }

    //    logger.debug(s"groupedBy: $groupedBy")
    val groupedByFiltered = for {
      (vertex, edgesWithScore) <- groupedBy
      aggregatedScore = edgesWithScore.map(_._2._2).sum if aggregatedScore >= prevStepThreshold
    } yield vertex -> aggregatedScore

    val prevStepTgtVertexIdEdges = for {
      (vertex, edgesWithScore) <- groupedBy
    } yield vertex.id -> edgesWithScore.map { case (vertex, (edge, score)) => EdgeWithScore(edge, score) }
    //    logger.debug(s"groupedByFiltered: $groupedByFiltered")

    val nextStepSrcVertices = if (prevStepLimit >= 0) {
      groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
    } else {
      groupedByFiltered.toSeq
    }
    //    logger.debug(s"nextStepSrcVertices: $nextStepSrcVertices")

    val queryRequests = for {
      (vertex, prevStepScore) <- nextStepSrcVertices
      queryParam <- step.queryParams
    } yield {
        QueryRequest(q, stepIdx, vertex, queryParam, prevStepScore, None, Nil, isInnerCall = false)
      }

    val fallback = queryRequests.map(request => QueryResult(q, stepIdx, request.queryParam))
    val groupedDefer = fetchStep(queryRequests, prevStepTgtVertexIdEdges)

    Graph.filterEdges(deferredToFuture(groupedDefer)(new ArrayList(fallback)), q, stepIdx, alreadyVisited)(ec)
  }

  /** edge Update **/
  private def indexedEdgeMutations(edgeUpdate: EdgeMutate): List[HBaseRpc] = {
    val deleteMutations = edgeUpdate.edgesToDelete.flatMap(edge => buildDeletesAsync(edge))
    val insertMutations = edgeUpdate.edgesToInsert.flatMap(edge => buildPutsAsync(edge))

    deleteMutations ++ insertMutations
  }

  private def invertedEdgeMutations(edgeUpdate: EdgeMutate): List[HBaseRpc] = {
    edgeUpdate.newInvertedEdge.map(e => buildDeleteAsync(e)).getOrElse(Nil)
  }

  private def increments(edgeUpdate: EdgeMutate): List[HBaseRpc] = {
    (edgeUpdate.edgesToDelete.isEmpty, edgeUpdate.edgesToInsert.isEmpty) match {
      case (true, true) =>

        /** when there is no need to update. shouldUpdate == false */
        List.empty[AtomicIncrementRequest]
      case (true, false) =>

        /** no edges to delete but there is new edges to insert so increase degree by 1 */
        edgeUpdate.edgesToInsert.flatMap { e => buildIncrementsAsync(e) }
      case (false, true) =>

        /** no edges to insert but there is old edges to delete so decrease degree by 1 */
        edgeUpdate.edgesToDelete.flatMap { e => buildIncrementsAsync(e, -1L) }
      case (false, false) =>

        /** update on existing edges so no change on degree */
        List.empty[AtomicIncrementRequest]
    }
  }

  /** EdgeWithIndex */
  private def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): List[HBaseRpc] = {
    indexEdgeSerializer(indexedEdge).toKeyValues.headOption match {
      case None => Nil
      case Some(kv) =>
        val copiedKV = kv.copy(qualifier = Array.empty[Byte], value = Bytes.toBytes(amount))
        increment(Seq(copiedKV)).toList
    }
  }

  private def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): List[HBaseRpc] = {
    indexEdgeSerializer(indexedEdge).toKeyValues.headOption match {
      case None => Nil
      case Some(kv) =>
        val copiedKV = kv.copy(value = Bytes.toBytes(amount))
        increment(Seq(copiedKV)).toList
    }
  }

  private def buildDeletesAsync(indexedEdge: IndexEdge): List[HBaseRpc] = {
    delete(indexEdgeSerializer(indexedEdge).toKeyValues).toList
  }

  private def buildPutsAsync(indexedEdge: IndexEdge): List[HBaseRpc] = {
    put(indexEdgeSerializer(indexedEdge).toKeyValues).toList
  }

  /** EdgeWithIndexInverted  */
  private def buildPutAsync(snapshotEdge: SnapshotEdge): List[HBaseRpc] = {
    put(snapshotEdgeSerializer(snapshotEdge).toKeyValues).toList
  }

  private def buildDeleteAsync(snapshotEdge: SnapshotEdge): List[HBaseRpc] = {
    delete(snapshotEdgeSerializer(snapshotEdge).toKeyValues).toList
  }

  /** Vertex */
  private def buildPutsAsync(vertex: Vertex): List[HBaseRpc] = {
    val kvs = vertexSerializer(vertex).toKeyValues
    put(kvs).toList
  }

  private def buildDeleteAsync(vertex: Vertex): List[HBaseRpc] = {

    val kvs = vertexSerializer(vertex).toKeyValues
    val kv = kvs.head
    delete(Seq(kv.copy(qualifier = null))).toList
  }

  private def buildPutsAll(vertex: Vertex): List[HBaseRpc] = {
    vertex.op match {
      case d: Byte if d == GraphUtil.operations("delete") => buildDeleteAsync(vertex)
      case _ => buildPutsAsync(vertex)
    }
  }

  /** */
  private def buildDeleteBelongsToId(vertex: Vertex): List[HBaseRpc] = {
    val kvs = vertexSerializer(vertex).toKeyValues
    val kv = kvs.head

    import org.apache.hadoop.hbase.util.Bytes
    val newKVs = vertex.belongLabelIds.map { id => kv.copy(qualifier = Bytes.toBytes(Vertex.toPropKey(id))) }
    delete(newKVs).toList
  }

  private def buildVertexPutsAsync(edge: Edge): List[HBaseRpc] =
    if (edge.op == GraphUtil.operations("delete"))
      buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
    else
      buildPutsAsync(edge.srcForVertex) ++ buildPutsAsync(edge.tgtForVertex)



//  private def writeAsyncWithWaitRetry(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]], retryNum: Int): Future[Seq[Boolean]] =
//    writeAsyncWithWait(zkQuorum, elementRpcs).flatMap { rets =>
//      val allSuccess = rets.forall(identity)
//      if (allSuccess) Future.successful(elementRpcs.map(_ => true))
//      else throw FetchTimeoutException("writeAsyncWithWaitRetry")
//    }.retryFallback(retryNum) {
//      logger.error(s"writeAsyncWithWaitRetry: $elementRpcs")
//      elementRpcs.map(_ => false)
//    }

  private def writeAsyncWithWaitRetrySimple(zkQuorum: String, elementRpcs: Seq[HBaseRpc], retryNum: Int): Future[Boolean] =
    writeAsyncWithWaitSimple(zkQuorum, elementRpcs).flatMap { ret =>
      if (ret) Future.successful(ret)
      else throw FetchTimeoutException("writeAsyncWithWaitRetrySimple")
    }.retryFallback(retryNum) {
      logger.error(s"writeAsyncWithWaitRetrySimple: $elementRpcs")
      false
    }

  private def writeAsyncWithWait(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]]): Future[Seq[Boolean]] = {
    val client = clientWithFlush
    if (elementRpcs.isEmpty) {
      Future.successful(Seq.empty[Boolean])
    } else {
      val defers = elementRpcs.map { rpcs =>
        val defer = rpcs.map { rpc =>
          // logger.error(rpc.toString)
          val deferred = rpc match {
            case d: DeleteRequest => client.delete(d).addErrback(errorBack(ex => logger.error(s"delete request failed. $d, $ex", ex)))
            case p: PutRequest => client.put(p).addErrback(errorBack(ex => logger.error(s"put request failed. $p, $ex", ex)))
            case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).addErrback(errorBack(ex => logger.error(s"increment request failed. $i, $ex", ex)))
          }

          deferredCallbackWithFallback(deferred)({
            (anyRef: Any) => anyRef match {
              case e: Exception =>
                logger.error(s"mutation failed. $e", e)
                false
              case _ => true
            }
          }, false)
        }

        deferredToFutureWithoutFallback(Deferred.group(defer)).map { arr => arr.forall(identity) }
      }

      Future.sequence(defers)
    }
  }

  private def writeAsyncWithWaitSimple(zkQuorum: String, elementRpcs: Seq[HBaseRpc]): Future[Boolean] = {
    if (elementRpcs.isEmpty) {
      Future.successful(true)
    } else {
      val defers = elementRpcs.map { rpc =>
        //        logger.error(rpc.toString)
        val deferred = rpc match {
          case d: DeleteRequest => client.delete(d).addErrback(errorBack(ex => logger.error(s"delete request failed. $d, $ex", ex)))
          case p: PutRequest => client.put(p).addErrback(errorBack(ex => logger.error(s"put request failed. $p, $ex", ex)))
          case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).addErrback(errorBack(ex => logger.error(s"increment request failed. $i, $ex", ex)))
        }

        deferredCallbackWithFallback(deferred)({
          (anyRef: Any) => anyRef match {
            case e: Exception =>
              logger.error(s"mutation failed. $e", e)
              false
            case _ => true
          }
        }, false)
      }
      deferredToFutureWithoutFallback(Deferred.group(defers)).map { arr => arr.forall(identity) }
    }
  }

  private def writeAsyncSimple(zkQuorum: String, elementRpcs: Seq[HBaseRpc]): Future[Boolean] = {
    if (elementRpcs.isEmpty) {
      Future.successful(true)
    } else {
      elementRpcs.foreach { rpc =>
        //        logger.error(rpc.toString)
        val deferred = rpc match {
          case d: DeleteRequest => client.delete(d).addErrback(errorBack(ex => logger.error(s"delete request failed. $d, $ex", ex)))
          case p: PutRequest => client.put(p).addErrback(errorBack(ex => logger.error(s"put request failed. $p, $ex", ex)))
          case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).addErrback(errorBack(ex => logger.error(s"increment request failed. $i, $ex", ex)))
        }

        deferredCallbackWithFallback(deferred)({
          (anyRef: Any) => anyRef match {
            case e: Exception =>
              logger.error(s"mutation failed. $e", e)
              false
            case _ => true
          }
        }, false)
      }
      Future.successful(true)
    }
  }

  private def writeAsync(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]]): Future[Seq[Boolean]] = {
    if (elementRpcs.isEmpty) {
      Future.successful(Seq.empty[Boolean])
    } else {
      elementRpcs.foreach { rpcs =>
        rpcs.foreach { rpc =>
          // logger.error(rpc.toString)
          val deferred = rpc match {
            case d: DeleteRequest => client.delete(d).addErrback(errorBack(ex => logger.error(s"delete request failed. $d, $ex", ex)))
            case p: PutRequest => client.put(p).addErrback(errorBack(ex => logger.error(s"put request failed. $p, $ex", ex)))
            case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).addErrback(errorBack(ex => logger.error(s"increment request failed. $i, $ex", ex)))
          }

          deferredCallbackWithFallback(deferred)({
            (anyRef: Any) => anyRef match {
              case e: Exception =>
                logger.error(s"mutation failed. $e", e)
                false
              case _ => true
            }
          }, false)
        }
      }

      Future.successful(elementRpcs.map(x => true))
    }
  }


  private def fetchInvertedAsync(edge: Edge): Future[(QueryParam, Option[Edge])] = {
    val labelWithDir = edge.labelWithDir
    val queryParam = QueryParam(labelWithDir)

    getEdge(edge.srcVertex, edge.tgtVertex, queryParam, isInnerCall = true).map { queryResult =>
      (queryParam, queryResult.edgeWithScoreLs.headOption.map { case (e, _) => e })
    }
  }

  private def commitPending(snapshotEdgeOpt: Option[Edge]): Future[Boolean] = {
    val pendingEdges =
      if (snapshotEdgeOpt.isEmpty || snapshotEdgeOpt.get.pendingEdgeOpt.isEmpty) Nil
      else Seq(snapshotEdgeOpt.get.pendingEdgeOpt.get)

    if (pendingEdges == Nil) Future.successful(true)
    else {
      val snapshotEdge = snapshotEdgeOpt.get
      // 1. commitPendingEdges
      // after: state without pending edges
      // before: state with pending edges

      val after = buildPutAsync(snapshotEdge.toSnapshotEdge.withNoPendingEdge()).head.asInstanceOf[PutRequest]
      val before = snapshotEdgeSerializer(snapshotEdge.toSnapshotEdge).toKeyValues.head.value
      for {
        pendingEdgesLock <- mutateEdges(pendingEdges, withWait = true)
        ret <- if (pendingEdgesLock.forall(identity)) deferredToFutureWithoutFallback(client.compareAndSet(after, before)).map(_.booleanValue())
        else Future.successful(false)
      } yield ret
    }
  }


  private def commitUpdate(edgeWriter: EdgeWriter)(snapshotEdgeOpt: Option[Edge], edgeUpdate: EdgeMutate, retryNum: Int): Future[Boolean] = {
    val edge = edgeWriter.edge
    val label = edgeWriter.label

    if (edgeUpdate.newInvertedEdge.isEmpty) Future.successful(true)
    else {
      val lock = buildPutAsync(edgeUpdate.newInvertedEdge.get.withPendingEdge(Option(edge))).head.asInstanceOf[PutRequest]
      val before = snapshotEdgeOpt.map(old => snapshotEdgeSerializer(old.toSnapshotEdge).toKeyValues.head.value).getOrElse(Array.empty[Byte])
      val after = buildPutAsync(edgeUpdate.newInvertedEdge.get.withNoPendingEdge()).head.asInstanceOf[PutRequest]

      def indexedEdgeMutationFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else writeAsyncWithWait(label.hbaseZkAddr, Seq(indexedEdgeMutations(edgeUpdate))).map { indexedEdgesUpdated =>
          indexedEdgesUpdated.forall(identity)
        }
      }
      def indexedEdgeIncrementFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else writeAsyncWithWaitRetrySimple(label.hbaseZkAddr, increments(edgeUpdate), MaxRetryNum).map { allSuccess =>
          //          if (!allSuccess) logger.error(s"indexedEdgeIncrement failed: $edgeUpdate")
          //          else logger.debug(s"indexedEdgeIncrement success: $edgeUpdate")
          allSuccess
        }
      }
      val fallback = Future.successful(false)
      val javaFallback = Future.successful[java.lang.Boolean](false)

      /**
       * step 1. acquire lock on snapshot edge.
       * step 2. try mutate indexed Edge mutation. note that increment is seperated for retry cases.
       * step 3. once all mutate on indexed edge success, then try release lock.
       * step 4. once lock is releaseed successfully, then mutate increment on this edgeUpdate.
       * note thta step 4 never fail to avoid multiple increments.
       */
      for {
        locked <- deferredToFutureWithoutFallback(client.compareAndSet(lock, before))
        indexEdgesUpdated <- indexedEdgeMutationFuture(locked)
        releaseLock <- if (indexEdgesUpdated) deferredToFutureWithoutFallback(client.compareAndSet(after, lock.value())) else javaFallback
        indexEdgesIncremented <- if (releaseLock) indexedEdgeIncrementFuture(releaseLock) else fallback
      } yield indexEdgesIncremented
    }
  }

  private def mutateEdgesInner(edges: Seq[Edge],
                               checkConsistency: Boolean,
                               withWait: Boolean)(f: (Option[Edge], Seq[Edge]) => (Edge, EdgeMutate), tryNum: Int = 0): Future[Boolean] = {

    if (!checkConsistency) {
      val zkQuorum = edges.head.label.hbaseZkAddr
      val futures = edges.map { edge =>
        val (newEdge, edgeUpdate) = f(None, Seq(edge))
        val mutations = indexedEdgeMutations(edgeUpdate) ++ invertedEdgeMutations(edgeUpdate) ++ increments(edgeUpdate)
        if (withWait) writeAsyncWithWaitSimple(zkQuorum, mutations)
        else writeAsyncSimple(zkQuorum, mutations)
      }
      Future.sequence(futures).map { rets => rets.forall(identity) }
    } else {
      if (tryNum >= MaxRetryNum) {
        logger.error(s"mutate failed after $tryNum retry")
        edges.foreach { edge => ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = edge)) }
        Future.successful(false)
      } else {
        fetchInvertedAsync(edges.head) flatMap { case (queryParam, snapshotEdgeOpt) =>
          val (newEdge, edgeUpdate) = f(snapshotEdgeOpt, edges)
          if (edgeUpdate.newInvertedEdge.isEmpty) Future.successful(true)
          else {
            val waitTime = Random.nextInt(MaxBackOff) + 1
            commitPending(snapshotEdgeOpt).flatMap { case pendingAllCommitted =>
              if (pendingAllCommitted) {
                commitUpdate(EdgeWriter(newEdge))(snapshotEdgeOpt, edgeUpdate, tryNum).flatMap { case updateCommitted =>
                  if (!updateCommitted) {
                    Thread.sleep(waitTime)
                    logger.info(s"mutate failed $tryNum.")
                    mutateEdgesInner(edges, checkConsistency, withWait)(f, tryNum + 1)
                  } else {
                    logger.debug(s"mutate success $tryNum.")
                    Future.successful(true)
                  }
                }
              } else {
                Thread.sleep(waitTime)
                logger.info(s"mutate failed $tryNum.")
                mutateEdgesInner(edges, checkConsistency, withWait)(f, tryNum + 1)
              }
            }
          }
        }
      }
    }

  }

  private def mutateEdgeInner(edgeWriter: EdgeWriter,
                              checkConsistency: Boolean,
                              withWait: Boolean)
                             (f: (Option[Edge], Edge) => (Edge, EdgeMutate), tryNum: Int = 0): Future[Boolean] = {

    val edge = edgeWriter.edge
    val zkQuorum = edge.label.hbaseZkAddr
    if (!checkConsistency) {
      val (newEdge, update) = f(None, edge)
      val mutations = indexedEdgeMutations(update) ++ invertedEdgeMutations(update) ++ increments(update)
      if (withWait) writeAsyncWithWaitSimple(zkQuorum, mutations)
      else writeAsyncSimple(zkQuorum, mutations)
    } else {
      if (tryNum >= MaxRetryNum) {
        logger.error(s"mutate failed after $tryNum retry")
        ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = edge))
        Future.successful(false)
      } else {
        val waitTime = Random.nextInt(MaxBackOff) + 1

        fetchInvertedAsync(edge).flatMap { case (queryParam, snapshotEdgeOpt) =>
          val (newEdge, edgeUpdate) = f(snapshotEdgeOpt, edge)

          /** if there is no changes to be mutate, then just return true */
          if (edgeUpdate.newInvertedEdge.isEmpty) Future.successful(true)
          else
            commitPending(snapshotEdgeOpt).flatMap { case pendingAllCommitted =>
              if (pendingAllCommitted) {
                commitUpdate(edgeWriter)(snapshotEdgeOpt, edgeUpdate, tryNum).flatMap { case updateCommitted =>
                  if (!updateCommitted) {
                    Thread.sleep(waitTime)
                    logger.info(s"mutate failed $tryNum.\nretry $edge")
                    mutateEdgeInner(edgeWriter, checkConsistency, withWait = true)(f, tryNum + 1)
                  } else {
                    logger.debug(s"mutate success $tryNum.\n${edgeUpdate.toLogString}\n$edge")
                    Future.successful(true)
                  }
                }
              } else {
                Thread.sleep(waitTime)
                mutateEdgeInner(edgeWriter, checkConsistency, withWait = true)(f, tryNum + 1)
              }
            }

        }
      }
    }
  }

  private def mutateEdgeWithOp(edge: Edge, withWait: Boolean): Future[Boolean] = {
    val edgeWriter = EdgeWriter(edge)
    val zkQuorum = edge.label.hbaseZkAddr
    val rpcLs = new ListBuffer[HBaseRpc]()
    // all cases, it is necessary to insert vertex.
    rpcLs.appendAll(buildVertexPutsAsync(edge))

    val vertexMutateFuture =
      if (withWait) writeAsyncWithWaitSimple(zkQuorum, rpcLs)
      else writeAsyncSimple(zkQuorum, rpcLs)

    val edgeMutateFuture = edge.op match {
      case op if op == GraphUtil.operations("insert") =>
        edge.label.consistencyLevel match {
          case "strong" => // upsert
            mutateEdgeInner(edgeWriter, checkConsistency = true, withWait = withWait)(Edge.buildUpsert)
          case _ => // insert
            mutateEdgeInner(edgeWriter, checkConsistency = false, withWait = withWait)(Edge.buildInsertBulk)
        }

      case op if op == GraphUtil.operations("delete") =>
        edge.label.consistencyLevel match {
          case "strong" => // delete
            mutateEdgeInner(edgeWriter, checkConsistency = true, withWait = withWait)(Edge.buildDelete)
          case _ => // deleteBulk
            mutateEdgeInner(edgeWriter, checkConsistency = false, withWait = withWait)(Edge.buildDeleteBulk)
        }

      case op if op == GraphUtil.operations("update") =>
        mutateEdgeInner(edgeWriter, checkConsistency = true, withWait = withWait)(Edge.buildUpdate)

      case op if op == GraphUtil.operations("increment") =>
        mutateEdgeInner(edgeWriter, checkConsistency = true, withWait = withWait)(Edge.buildIncrement)

      case op if op == GraphUtil.operations("insertBulk") =>
        mutateEdgeInner(edgeWriter, checkConsistency = false, withWait = withWait)(Edge.buildInsertBulk)
      //        rpcLs.appendAll(edgeWriter.insert(createRelEdges = true))

      case _ =>
        logger.error(s"not supported operation on edge: ${edge.op}, $edge")
        throw new RuntimeException(s"operation ${edge.op} is not supported on edge.")
    }

    edgeMutateFuture
  }


  private def deleteVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = {
    if (withWait)
      writeAsyncWithWait(vertex.hbaseZkAddr, Seq(vertex).map(buildDeleteAsync(_))).map(_.forall(identity))
    else
      writeAsync(vertex.hbaseZkAddr, Seq(vertex).map(buildDeleteAsync(_))).map(_.forall(identity))
  }

  private def deleteAllFetchedEdgesAsync(queryResult: QueryResult,
                                         requestTs: Long,
                                         retryNum: Int = 0): Future[Boolean] = {
    val queryParam = queryResult.queryParam
    val queryResultToDelete = queryResult.edgeWithScoreLs.filter { case (edge, score) =>
      (edge.ts < requestTs) && !edge.propsWithTs.containsKey(LabelMeta.degreeSeq)
    }

    if (queryResultToDelete.isEmpty) {
      Future.successful(true)
    } else {
      val edgesToDelete = queryResultToDelete.flatMap { case (edge, score) =>
        edge.copy(op = GraphUtil.operations("delete"), ts = requestTs, version = requestTs).relatedEdges
      }
      mutateEdges(edgesToDelete, withWait = true).map { rets => rets.forall(identity) }
    }
  }

  private def deleteAllFetchedEdgesLs(queryResultLs: Seq[QueryResult], requestTs: Long,
                                      retryNum: Int = 0): Future[Boolean] = {
    if (retryNum > MaxRetryNum) {
      logger.error(s"deleteDuplicateEdgesLs failed. ${queryResultLs}")
      Future.successful(false)
    } else {
      val futures = for {
        queryResult <- queryResultLs
      } yield deleteAllFetchedEdgesAsync(queryResult, requestTs, 0)

      Future.sequence(futures).flatMap { rets =>
        val allSuccess = rets.forall(identity)

        if (!allSuccess) deleteAllFetchedEdgesLs(queryResultLs, requestTs, retryNum + 1)
        else Future.successful(allSuccess)
      }
    }
  }

  def flush(): Unit = clients.foreach { client =>
    val timeout = Duration((clientFlushInterval + 10) * 20, duration.MILLISECONDS)
    Await.result(deferredToFutureWithoutFallback(client.flush()), timeout)
  }

}
