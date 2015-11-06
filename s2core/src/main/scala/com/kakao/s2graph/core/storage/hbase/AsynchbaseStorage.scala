package com.kakao.s2graph.core.storage.hbase

import java.util
import java.util.ArrayList

import com.google.common.cache.Cache
import com.kakao.s2graph.core.GraphExceptions.FetchTimeoutException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{Label, LabelMeta}
import com.kakao.s2graph.core.storage.{QueryBuilder, SKeyValue, Storage}
import com.kakao.s2graph.core.types._
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
  import Extensions.DeferOps

  val client = AsynchbaseStorage.makeClient(config)
  val queryBuilder = new AsynchbaseQueryBuilder(this)

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
    } yield queryBuilder.getEdge(srcVertex, tgtVertex, queryParam, false).toFuture

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
        client.get(get).toFutureWith(emptyKVs).map { kvs =>
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

  private def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = {
    //    mutateEdgeWithOp(edge, withWait)
    val strongConsistency = edge.label.consistencyLevel == "strong"
    val edgeFuture =
      if (edge.op == GraphUtil.operations("delete") && !strongConsistency) {
        val zkQuorum = edge.label.hbaseZkAddr
        val (_, edgeUpdate) = Edge.buildDeleteBulk(None, edge)
        val mutations = indexedEdgeMutations(edgeUpdate) ++ invertedEdgeMutations(edgeUpdate) ++ increments(edgeUpdate)
        writeAsyncSimple(zkQuorum, mutations, withWait)
      } else {
        mutateEdgesInner(Seq(edge), strongConsistency, withWait)(Edge.buildOperation)
      }
    val vertexFuture = writeAsyncSimple(edge.label.hbaseZkAddr, buildVertexPutsAsync(edge), withWait)
    Future.sequence(Seq(edgeFuture, vertexFuture)).map { rets => rets.forall(identity) }
  }

  def mutateEdges(edges: Seq[Edge], withWait: Boolean): Future[Seq[Boolean]] = {
    val edgeGrouped = edges.groupBy { edge => (edge.label, edge.srcVertex.innerId, edge.tgtVertex.innerId) } toSeq

    val ret = edgeGrouped.map { case ((label, srcId, tgtId), edges) =>
      if (edges.isEmpty) Future.successful(true)
      else {
        val head = edges.head
        val strongConsistency = head.label.consistencyLevel == "strong"

        if (strongConsistency) {
          val edgeFuture = mutateEdgesInner(edges, strongConsistency, withWait)(Edge.buildOperation)
          //TODO: decide what we will do on failure on vertex put
          val vertexFuture = writeAsyncSimple(head.label.hbaseZkAddr, buildVertexPutsAsync(head), withWait)
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
      writeAsyncSimple(vertex.hbaseZkAddr, buildPutsAll(vertex), withWait)
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
        val edgeWithIndex = edge.edgesWithIndex.head
        val countWithTs = edge.propsWithTs(LabelMeta.countSeq)
        val countVal = countWithTs.innerVal.toString().toLong
        val incr = buildIncrementsCountAsync(edgeWithIndex, countVal).head
        val request = incr.asInstanceOf[AtomicIncrementRequest]
        client.bufferAtomicIncrement(request) withCallback { resultCount: java.lang.Long =>
          (true, resultCount.longValue())
        } recoverWith { ex =>
          logger.error(s"mutation failed. $request", ex)
          (false, -1L)
        }
      }

    val grouped: Deferred[util.ArrayList[(Boolean, Long)]] = Deferred.groupInOrder(defers)
    grouped.toFuture.map(_.toSeq)
  }


  private def fetchhQueryParamWithCache(queryRequest: QueryRequest): Deferred[QueryResult] = {
    val queryParam = queryRequest.queryParam
    val request = queryBuilder.buildRequest(queryRequest)

    val cacheKey = queryParam.toCacheKey(queryBuilder.toCacheKeyBytes(request))

    def setCacheAfterFetch =
      queryBuilder.fetch(queryRequest) withCallback { queryResult: QueryResult =>
        cache.put(cacheKey, Seq(queryResult)); queryResult
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
      queryResult.edgeWithScoreLs.map { case edgeWithScore =>
        edgeWithScore.edge.tgtVertex -> edgeWithScore
      }
    }.groupBy { case (vertex, edgeWithScore) => vertex }

    //    logger.debug(s"groupedBy: $groupedBy")
    val groupedByFiltered = for {
      (vertex, edgesWithScore) <- groupedBy
      aggregatedScore = edgesWithScore.map(_._2.score).sum if aggregatedScore >= prevStepThreshold
    } yield vertex -> aggregatedScore

    val prevStepTgtVertexIdEdges = for {
      (vertex, edgesWithScore) <- groupedBy
    } yield vertex.id -> edgesWithScore.map { case (vertex, edgeWithScore) => edgeWithScore }
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

    val future = groupedDefer.recoverWith { ex: Exception =>
      logger.error("fetch step failed. fallback return.", ex)
      new ArrayList(fallback)
    }.toFuture

    Graph.filterEdges(future, q, stepIdx, alreadyVisited)(ec)
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

  private def writeAsyncSimpleRetry(zkQuorum: String, elementRpcs: Seq[HBaseRpc], withWait: Boolean, retryNum: Int): Future[Boolean] =
    writeAsyncSimple(zkQuorum, elementRpcs, withWait).flatMap { ret =>
      if (ret) Future.successful(ret)
      else throw FetchTimeoutException("writeAsyncWithWaitRetrySimple")
    }.retryFallback(retryNum) {
      logger.error(s"writeAsyncWithWaitRetrySimple: $elementRpcs")
      false
    }

  private def writeToStorage(_client: HBaseClient, rpc: HBaseRpc): Deferred[Boolean] = {
    val defer = rpc match {
      case d: DeleteRequest => _client.delete(d)
      case p: PutRequest => _client.put(p)
      case i: AtomicIncrementRequest => _client.bufferAtomicIncrement(i)
    }
    defer withCallback { ret => true } recoverWith { ex =>
      logger.error(s"mutation failed. $rpc", ex)
      false
    }
  }

  private def writeAsyncSimple(zkQuorum: String, elementRpcs: Seq[HBaseRpc], withWait: Boolean): Future[Boolean] = {
    val _client = if (withWait) clientWithFlush else client
    if (elementRpcs.isEmpty) {
      Future.successful(true)
    } else {
      val defers = elementRpcs.map { rpc => writeToStorage(_client, rpc) }
      if (withWait)
        Deferred.group(defers).toFuture map { arr => arr.forall(identity) }
      else
        Future.successful(true)
    }
  }

  private def writeAsync(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]], withWait: Boolean): Future[Seq[Boolean]] = {
    val _client = if (withWait) clientWithFlush else client
    if (elementRpcs.isEmpty) {
      Future.successful(Seq.empty[Boolean])
    } else {
      val futures = elementRpcs.map { rpcs =>
        val defers = rpcs.map { rpc => writeToStorage(_client, rpc) }
        if (withWait)
          Deferred.group(defers).toFuture map { arr => arr.forall(identity) }
        else
          Future.successful(true)
      }
      if (withWait)
        Future.sequence(futures)
      else
        Future.successful(elementRpcs.map(_ => true))
    }
  }

  private def fetchInvertedAsync(edge: Edge): Future[(QueryParam, Option[Edge])] = {
    val labelWithDir = edge.labelWithDir
    val queryParam = QueryParam(labelWithDir)

    queryBuilder.getEdge(edge.srcVertex, edge.tgtVertex, queryParam, isInnerCall = true).toFuture map { queryResult =>
      (queryParam, queryResult.edgeWithScoreLs.headOption.map(_.edge))
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
        ret <- if (pendingEdgesLock.forall(identity)) client.compareAndSet(after, before).toFuture.map(_.booleanValue())
        else Future.successful(false)
      } yield ret
    }
  }


  private def commitUpdate(edge: Edge)(snapshotEdgeOpt: Option[Edge], edgeUpdate: EdgeMutate, retryNum: Int): Future[Boolean] = {
    val label = edge.label

    if (edgeUpdate.newInvertedEdge.isEmpty) Future.successful(true)
    else {
      val lock = buildPutAsync(edgeUpdate.newInvertedEdge.get.withPendingEdge(Option(edge))).head.asInstanceOf[PutRequest]
      val before = snapshotEdgeOpt.map(old => snapshotEdgeSerializer(old.toSnapshotEdge).toKeyValues.head.value).getOrElse(Array.empty[Byte])
      val after = buildPutAsync(edgeUpdate.newInvertedEdge.get.withNoPendingEdge()).head.asInstanceOf[PutRequest]

      def indexedEdgeMutationFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else writeAsyncSimple(label.hbaseZkAddr, indexedEdgeMutations(edgeUpdate), withWait = true)
      }
      def indexedEdgeIncrementFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else writeAsyncSimpleRetry(label.hbaseZkAddr, increments(edgeUpdate), withWait = true, MaxRetryNum).map { allSuccess =>
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
        locked <- client.compareAndSet(lock, before).toFuture
        indexEdgesUpdated <- indexedEdgeMutationFuture(locked)
        releaseLock <- if (indexEdgesUpdated) client.compareAndSet(after, lock.value()).toFuture else javaFallback
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
        val (_, edgeUpdate) = f(None, Seq(edge))
        val mutations = indexedEdgeMutations(edgeUpdate) ++ invertedEdgeMutations(edgeUpdate) ++ increments(edgeUpdate)
        writeAsyncSimple(zkQuorum, mutations, withWait)
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
                commitUpdate(newEdge)(snapshotEdgeOpt, edgeUpdate, tryNum).flatMap { case updateCommitted =>
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

  private def deleteVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = {
    writeAsync(vertex.hbaseZkAddr, Seq(vertex).map(buildDeleteAsync(_)), withWait).map(_.forall(identity))
  }

  private def deleteAllFetchedEdgesAsync(queryResult: QueryResult,
                                         requestTs: Long,
                                         retryNum: Int = 0): Future[Boolean] = {
    val queryParam = queryResult.queryParam
    val queryResultToDelete = queryResult.edgeWithScoreLs.filter { edgeWithScore =>
      (edgeWithScore.edge.ts < requestTs) && !edgeWithScore.edge.propsWithTs.containsKey(LabelMeta.degreeSeq)
    }

    if (queryResultToDelete.isEmpty) {
      Future.successful(true)
    } else {
      val edgesToDelete = queryResultToDelete.flatMap { edgeWithScore =>
        edgeWithScore.edge.copy(op = GraphUtil.operations("delete"), ts = requestTs, version = requestTs).relatedEdges
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
    Await.result(client.flush().toFuture, timeout)
  }

}
