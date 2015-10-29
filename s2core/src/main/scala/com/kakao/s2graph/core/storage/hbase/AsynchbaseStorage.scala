package com.kakao.s2graph.core.storage.hbase

import java.util
import java.util.ArrayList
import java.util.concurrent.ConcurrentHashMap

import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core.Graph._
import com.kakao.s2graph.core.mysqls.{LabelMeta, Label}
import com.kakao.s2graph.core.parsers.WhereParser
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.DeferOp._
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{GraphSerializable, GKeyValue, GStorable}
import com.kakao.s2graph.core.utils.logger
import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async._
import scala.collection.JavaConversions._
import scala.collection.{Iterable, Map, Seq}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, duration, Await, ExecutionContext}
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success, Try, Random}

class AsynchbaseStorage(config: Config)(implicit ex: ExecutionContext) extends GStorable[HBaseRpc, HBaseRpc, HBaseRpc] {
  override def put(kvs: Seq[GKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }


  override def increment(kvs: Seq[GKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value)) }


  override def delete(kvs: Seq[GKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv =>
      if (kv.qualifier == null) new DeleteRequest(kv.table, kv.row, kv.cf, kv.timestamp)
      else new DeleteRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.timestamp)
    }

  override def fetch(): Seq[GKeyValue] = Nil

  def loadAsyncConfig() = {
    for (entry <- config.entrySet() if entry.getKey.contains("hbase")) {
      this.asyncConfig.overrideConfig(entry.getKey, entry.getValue.unwrapped().toString)
    }
  }

  var emptyKVs = new ArrayList[KeyValue]()

  val asyncConfig: org.hbase.async.Config = new org.hbase.async.Config()
  loadAsyncConfig()

  val client = new HBaseClient(asyncConfig)
  asyncConfig.overrideConfig("hbase.rpcs.buffered_flush_interval", "0")
  val clientWithFlush = new HBaseClient(asyncConfig)


  val maxValidEdgeListSize = 10000
  val DefaultScore = 1.0

  val MaxBackOff = 10
  val clientFlushInterval = this.config.getInt("hbase.rpcs.buffered_flush_interval").toString().toShort
  val MaxRetryNum = this.config.getInt("max.retry.number")

  def snapshotEdgeSerializer(snapshotEdge: EdgeWithIndexInverted) =
    SnapshotEdgeHGStorageSerializable(snapshotEdge)

  def indexedEdgeSerializer(indexedEdge: EdgeWithIndex) =
    IndexedEdgeHGStorageSerializable(indexedEdge)

  def vertexSerializer(vertex: Vertex) =
    VertexHGStorageSerializable(vertex)

  val snapshotEdgeDeserializer =
    SnapshotEdgeHGStorageDeserializable

  val indexedEdgeDeserializer =
    IndexedEdgeHGStorageDeserializable

  val vertexDeserializer =
    VertexHGStorageDeserializable

  def toGKeyValue(kv: KeyValue) = HGKeyValue.apply(kv)

  def flush: Unit =
    Await.result(deferredToFutureWithoutFallback(client.flush), Duration((clientFlushInterval + 10) * 20, duration.MILLISECONDS))

  /** public methods */
  def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): Future[QueryResult] = {
    //TODO:
    val invertedEdge = Edge(srcVertex, tgtVertex, queryParam.labelWithDir).toInvertedEdgeHashLike
    val getRequest = queryParam.tgtVertexInnerIdOpt(Option(invertedEdge.tgtVertex.innerId))
      .buildGetRequest(invertedEdge.srcVertex)
    val q = Query.toQuery(Seq(srcVertex), queryParam)

    deferredToFuture(client.get(getRequest))(emptyKVs).map { kvs =>
      val edgeWithScoreLs = Edge.toEdges(kvs, queryParam, prevScore = 1.0, isInnerCall = isInnerCall, Nil)
      QueryResult(query = q, stepIdx = 0, queryParam = queryParam, edgeWithScoreLs = edgeWithScoreLs)
    }
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
    val futures = vertices.map { vertex =>
      val get = new GetRequest(vertex.hbaseTableName.getBytes, vertex.kvs.head.row, vertexCf)
      //      get.setTimeout(this.singleGetTimeout.toShort)
      get.setFailfast(true)
      get.maxVersions(1)

      val cacheKey = MurmurHash3.stringHash(get.toString)
      val cacheVal = vertexCache.getIfPresent(cacheKey)
      if (cacheVal == null)
        deferredToFuture(client.get(get))(emptyKVs).map { kvs =>
          Vertex(QueryParam.Empty, kvs, vertex.serviceColumn.schemaVersion)
        }
      else Future.successful(cacheVal)
    }
    Future.sequence(futures).map { result => result.toList.flatten }
  }

  def deleteAllAdjacentEdges(srcVertices: List[Vertex],
                             labels: Seq[Label],
                             dir: Int,
                             ts: Option[Long] = None,
                             walTopic: String): Future[Boolean] = {
    val requestTs = ts.getOrElse(System.currentTimeMillis())
    val queryParams = for {
      label <- labels
    } yield {
        val labelWithDir = LabelWithDirection(label.id.get, dir)
        QueryParam(labelWithDir).limit(0, maxValidEdgeListSize * 5).duplicatePolicy(Option(Query.DuplicatePolicy.Raw))
      }

    val step = Step(queryParams.toList)
    val q = Query(srcVertices, Vector(step), false)

    for {
      queryResultLs <- getEdges(q)
      ret <- deleteAllFetchedEdgesLs(queryResultLs, requestTs, 0, walTopic)
    } yield ret
  }

  def mutateElements(elements: Seq[GraphElement], withWait: Boolean = false): Future[Seq[Boolean]] = {
    val futures = elements.map { element =>
      element match {
        case edge: Edge => mutateEdge(edge, withWait)
        case vertex: Vertex => mutateVertex(vertex, withWait)
        case _ => throw new RuntimeException(s"$element is not edge/vertex")
      }
    }
    Future.sequence(futures)
  }

  def mutateEdge(edge: Edge, withWait: Boolean = false): Future[Boolean] = mutateEdgeWithOp(edge, withWait)


  def mutateEdges(edges: Seq[Edge], withWait: Boolean = false): Future[Seq[Boolean]] = {
    val futures = edges.map { edge => mutateEdge(edge, withWait) }

    Future.sequence(futures)
  }

  def mutateVertex(vertex: Vertex, withWait: Boolean = false): Future[Boolean] = {
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

  def mutateVertices(vertices: Seq[Vertex], withWait: Boolean = false): Future[Seq[Boolean]] = {
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

  private def buildRequest(queryRequest: QueryRequest): GetRequest = {
    val srcVertex = queryRequest.vertex
    val tgtVertexOpt = queryRequest.tgtVertexOpt
    val queryParam = queryRequest.queryParam
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val (srcInnerId, tgtInnerId) = tgtVertexOpt match {
      case Some(tgtVertex) => // _to is given.
        /** we use toInvertedEdgeHashLike so dont need to swap src, tgt */
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        val tgt = InnerVal.convertVersion(tgtVertex.innerId, tgtColumn.columnType, label.schemaVersion)
        (src, tgt)
      case None =>
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        (src, src)
    }

    val (srcVId, tgtVId) = (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val edge = Edge(srcV, tgtV, labelWithDir)

    val get = if (tgtVertexOpt.isDefined) {
      val snapshotEdge = edge.toInvertedEdgeHashLike
      val kv = snapshotEdge.kvs.head
      new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf, kv.qualifier)
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == queryParam.labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)
      val indexedEdge = indexedEdgeOpt.get
      val kv = indexedEdge.kvs.head
      val table = label.hbaseTableName.getBytes
      //kv.table //
      val rowKey = kv.row // indexedEdge.rowKey.bytes
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
        //        logger.debug(s"queryResultCachePut, $arg")
        cache.put(cacheKey, Seq(arg))
        arg
      }
    }
    if (queryParam.cacheTTLInMillis > 0) {
      val cacheTTL = queryParam.cacheTTLInMillis
      if (cache.asMap().containsKey(cacheKey)) {
        val cachedVal = cache.asMap().get(cacheKey)
        if (cachedVal != null && cachedVal.nonEmpty && queryParam.timestamp - cachedVal.head.timestamp < cacheTTL) {
          // val elapsedTime = queryParam.timestamp - cachedVal.timestamp
          //          logger.debug(s"cacheHitAndValid: $cacheKey, $cacheTTL, $elapsedTime")
          Deferred.fromResult(cachedVal.head)
        }
        else {
          // cache.asMap().remove(cacheKey)
          //          logger.debug(s"cacheHitInvalid(invalidated): $cacheKey, $cacheTTL")
          fetchQueryParam(queryRequest).addBoth(queryResultCallback(cacheKey))
        }
      } else {
        //        logger.debug(s"cacheMiss: $cacheKey")
        fetchQueryParam(queryRequest).addBoth(queryResultCallback(cacheKey))
      }
    } else {
      //      logger.debug(s"cacheMiss(no cacheTTL in QueryParam): $cacheKey")
      fetchQueryParam(queryRequest).addBoth(queryResultCallback(cacheKey))
    }
  }

  private def fetchQueryParam(queryRequest: QueryRequest): Deferred[QueryResult] = {
    val successCallback = (kvs: util.ArrayList[KeyValue]) => {
      val edgeWithScores = Edge.toEdges(kvs.toSeq, queryRequest.queryParam, queryRequest.prevStepScore, queryRequest.isInnerCall, queryRequest.parentEdges)
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

  /** end of public methods */


  /** private methods */


  def fetchStepWithFilter(queryResultsLs: Seq[QueryResult],
                                  q: Query,
                                  stepIdx: Int): Future[Seq[QueryResult]] = {

    val prevStepOpt = if (stepIdx > 0) Option(q.steps(stepIdx - 1)) else None
    val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.DefaultThreshold)
    val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
    val step = q.steps(stepIdx)
    val alreadyVisited =
      if (stepIdx == 0) Map.empty[(LabelWithDirection, Vertex), Boolean]
      else alreadyVisitedVertices(queryResultsLs)

    //TODO:
    val groupedBy = queryResultsLs.flatMap { queryResult =>
      queryResult.edgeWithScoreLs.map { case (edge, score) =>
        edge.tgtVertex ->(edge, score)
      }
    }.groupBy { case (vertex, (edge, score)) =>
      vertex
    }

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
    filterEdges(deferredToFuture(groupedDefer)(new ArrayList(fallback)), q, stepIdx, alreadyVisited)
  }

  /** edge Update **/
  def indexedEdgeMutations(edgeUpdate: EdgeUpdate): List[HBaseRpc] = {
    val deleteMutations = edgeUpdate.edgesToDelete.flatMap(edge => buildDeletesAsync(edge))
    val insertMutations = edgeUpdate.edgesToInsert.flatMap(edge => buildPutsAsync(edge))
    deleteMutations ++ insertMutations
  }

  def invertedEdgeMutations(edgeUpdate: EdgeUpdate): List[HBaseRpc] = {
    edgeUpdate.newInvertedEdge.map(e =>buildDeleteAsync(e)).getOrElse(Nil)
  }

  def increments(edgeUpdate: EdgeUpdate): List[HBaseRpc] = {
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
  def buildIncrementsAsync(indexedEdge: EdgeWithIndex, amount: Long = 1L): List[HBaseRpc] = {
    indexedEdge.kvs.headOption match {
      case None => Nil
      case Some(kv) =>
        val copiedKV = kv.copy(_qualifier = Array.empty[Byte], _value = Bytes.toBytes(amount))
        Graph.client.increment(Seq(copiedKV)).toList
    }
  }

  def buildIncrementsCountAsync(indexedEdge: EdgeWithIndex, amount: Long = 1L): List[HBaseRpc] = {
    indexedEdge.kvs.headOption match {
      case None => Nil
      case Some(kv) =>
        val copiedKV = kv.copy(_value = Bytes.toBytes(amount))
        Graph.client.increment(Seq(copiedKV)).toList
    }
  }

  def buildDeletesAsync(indexedEdge: EdgeWithIndex): List[HBaseRpc] = {
    delete(indexedEdge.kvs).toList
  }

  def buildPutsAsync(indexedEdge: EdgeWithIndex): List[HBaseRpc] = {
    put(indexedEdge.kvs).toList
  }

  /** EdgeWithIndexInverted  */
  def buildPutAsync(snapshotEdge: EdgeWithIndexInverted): List[HBaseRpc] = {
    put(snapshotEdge.kvs).toList
  }
  def buildDeleteAsync(snapshotEdge: EdgeWithIndexInverted): List[HBaseRpc] = {
    delete(snapshotEdge.kvs).toList
  }

  /** Vertex */
  def buildPutsAsync(vertex: Vertex): List[HBaseRpc] = put(vertex.kvs).toList

  def buildDeleteAsync(vertex: Vertex): List[HBaseRpc] = {
    val kv = vertex.kvs.head
    delete(Seq(kv.copy(_qualifier = null))).toList
  }

  def buildPutsAll(vertex: Vertex): List[HBaseRpc] = {
    vertex.op match {
      case d: Byte if d == GraphUtil.operations("delete") => buildDeleteAsync(vertex)
      case _ => buildPutsAsync(vertex)
    }
  }

  /** */
  def buildDeleteBelongsToId(vertex: Vertex): List[HBaseRpc] = {
    val kv = vertex.kvs.head
    import org.apache.hadoop.hbase.util.Bytes
    val newKVs = vertex.belongLabelIds.map { id => kv.copy(_qualifier = Bytes.toBytes(Vertex.toPropKey(id)) )}
    delete(newKVs).toList
  }

  def buildVertexPutsAsync(edge: Edge): List[HBaseRpc] =
    if (edge.op == GraphUtil.operations("delete"))
      buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
    else
      buildPutsAsync(edge.srcForVertex) ++ buildPutsAsync(edge.tgtForVertex)

  def insertBulkForLoaderAsync(edge: Edge, createRelEdges: Boolean = true) = {
    val relEdges = if (createRelEdges) edge.relatedEdges else List(edge)
    buildPutAsync(edge.toInvertedEdgeHashLike) ++ relEdges.flatMap { relEdge =>
      relEdge.edgesWithIndex.flatMap(e => buildPutsAsync(e))
    }
  }

  private def writeAsyncWithWaitRetry(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]], retryNum: Int): Future[Seq[Boolean]] = {
    if (retryNum > MaxRetryNum) {
      logger.error(s"writeAsyncWithWaitRetry failed: $elementRpcs")
      Future.successful(elementRpcs.map(_ => false))
    } else {
      writeAsyncWithWait(zkQuorum, elementRpcs).flatMap { rets =>
        val allSuccess = rets.forall(identity)
        if (allSuccess) Future.successful(elementRpcs.map(_ => true))
        else {
          Thread.sleep(Random.nextInt(MaxBackOff) + 1)
          writeAsyncWithWaitRetry(zkQuorum, elementRpcs, retryNum + 1)
        }
      }
    }
  }

  private def writeAsyncWithWaitRetrySimple(zkQuorum: String, elementRpcs: Seq[HBaseRpc], retryNum: Int): Future[Boolean] = {
    if (retryNum > MaxRetryNum) {
      logger.error(s"writeAsyncWithWaitRetry failed: $elementRpcs")
      Future.successful(false)
    } else {
      writeAsyncWithWaitSimple(zkQuorum, elementRpcs).flatMap { ret =>
        if (ret) Future.successful(ret)
        else {
          Thread.sleep(Random.nextInt(MaxBackOff) + 1)
          writeAsyncWithWaitRetrySimple(zkQuorum, elementRpcs, retryNum + 1)
        }
      }
    }
  }

  private def writeAsyncWithWait(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]]): Future[Seq[Boolean]] = {
    val client = clientWithFlush
    if (elementRpcs.isEmpty) {
      Future.successful(Seq.empty[Boolean])
    } else {
      val defers = elementRpcs.map { rpcs =>
        val defer = rpcs.map { rpc =>
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


  private def fetchInvertedAsync(edgeWriter: EdgeWriter): Future[(QueryParam, Option[Edge])] = {
    val edge = edgeWriter.edge
    val labelWithDir = edgeWriter.labelWithDir
    val queryParam = QueryParam(labelWithDir)

    getEdge(edge.srcVertex, edge.tgtVertex, queryParam, isInnerCall = true).map { queryResult =>
      (queryParam, queryResult.edgeWithScoreLs.headOption.map { case (e, _) => e })
    }
  }

  private def commitPending(edgeWriter: EdgeWriter)(snapshotEdgeOpt: Option[Edge]): Future[Boolean] = {
    val edge = edgeWriter.edge
    val label = edgeWriter.label
    val labelWithDir = edgeWriter.labelWithDir
    val pendingEdges =
      if (snapshotEdgeOpt.isEmpty || snapshotEdgeOpt.get.pendingEdgeOpt.isEmpty) Nil
      else Seq(snapshotEdgeOpt.get.pendingEdgeOpt.get)

    if (pendingEdges == Nil) Future.successful(true)
    else {
      val snapshotEdge = snapshotEdgeOpt.get
      // 1. commitPendingEdges
      // after: state without pending edges
      // before: state with pending edges

      val after = buildPutAsync(snapshotEdge.toInvertedEdgeHashLike.withNoPendingEdge()).head.asInstanceOf[PutRequest]
      val before = snapshotEdge.toInvertedEdgeHashLike.valueBytes
      for {
        pendingEdgesLock <- mutateEdges(pendingEdges, withWait = true)
        ret <- if (pendingEdgesLock.forall(identity)) deferredToFutureWithoutFallback(client.compareAndSet(after, before)).map(_.booleanValue())
        else Future.successful(false)
      } yield ret
    }
  }


  private def commitUpdate(edgeWriter: EdgeWriter)(snapshotEdgeOpt: Option[Edge], edgeUpdate: EdgeUpdate, retryNum: Int): Future[Boolean] = {
    val edge = edgeWriter.edge
    val label = edgeWriter.label

    if (edgeUpdate.newInvertedEdge.isEmpty) Future.successful(true)
    else {
      val lock = buildPutAsync(edgeUpdate.newInvertedEdge.get.withPendingEdge(Option(edge))).head.asInstanceOf[PutRequest]
      val before = snapshotEdgeOpt.map(old => old.toInvertedEdgeHashLike.valueBytes).getOrElse(Array.empty[Byte])
      val after = buildPutAsync(edgeUpdate.newInvertedEdge.get.withNoPendingEdge()).head.asInstanceOf[PutRequest]

      def indexedEdgeMutationFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else writeAsyncWithWait(label.hbaseZkAddr, Seq(indexedEdgeMutations(edgeUpdate))).map { indexedEdgesUpdated =>
          indexedEdgesUpdated.forall(identity)
        }
      }
      def indexedEdgeIncrementFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else writeAsyncWithWaitRetry(label.hbaseZkAddr, Seq(increments(edgeUpdate)), 0).map { rets =>
          val allSuccess = rets.forall(identity)
          if (!allSuccess) logger.error(s"indexedEdgeIncrement failed: $edgeUpdate")
          else logger.debug(s"indexedEdgeIncrement success: $edgeUpdate")
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

  private def mutateEdgeInner(edgeWriter: EdgeWriter,
                              checkConsistency: Boolean,
                              withWait: Boolean)(f: (Option[Edge], Edge) => EdgeUpdate, tryNum: Int = 0): Future[Boolean] = {
    val edge = edgeWriter.edge
    val zkQuorum = edge.label.hbaseZkAddr
    if (!checkConsistency) {
      val update = f(None, edge)
      val mutations = indexedEdgeMutations(update) ++ invertedEdgeMutations(update) ++ increments(update)
      if (withWait) writeAsyncWithWaitSimple(zkQuorum, mutations)
      else writeAsyncSimple(zkQuorum, mutations)
    } else {
      if (tryNum >= MaxRetryNum) {
        logger.error(s"mutate failed after $tryNum retry, $this")
        ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = edge))
        Future.successful(false)
      } else {
        val waitTime = Random.nextInt(Graph.MaxBackOff) + 1

        fetchInvertedAsync(edgeWriter).flatMap { case (queryParam, edges) =>
          val snapshotEdgeOpt = edges.headOption
          val edgeUpdate = f(snapshotEdgeOpt, edge)

          /** if there is no changes to be mutate, then just return true */
          if (edgeUpdate.newInvertedEdge.isEmpty) Future.successful(true)
          else
            commitPending(edgeWriter)(snapshotEdgeOpt).flatMap { case pendingAllCommitted =>
              if (pendingAllCommitted) {
                commitUpdate(edgeWriter)(snapshotEdgeOpt, edgeUpdate, tryNum).flatMap { case updateCommitted =>
                  if (!updateCommitted) {
                    Thread.sleep(waitTime)
                    logger.info(s"mutate failed. retry $edge")
                    mutateEdgeInner(edgeWriter, checkConsistency, withWait = true)(f, tryNum + 1)
                  } else {
                    logger.debug(s"mutate success: ${edgeUpdate.toLogString}\n$edge")
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

  private def mutateEdgeWithOp(edge: Edge, withWait: Boolean = false): Future[Boolean] = {
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


  /**
   *
   * @param vertex
   * @param withWait
   * @return
   */
  private def deleteVertex(vertex: Vertex, withWait: Boolean = false): Future[Boolean] = {
    if (withWait)
      writeAsyncWithWait(vertex.hbaseZkAddr, Seq(vertex).map(buildDeleteAsync(_))).map(_.forall(identity))
    else
      writeAsync(vertex.hbaseZkAddr, Seq(vertex).map(buildDeleteAsync(_))).map(_.forall(identity))
  }

  /**
   *
   * @param vertices
   * @return
   */
  private def deleteVertices(vertices: Seq[Vertex]): Future[Seq[Boolean]] = {
    val futures = vertices.map { vertex => deleteVertex(vertex) }
    Future.sequence(futures)
  }

  private def deleteAllFetchedEdgesAsync(queryResult: QueryResult,
                                         requestTs: Long,
                                         retryNum: Int = 0,
                                         walTopic: String): Future[Boolean] = {
    val queryParam = queryResult.queryParam
    //    val size = queryResult.edgeWithScoreLs.size
    val size = queryResult.sizeWithoutDegreeEdge()
    if (retryNum > MaxRetryNum) {
      queryResult.edgeWithScoreLs.foreach { case (edge, score) =>
        val copiedEdge = edge.copy(op = GraphUtil.operations("delete"), ts = requestTs, version = requestTs)
        logger.error(s"deleteAll failed: $copiedEdge")
        ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = copiedEdge))
      }
      Future.successful(false)
    } else {
      val futures: Seq[Future[Boolean]] =
        for {
          (edge, score) <- queryResult.edgeWithScoreLs
          duplicateEdge = edge.duplicateEdge.copy(op = GraphUtil.operations("delete"))
          //        version = edge.version + Edge.incrementVersion // this lead to forcing delete on fetched edges
          version = requestTs
          copiedEdge = edge.copy(op = GraphUtil.operations("delete"), ts = requestTs, version = version)
          hbaseZkAddr = queryResult.queryParam.label.hbaseZkAddr
        } yield {
          if (retryNum == 0)
            ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(topic = walTopic, element = copiedEdge))

          logger.debug(s"FetchedEdge: $edge")
          logger.debug(s"DeleteEdge: $duplicateEdge")

          val indexedEdgesDeletes = if (edge.ts < requestTs) duplicateEdge.edgesWithIndex.flatMap { indexedEdge =>
            val delete = buildDeletesAsync(indexedEdge)
            logger.debug(s"indexedEdgeDelete: $delete")
            delete
          } else Nil

          val snapshotEdgeDelete =
            if (edge.ts < requestTs) buildDeleteAsync(duplicateEdge.toInvertedEdgeHashLike)
            else Nil

          val copyEdgeIndexedEdgesDeletes =
            if (edge.ts < requestTs) copiedEdge.edgesWithIndex.flatMap { e => buildDeletesAsync(e) }
            else Nil

          val indexedEdgesIncrements =
            if (edge.ts < requestTs) duplicateEdge.edgesWithIndex.flatMap { indexedEdge =>
              val incr = buildIncrementsAsync(indexedEdge, -1L)
              logger.debug(s"indexedEdgeIncr: $incr")
              incr
            } else Nil

          val deletesForThisEdge = snapshotEdgeDelete ++ indexedEdgesDeletes ++ copyEdgeIndexedEdgesDeletes
          writeAsyncWithWait(queryParam.label.hbaseZkAddr, Seq(deletesForThisEdge)).flatMap { rets =>
            if (rets.forall(identity)) {
              writeAsyncWithWait(queryParam.label.hbaseZkAddr, Seq(indexedEdgesIncrements)).map { rets =>
                rets.forall(identity)
              }
            } else {
              Future.successful(false)
            }
          }
        }

      Future.sequence(futures).flatMap { duplicateEdgeDeletedLs =>
        val edgesToRetry = for {
          ((edge, score), duplicatedEdgeDeleted) <- queryResult.edgeWithScoreLs.zip(duplicateEdgeDeletedLs)
          if !duplicatedEdgeDeleted
        } yield (edge, score)
        val deletedEdgesNum = size - edgesToRetry.size
        val queryResultToRetry = queryResult.copy(edgeWithScoreLs = edgesToRetry)
        // not sure if increment rpc itset fail, then should we retry increment also?
        if (deletedEdgesNum > 0) {
          // decrement on current queryResult`s start vertex`s degree
          val incrs = queryResult.edgeWithScoreLs.headOption.map { case (edge, score) =>
            edge.edgesWithIndex.flatMap { indexedEdge => buildIncrementsAsync(indexedEdge, -1 * deletedEdgesNum) }
          }.getOrElse(Nil)
          writeAsyncWithWaitRetry(queryParam.label.hbaseZkAddr, Seq(incrs), 0).map { rets =>
            if (!rets.forall(identity)) logger.error(s"decrement for deleteAll failed. $incrs")
            else logger.debug(s"decrement for deleteAll successs. $incrs")
            rets
          }
        }
        if (edgesToRetry.isEmpty) {
          Future.successful(true)
        } else {
          deleteAllFetchedEdgesAsync(queryResultToRetry, requestTs, retryNum + 1, walTopic)
        }
      }
    }
  }

  private def deleteAllFetchedEdgesLs(queryResultLs: Seq[QueryResult], requestTs: Long,
                                      retryNum: Int = 0, walTopic: String): Future[Boolean] = {
    if (retryNum > MaxRetryNum) {
      logger.error(s"deleteDuplicateEdgesLs failed. ${queryResultLs}")
      Future.successful(false)
    } else {
      val futures = for {
        queryResult <- queryResultLs
      } yield {
          deleteAllFetchedEdgesAsync(queryResult, requestTs, 0, walTopic)
        }
      Future.sequence(futures).flatMap { rets =>
        val allSuccess = rets.forall(identity)
        if (!allSuccess) deleteAllFetchedEdgesLs(queryResultLs, requestTs, retryNum + 1, walTopic)
        else Future.successful(allSuccess)
      }
    }
  }


}
