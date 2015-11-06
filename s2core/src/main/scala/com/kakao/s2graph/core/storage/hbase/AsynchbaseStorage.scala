package com.kakao.s2graph.core.storage.hbase

import java.util

import com.google.common.cache.Cache
import com.kakao.s2graph.core.GraphExceptions.FetchTimeoutException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{Label, LabelMeta}
import com.kakao.s2graph.core.storage.{Storage}
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.{Extensions, logger}
import com.stumbleupon.async.{Deferred}
import com.typesafe.config.Config
import org.hbase.async._

import scala.collection.JavaConversions._
import scala.collection.{Seq}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, duration}
import scala.util.hashing.MurmurHash3
import scala.util.{Random}


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
  val queryBuilder = new AsynchbaseQueryBuilder(this)(ec)
  val mutationBuilder = new AsynchbaseMutationBuilder(this)(ec)

  val cacheOpt = Option(cache)
  val vertexCacheOpt = Option(vertexCache)

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

  def getEdges(q: Query): Future[Seq[QueryResult]] = queryBuilder.getEdges(q)

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

  def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = {
    //    mutateEdgeWithOp(edge, withWait)
    val strongConsistency = edge.label.consistencyLevel == "strong"
    val edgeFuture =
      if (edge.op == GraphUtil.operations("delete") && !strongConsistency) {
        val zkQuorum = edge.label.hbaseZkAddr
        val (_, edgeUpdate) = Edge.buildDeleteBulk(None, edge)
        val mutations =
          mutationBuilder.indexedEdgeMutations(edgeUpdate) ++
            mutationBuilder.invertedEdgeMutations(edgeUpdate) ++
            mutationBuilder.increments(edgeUpdate)
        writeAsyncSimple(zkQuorum, mutations, withWait)
      } else {
        mutateEdgesInner(Seq(edge), strongConsistency, withWait)(Edge.buildOperation)
      }
    val vertexFuture = writeAsyncSimple(edge.label.hbaseZkAddr,
      mutationBuilder.buildVertexPutsAsync(edge), withWait)
    Future.sequence(Seq(edgeFuture, vertexFuture)).map { rets => rets.forall(identity) }
  }

  override def mutateEdges(edges: Seq[Edge], withWait: Boolean): Future[Seq[Boolean]] = {
    val edgeGrouped = edges.groupBy { edge => (edge.label, edge.srcVertex.innerId, edge.tgtVertex.innerId) } toSeq

    val ret = edgeGrouped.map { case ((label, srcId, tgtId), edges) =>
      if (edges.isEmpty) Future.successful(true)
      else {
        val head = edges.head
        val strongConsistency = head.label.consistencyLevel == "strong"

        if (strongConsistency) {
          val edgeFuture = mutateEdgesInner(edges, strongConsistency, withWait)(Edge.buildOperation)
          //TODO: decide what we will do on failure on vertex put
          val vertexFuture = writeAsyncSimple(head.label.hbaseZkAddr,
            mutationBuilder.buildVertexPutsAsync(head), withWait)
          Future.sequence(Seq(edgeFuture, vertexFuture)).map { rets => rets.forall(identity) }
        } else {
          Future.sequence(edges.map { edge =>
            mutateEdge(edge, withWait = withWait)
          }).map { rets =>
            rets.forall(identity)
          }
        }
      }
    }
    Future.sequence(ret)
  }

  def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = {
    if (vertex.op == GraphUtil.operations("delete")) {
      writeAsyncSimple(vertex.hbaseZkAddr, mutationBuilder.buildDeleteAsync(vertex), withWait)
    } else if (vertex.op == GraphUtil.operations("deleteAll")) {
      logger.info(s"deleteAll for vertex is truncated. $vertex")
      Future.successful(true) // Ignore withWait parameter, because deleteAll operation may takes long time
    } else {
      writeAsyncSimple(vertex.hbaseZkAddr, mutationBuilder.buildPutsAll(vertex), withWait)
    }
  }

  def incrementCounts(edges: Seq[Edge]): Future[Seq[(Boolean, Long)]] = {
    val defers: Seq[Deferred[(Boolean, Long)]] = for {
      edge <- edges
    } yield {
        val edgeWithIndex = edge.edgesWithIndex.head
        val countWithTs = edge.propsWithTs(LabelMeta.countSeq)
        val countVal = countWithTs.innerVal.toString().toLong
        val incr = mutationBuilder.buildIncrementsCountAsync(edgeWithIndex, countVal).head
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

      val after = mutationBuilder.buildPutAsync(snapshotEdge.toSnapshotEdge.withNoPendingEdge()).head.asInstanceOf[PutRequest]
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
      val lock = mutationBuilder.buildPutAsync(edgeUpdate.newInvertedEdge.get.withPendingEdge(Option(edge))).head.asInstanceOf[PutRequest]
      val before = snapshotEdgeOpt.map(old => snapshotEdgeSerializer(old.toSnapshotEdge).toKeyValues.head.value).getOrElse(Array.empty[Byte])
      val after = mutationBuilder.buildPutAsync(edgeUpdate.newInvertedEdge.get.withNoPendingEdge()).head.asInstanceOf[PutRequest]

      def indexedEdgeMutationFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else writeAsyncSimple(label.hbaseZkAddr, mutationBuilder.indexedEdgeMutations(edgeUpdate), withWait = true)
      }
      def indexedEdgeIncrementFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else writeAsyncSimpleRetry(label.hbaseZkAddr, mutationBuilder.increments(edgeUpdate), withWait = true, MaxRetryNum).map { allSuccess =>
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
        val mutations =
          mutationBuilder.indexedEdgeMutations(edgeUpdate) ++
            mutationBuilder.invertedEdgeMutations(edgeUpdate) ++
            mutationBuilder.increments(edgeUpdate)
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

  private def deleteAllFetchedEdgesAsyncOld(queryResult: QueryResult,
                                            requestTs: Long,
                                            retryNum: Int = 0): Future[Boolean] = {
    val queryParam = queryResult.queryParam
    val queryResultToDelete = queryResult.edgeWithScoreLs.filter { edgeWithScore =>
      val (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
      (edge.ts < requestTs) && !edge.propsWithTs.containsKey(LabelMeta.degreeSeq)
    }
    if (queryResultToDelete.isEmpty) {
      Future.successful(true)
    } else {
      if (retryNum > MaxRetryNum) {
        queryResult.edgeWithScoreLs.foreach { edgeWithScore =>
          val (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
          val copiedEdge = edge.copy(op = GraphUtil.operations("delete"), ts = requestTs, version = requestTs)
          logger.error(s"deleteAll failed: $copiedEdge")
          ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = copiedEdge))
        }
        Future.successful(false)
      } else {
        val futures: Seq[Future[Boolean]] =
          for {
            edgeWithScore <- queryResultToDelete
            (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
            duplicateEdge = edge.duplicateEdge.copy(op = GraphUtil.operations("delete"))
            //        version = edge.version + Edge.incrementVersion // this lead to forcing delete on fetched edges
            version = requestTs
            copiedEdge = edge.copy(op = GraphUtil.operations("delete"), ts = requestTs, version = version)
            hbaseZkAddr = queryResult.queryParam.label.hbaseZkAddr
          } yield {
            /** reverted direction */
            val indexedEdgesDeletes = duplicateEdge.edgesWithIndex.flatMap { indexedEdge =>
              val delete = mutationBuilder.buildDeletesAsync(indexedEdge)
              logger.debug(s"indexedEdgeDelete: $delete")
              delete
            }

            val snapshotEdgeDelete = mutationBuilder.buildDeleteAsync(duplicateEdge.toSnapshotEdge)

            val indexedEdgesIncrements = duplicateEdge.edgesWithIndex.flatMap { indexedEdge =>
              val incr = mutationBuilder.buildIncrementsAsync(indexedEdge, -1L)
              logger.debug(s"indexedEdgeIncr: $incr")
              incr
            }

            /** forward direction */
            val copyEdgeIndexedEdgesDeletes = copiedEdge.edgesWithIndex.flatMap { e => mutationBuilder.buildDeletesAsync(e) }

            val deletesForThisEdge = snapshotEdgeDelete ++ indexedEdgesDeletes ++ copyEdgeIndexedEdgesDeletes

            writeAsyncSimple(queryParam.label.hbaseZkAddr, deletesForThisEdge, withWait = true).flatMap { rets =>
              if (!rets) Future.successful(false)
              else writeAsyncSimple(queryParam.label.hbaseZkAddr, indexedEdgesIncrements, withWait = true)
            }
          }

        Future.sequence(futures).flatMap { duplicateEdgeDeletedLs =>
          val edgesToRetry = for {
            (edgeWithScore, duplicatedEdgeDeleted) <- queryResultToDelete.zip(duplicateEdgeDeletedLs) if !duplicatedEdgeDeleted
          } yield edgeWithScore

          val deletedEdgesNum = queryResultToDelete.size - edgesToRetry.size
          //        val deletedEdgeNum = size - edgesToRetry.size
          val queryResultToRetry = queryResult.copy(edgeWithScoreLs = edgesToRetry)
          // not sure if increment rpc itset fail, then should we retry increment also?
          if (deletedEdgesNum > 0) {
            // decrement on current queryResult`s start vertex`s degree
            val (edge, score) = EdgeWithScore.unapply(queryResultToDelete.head).get
            val incrs = edge.edgesWithIndex.flatMap { indexedEdge => mutationBuilder.buildIncrementsAsync(indexedEdge, -1 * deletedEdgesNum) }

            writeAsyncSimpleRetry(queryParam.label.hbaseZkAddr, incrs, withWait = true, 0)
          }
          if (edgesToRetry.isEmpty) {
            Future.successful(true)
          } else {
            deleteAllFetchedEdgesAsync(queryResultToRetry, requestTs, retryNum + 1)
          }
        }
      }
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
      } yield {
          queryResult.queryParam.label.schemaVersion match {
            case HBaseType.VERSION3 => deleteAllFetchedEdgesAsync(queryResult, requestTs, 0)
            case _ => deleteAllFetchedEdgesAsyncOld(queryResult, requestTs, 0)
          }
        }

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
