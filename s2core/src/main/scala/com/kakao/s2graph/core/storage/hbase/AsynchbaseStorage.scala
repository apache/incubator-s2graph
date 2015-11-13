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
import scala.util.{Success, Random}


object AsynchbaseStorage {
  val vertexCf = HSerializable.vertexCf
  val edgeCf = HSerializable.edgeCf
  val emptyKVs = new util.ArrayList[KeyValue]()


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

  //  import Extensions.FutureOps

  import Extensions.DeferOps

  val client = AsynchbaseStorage.makeClient(config)
  val queryBuilder = new AsynchbaseQueryBuilder(this)(ec)
  val mutationBuilder = new AsynchbaseMutationBuilder(this)(ec)

  val cacheOpt = Option(cache)
  val vertexCacheOpt = Option(vertexCache)

  private val clientWithFlush = AsynchbaseStorage.makeClient(config, "hbase.rpcs.buffered_flush_interval" -> "0")
  private val clients = Seq(client, clientWithFlush)

  private val clientFlushInterval = config.getInt("hbase.rpcs.buffered_flush_interval").toString().toShort
  val MaxRetryNum = config.getInt("max.retry.number")
  val MaxBackOff = config.getInt("max.back.off")
  val DeleteAllFetchSize = config.getInt("delete.all.fetch.size")

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

  private def writeAsyncSimpleRetry(zkQuorum: String, elementRpcs: Seq[HBaseRpc], withWait: Boolean): Future[Boolean] = {
    def compute = writeAsyncSimple(zkQuorum, elementRpcs, withWait).flatMap { ret =>
      if (ret) Future.successful(ret)
      else throw FetchTimeoutException("writeAsyncWithWaitRetrySimple")
    }
    Extensions.retryOnFailure(MaxRetryNum) {
      compute
    } {
      logger.error(s"writeAsyncWithWaitRetrySimple: $elementRpcs")
      false
    }
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


  case class AcquireLockFailedException(msg: String) extends Exception(s"${System.currentTimeMillis()}: $msg")

  case class ReleaseLockFailedException(msg: String) extends Exception(s"${System.currentTimeMillis()}: $msg")

  case class IndexEdgeMutationFailedException(msg: String) extends Exception(s"${System.currentTimeMillis()}: $msg")

  case class IndexEdgeIncrementFailedException(msg: String) extends Exception(s"${System.currentTimeMillis()}: $msg")

  case class BuildOperationFailedException(msg: String) extends Exception(s"${System.currentTimeMillis()}: $msg")

  case class CommitFailedException(msg: String) extends Exception(s"${System.currentTimeMillis()}: $msg")

  case class LockedException(msg: String) extends Exception(s"${System.currentTimeMillis()}: $msg")

  case class PredicateFailedException(msg: String) extends Exception(s"${System.currentTimeMillis()}: $msg")

  case class UnReachableStateException(msg: String) extends Exception(s"${System.currentTimeMillis()}: $msg")


  def debug(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge) = {
    logger.debug(s"[${ret}] [${phase}] [TS: ${System.currentTimeMillis()}]: ${snapshotEdge.toLogString}")
  }

  def error(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge) = {
    logger.error(s"[${ret}] [${phase}] [TS: ${System.currentTimeMillis()}]: ${snapshotEdge.toLogString}")
  }

  private def commitUpdate(edge: Edge)(snapshotEdgeOpt: Option[Edge], edgeUpdate: EdgeMutate): Future[Boolean] = {
    val label = edge.label

    val lockEdge = if (snapshotEdgeOpt.isDefined) {
      // some mutation successed.
      snapshotEdgeOpt.get.toSnapshotEdge.copy(lockTsOpt = Option(edge.ts), version = snapshotEdgeOpt.get.version + 1)
    } else {
      edge.copy(propsWithTs = Map.empty).toSnapshotEdge.copy(lockTsOpt = Option(edge.ts))
    }

    val oldBytes = snapshotEdgeOpt.map { e =>
      snapshotEdgeSerializer(e.toSnapshotEdge).toKeyValues.head.value
    }.getOrElse(Array.empty)

    val releaseLockEdge = edgeUpdate.newInvertedEdge match {
      case None => // shouldReplace false
        assert(snapshotEdgeOpt.isDefined)
        snapshotEdgeOpt.get.toSnapshotEdge.copy(version = lockEdge.version + 1, lockTsOpt = None)
      case Some(newSnapshotEdge) => newSnapshotEdge.copy(version = lockEdge.version + 1, lockTsOpt = None)
    }
    val lockEdgePut = mutationBuilder.buildPutAsync(lockEdge).head.asInstanceOf[PutRequest]
    val releaseLockEdgePut = mutationBuilder.buildPutAsync(releaseLockEdge).head.asInstanceOf[PutRequest]

    assert(org.apache.hadoop.hbase.util.Bytes.compareTo(releaseLockEdgePut.value(), lockEdgePut.value()) != 0)
    assert(lockEdgePut.timestamp() < releaseLockEdgePut.timestamp())

    val prob = 0.1
    def acquireLock: Future[Boolean] =
      if (Random.nextDouble() < prob) throw new AcquireLockFailedException(s"${edge.toLogString}")
      else
        client.compareAndSet(lockEdgePut, oldBytes).toFuture.map { ret =>
          if (ret)
            debug(ret, "acquireLock", lockEdge)
          else
            throw new AcquireLockFailedException(s"${edge.toLogString}")
          ret.booleanValue()
        }

    def releaseLock(predicate: Boolean): Future[Boolean] = {
      if (!predicate) throw new PredicateFailedException(s"releaseLock. ${edge.toLogString}")

      if (Random.nextDouble() < prob) throw new ReleaseLockFailedException(s"${edge.toLogString}")
      else
        client.compareAndSet(releaseLockEdgePut, lockEdgePut.value()).toFuture.map { ret =>
          if (ret)
            debug(ret, "releaseLock", releaseLockEdge)
          else {
            error(ret, "releaseLock", snapshotEdgeOpt.get.toSnapshotEdge)
            logger.error(s"[${System.currentTimeMillis()}] ${lockEdgePut.value.toList} ${oldBytes.toList}")
            throw new ReleaseLockFailedException(s"${edge.toLogString}")
          }
          ret.booleanValue()
        }
    }

    def mutate(predicate: Boolean): Future[Boolean] = {
      if (!predicate) throw new PredicateFailedException(s"mutate. ${edge.toLogString}")

      if (Random.nextDouble() < prob) throw new IndexEdgeMutationFailedException(s"${edge.toLogString}")
      else
        writeAsyncSimple(label.hbaseZkAddr, mutationBuilder.indexedEdgeMutations(edgeUpdate), withWait = true).map { ret =>
          if (ret)
            debug(ret, "mutate", edge.toSnapshotEdge)
          else
            throw new IndexEdgeMutationFailedException(s"${edge.toLogString}")
          ret
        }
    }
    def increment(predicate: Boolean): Future[Boolean] = {
      if (!predicate) throw new PredicateFailedException(s"increment. ${edge.toLogString}")

      if (Random.nextDouble() < prob) throw new IndexEdgeIncrementFailedException(s"${edge.toLogString}")
      else
        writeAsyncSimple(label.hbaseZkAddr, mutationBuilder.increments(edgeUpdate), withWait = true).map { ret =>
          if (ret)
            debug(ret, "increment", edge.toSnapshotEdge)
          else
            throw new IndexEdgeIncrementFailedException(s"${edge.toLogString}")
          ret
        }
    }

    def process(withLock: Boolean): Future[Boolean] =
      for {
        locked <- if (withLock) acquireLock else Future.successful(true)
        mutationSuccess <- mutate(locked)
        lockReleased <- releaseLock(mutationSuccess)
        incrementSuccess <- increment(lockReleased)
      } yield {
        incrementSuccess
//        lockReleased
      }


    logger.info(s"CurrentLock: ${System.currentTimeMillis()}\n${snapshotEdgeOpt.map(_.lockTsOpt)}")

    snapshotEdgeOpt match {
      case None =>
        // no one ever did success on acquire lock.
        process(withLock = true)
      case Some(snapshotEdge) =>
        // someone did success on acquire lock at least one.
        snapshotEdge.lockTsOpt match {
          case None =>
            // not locked
            process(withLock = true)
          case Some(lockTs) =>
            // locked
            if (lockTs == edge.ts) {
              // self locked)
              logger.error("self retry!!!!!!!")
              process(withLock = false)
            } else {
              // other thread locked current snapshot edge
              throw new LockedException(s"[Current LockTs]: ${lockTs}, Me: ${edge.ts}")
            }
        }
      case _ => throw new UnReachableStateException(s"${edge.toLogString}")
    }
  }

  private def mutateEdgesInner(edges: Seq[Edge],
                               checkConsistency: Boolean,
                               withWait: Boolean)(f: (Option[Edge], Seq[Edge]) => (Edge, EdgeMutate)): Future[Boolean] = {

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
      def compute = {
        fetchInvertedAsync(edges.head) flatMap { case (queryParam, snapshotEdgeOpt) =>
          val (newEdge, edgeUpdate) = f(snapshotEdgeOpt, edges)
          if (snapshotEdgeOpt.isDefined) {
            if (edgeUpdate.newInvertedEdge.isDefined)
              assert(snapshotEdgeOpt.get.version < newEdge.version)
          }
          //        val maxVersion = edges.map(e => e.version).max
          //        if (newEdge.version <= maxVersion) {
          //          logger.error(s"${edges.map(_.toLogString)}")
          //          logger.error(s"${newEdge.toLogString}")
          //          logger.error(s"${snapshotEdgeOpt.map(_.toLogString)}")
          //          logger.error(s"${edgeUpdate.toLogString}")
          //          throw new BuildOperationFailedException(s"\n[NewEdge]: ${newEdge.version}\n[RequestEdge]: ${maxVersion}")
          //        }

          commitUpdate(newEdge)(snapshotEdgeOpt, edgeUpdate).map { ret =>
            if (ret)
              logger.info(s"[Success] commit: \n${edges.map(_.toLogString)}")
            else
              throw new CommitFailedException(s"${edges.map(_.toLogString)}")
            ret
          }
        }
      }

      Extensions.retryOnFailure(MaxRetryNum) {
        compute
      } {
        logger.error(s"mutate failed after $MaxRetryNum retry")
        edges.foreach { edge => ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = edge)) }
        false
      }
    }
  }


  def mutateLog(snapshotEdgeOpt: Option[Edge], edges: Seq[Edge],
                newEdge: Edge, edgeMutate: EdgeMutate) = {
    Seq("----------------------------------------------",
      s"SnapshotEdge: ${snapshotEdgeOpt.map(_.toLogString)}",
      s"requestEdges: ${edges.map(_.toLogString).mkString("\n")}",
      s"newEdge: ${newEdge.toLogString}",
      s"mutation: \n${edgeMutate.toLogString}",
      "----------------------------------------------").mkString("\n")
  }

  private def deleteAllFetchedEdgesAsyncOld(queryResult: QueryResult,
                                            requestTs: Long,
                                            retryNum: Int): Future[Boolean] = {
    val queryParam = queryResult.queryParam
    val zkQuorum = queryParam.label.hbaseZkAddr
    val futures = for {
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
    } yield {
        /** reverted direction */
        val reversedIndexedEdgesMutations = edge.duplicateEdge.edgesWithIndex.flatMap { indexedEdge =>
          mutationBuilder.buildDeletesAsync(indexedEdge) ++ mutationBuilder.buildIncrementsAsync(indexedEdge, -1L)
        }
        val reversedSnapshotEdgeMutations = mutationBuilder.buildDeleteAsync(edge.toSnapshotEdge)
        val forwardIndexedEdgeMutations = edge.edgesWithIndex.flatMap { indexedEdge =>
          mutationBuilder.buildDeletesAsync(indexedEdge) ++ mutationBuilder.buildIncrementsAsync(indexedEdge, -1L)
        }
        val mutations = reversedIndexedEdgesMutations ++ reversedSnapshotEdgeMutations ++ forwardIndexedEdgeMutations
        writeAsyncSimple(zkQuorum, mutations, withWait = true)
      }

    Future.sequence(futures).map { rets => rets.forall(identity) }
  }

  private def buildEdgesToDelete(queryResult: QueryResult, requestTs: Long): QueryResult = {
    val edgeWithScoreLs = queryResult.edgeWithScoreLs.filter { edgeWithScore =>
      (edgeWithScore.edge.ts < requestTs) && !edgeWithScore.edge.propsWithTs.containsKey(LabelMeta.degreeSeq)
    }.map { edgeWithScore =>
      val copiedEdge = edgeWithScore.edge.copy(op = GraphUtil.operations("delete"), ts = requestTs, version = requestTs)
      edgeWithScore.copy(edge = copiedEdge)
    }
    queryResult.copy(edgeWithScoreLs = edgeWithScoreLs)
  }

  private def deleteAllFetchedEdgesLs(queryResultLs: Seq[QueryResult], requestTs: Long): Future[(Boolean, Boolean)] = {
    queryResultLs.foreach { queryResult =>
      if (queryResult.isFailure) throw new RuntimeException("fetched result is fallback.")
    }

    val futures = for {
      queryResult <- queryResultLs
      deleteQueryResult = buildEdgesToDelete(queryResult, requestTs) if deleteQueryResult.edgeWithScoreLs.nonEmpty
    } yield {
        queryResult.queryParam.label.schemaVersion match {
          case HBaseType.VERSION3 =>

            /**
             * read: snapshotEdge on queryResult = O(N)
             * write: N x (relatedEdges x indices(indexedEdge) + 1(snapshotEdge))
             */
            mutateEdges(deleteQueryResult.edgeWithScoreLs.map(_.edge), withWait = true).map { rets => rets.forall(identity) }
          case _ =>

            /**
             * read: x
             * write: N x ((1(snapshotEdge) + 2(1 for incr, 1 for delete) x indices)
             */
            deleteAllFetchedEdgesAsyncOld(queryResult, requestTs, MaxRetryNum)
        }
      }
    if (futures.isEmpty) {
      // all deleted.
      Future.successful(true -> true)
    } else {
      Future.sequence(futures).map { rets => false -> rets.forall(identity) }
    }
  }

  def fetchAndDeleteAll(query: Query, requestTs: Long): Future[(Boolean, Boolean)] = {
    val future = for {
      queryResultLs <- getEdges(query)
      (allDeleted, ret) <- deleteAllFetchedEdgesLs(queryResultLs, requestTs)
    } yield {
        (allDeleted, ret)
      }
    Extensions.retryOnFailure(MaxRetryNum) {
      future
    } {
      logger.error(s"fetch and deleteAll failed.")
      (true, false)
    }

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
        QueryParam(labelWithDir).limit(0, DeleteAllFetchSize).duplicatePolicy(Option(Query.DuplicatePolicy.Raw))
      }

    val step = Step(queryParams.toList)
    val q = Query(srcVertices, Vector(step))

    //    Extensions.retryOnSuccessWithBackoff(MaxRetryNum, Random.nextInt(MaxBackOff) + 1) {
    Extensions.retryOnSuccess(MaxRetryNum) {
      fetchAndDeleteAll(q, requestTs)
    } { case (allDeleted, deleteSuccess) =>
      allDeleted && deleteSuccess
    }.map { case (allDeleted, deleteSuccess) => allDeleted && deleteSuccess }
  }

  def flush(): Unit = clients.foreach { client =>
    val timeout = Duration((clientFlushInterval + 10) * 20, duration.MILLISECONDS)
    Await.result(client.flush().toFuture, timeout)
  }

}
