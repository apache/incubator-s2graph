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
//  val prob = config.getDouble("hbase.fail.prob")
  val prob = 0.1

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
//    logger.debug(s"$rpc")
    val defer = rpc match {
      case d: DeleteRequest => _client.delete(d)
      case p: PutRequest => _client.put(p)
      case i: AtomicIncrementRequest =>
        logger.error(s"$rpc")
        _client.bufferAtomicIncrement(i)
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

  private def fetchInvertedAsync(edge: Edge): Future[(QueryParam, Option[Edge], Boolean)] = {
    val labelWithDir = edge.labelWithDir
    val queryParam = QueryParam(labelWithDir)

    queryBuilder.getEdge(edge.srcVertex, edge.tgtVertex, queryParam, isInnerCall = true).toFuture map { queryResult =>
      if (queryResult.isFailure) throw new FetchTimeoutException(s"$edge")
      (queryParam, queryResult.edgeWithScoreLs.headOption.map(_.edge), queryResult.isFailure)
    }
  }


  case class PartialFailureException(edge: Edge, statusCode: Byte, faileReason: String) extends Exception

  def debug(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge) = {
    val msg = Seq(s"[$ret] [$phase]", s"${snapshotEdge.toLogString()}").mkString("\n")
    logger.debug(msg)
  }
  def debug(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge, edgeMutate: EdgeMutate) = {
    val msg = Seq(s"[$ret] [$phase]", s"${snapshotEdge.toLogString()}",
     s"${edgeMutate.toLogString}").mkString("\n")
    logger.debug(msg)
  }
  def error(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge) = {
    val msg = Seq(s"[$ret] [$phase]", s"${snapshotEdge.toLogString()}").mkString("\n")
    logger.error(msg)
  }
  def error(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge, edgeMutate: EdgeMutate) = {
    val msg = Seq(s"[$ret] [$phase]", s"${snapshotEdge.toLogString()}",
       s"${edgeMutate.toLogString}").mkString("\n")
    logger.error(msg)
  }

  private def buildLockEdge(snapshotEdgeOpt: Option[Edge], edge: Edge) = {
    val newVersion = snapshotEdgeOpt.map(_.version).getOrElse(edge.ts) + 1
    val pendingEdge = edge.copy(version = newVersion, statusCode = 1)
    val base = snapshotEdgeOpt match {
      case None =>
        // no one ever mutated on this snapshotEdge.
        edge.toSnapshotEdge.copy(pendingEdgeOpt = Option(pendingEdge))
      case Some(snapshotEdge) =>
        // there is at least one mutation have been succeed.
        snapshotEdgeOpt.get.toSnapshotEdge.copy(pendingEdgeOpt = Option(pendingEdge))
    }
    val _lockEdge = base.copy(version = newVersion, statusCode = 1)

    logger.error(s"Lock Edge: ${_lockEdge.toLogString}")
    logger.error(s"Pending Edge: ${_lockEdge.pendingEdgeOpt.map(_.toLogString).getOrElse("")}")

    _lockEdge
  }
  private def toPutRequest(snapshotEdge: SnapshotEdge): PutRequest = {
    mutationBuilder.buildPutAsync(snapshotEdge).head.asInstanceOf[PutRequest]
  }

  private def commitUpdate(edge: Edge, statusCode: Byte)(snapshotEdgeOpt: Option[Edge], edgeUpdate: EdgeMutate): Future[Boolean] = {
    val label = edge.label

    def oldBytes = snapshotEdgeOpt.map { e =>
      snapshotEdgeSerializer(e.toSnapshotEdge).toKeyValues.head.value
    }.getOrElse(Array.empty)

    def releaseLockEdge(edgeMutate: EdgeMutate) = {
      val newVersion = buildLockEdge(snapshotEdgeOpt, edge).version + 1
      val base = edgeMutate.newInvertedEdge match {
        case None =>
          // shouldReplace false
          assert(snapshotEdgeOpt.isDefined)
          snapshotEdgeOpt.get.toSnapshotEdge
        case Some(newSnapshotEdge) => newSnapshotEdge
      }
      base.copy(version = newVersion, statusCode = 0, pendingEdgeOpt = None)
    }

    def lockEdgePut = toPutRequest(buildLockEdge(snapshotEdgeOpt, edge))
    def releaseLockEdgePut(edgeMutate: EdgeMutate) = 
      mutationBuilder.buildPutAsync(releaseLockEdge(edgeMutate)).head.asInstanceOf[PutRequest]

    //    assert(org.apache.hadoop.hbase.util.Bytes.compareTo(releaseLockEdgePut.value(), lockEdgePut.value()) != 0)
    //    assert(lockEdgePut.timestamp() < releaseLockEdgePut.timestamp())


    def acquireLock(statusCode: Byte): Future[Boolean] =
      if (statusCode >= 1) {
        logger.debug(s"skip acquireLock: [$statusCode]\n${edge.toLogString}")
        Future.successful(true)
      } else {
        val p = Random.nextDouble()
        if (p < prob) throw new PartialFailureException(edge, 0, s"$p")
        else
          client.compareAndSet(lockEdgePut, oldBytes).toFuture.recoverWith {
            case ex: Exception =>
              logger.error(s"AcquireLock RPC Failed.")
              throw new PartialFailureException(edge, 0, "AcquireLock RPC Failed")
          }.map { ret =>
            if (ret) {
              logger.error(s"locked: ${edge.toLogString} ${lockEdgePut.value.toList}")
              debug(ret, "acquireLock", edge.toSnapshotEdge)
            } else {
              throw new PartialFailureException(edge, 0, "hbase fail.")
            }
            true
          }
      }

    def releaseLock(predicate: Boolean, _edgeMutate: EdgeMutate): Future[Boolean] = {
      if (!predicate) {
        throw new PartialFailureException(edge, 3, "predicate failed.")
      }
      //      if (statusCode == 0) {
      //        logger.debug(s"skip releaseLock: [$statusCode] ${edge.toLogString}")
      //        Future.successful(true)
      //      } else {
      val p = Random.nextDouble()
//      cnt += 1
//      if (cnt == 2)  throw new PartialFailureException(edge, 3, s"$p")
      if (p < prob) throw new PartialFailureException(edge, 3, s"$p")
      else {
        client.compareAndSet(releaseLockEdgePut(_edgeMutate), lockEdgePut.value()).toFuture.recoverWith{
          case ex: Exception =>
            logger.error(s"ReleaseLock RPC Failed.")
            throw new PartialFailureException(edge, 3, "ReleaseLock RPC Failed")
        }.map { ret =>
          if (ret) {
            debug(ret, "releaseLock", edge.toSnapshotEdge)
          } else {
            val msg = Seq("\nLock\n",
              "FATAL ERROR: =====================================================",
              oldBytes.toList,
              lockEdgePut.value.toList,
              releaseLockEdgePut(_edgeMutate).value().toList,
              "==================================================================",
              "\n"
            )

            logger.error(msg.mkString("\n"))
            error(ret, "releaseLock", edge.toSnapshotEdge)
            throw new PartialFailureException(edge, 3, "hbase fail.")
          }
          true
        }
      }
      //      }
    }

    def mutate(predicate: Boolean, statusCode: Byte, _edgeMutate: EdgeMutate): Future[Boolean] = {
      if (!predicate) throw new PartialFailureException(edge, 1, "predicate failed.")

      if (statusCode >= 2) {
        logger.debug(s"skip mutate: [$statusCode]\n${edge.toLogString}")
        Future.successful(true)
      } else {
        val p = Random.nextDouble()
        if (p < prob) throw new PartialFailureException(edge, 1, s"$p")
        else
          writeAsyncSimple(label.hbaseZkAddr, mutationBuilder.indexedEdgeMutations(_edgeMutate), withWait = true).map { ret =>
            if (ret) {
              debug(ret, "mutate", edge.toSnapshotEdge, _edgeMutate)
            } else {
              throw new PartialFailureException(edge, 1, "hbase fail.")
            }
            true
          }
      }
    }

    def increment(predicate: Boolean, statusCode: Byte, _edgeMutate: EdgeMutate): Future[Boolean] = {
      if (!predicate) throw new PartialFailureException(edge, 2, "predicate failed.")
      if (statusCode >= 3) {
        logger.debug(s"skip increment: [$statusCode]\n${edge.toLogString}")
        Future.successful(true)
      } else {
        val p = Random.nextDouble()
        if (p < prob) throw new PartialFailureException(edge, 2, s"$p")
        else
          writeAsyncSimple(label.hbaseZkAddr, mutationBuilder.increments(_edgeMutate), withWait = true).map { ret =>
            if (ret) {
              debug(ret, "increment", edge.toSnapshotEdge, _edgeMutate)
            } else {
              throw new PartialFailureException(edge, 2, "hbase fail.")
            }
            true
          }
      }
    }

    def process(_edgeMutate: EdgeMutate, statusCode: Byte): Future[Boolean] =
      for {
        locked <- acquireLock(statusCode)
        mutated <- mutate(locked, statusCode, _edgeMutate)
        incremented <- increment(mutated, statusCode, _edgeMutate)
        released <- releaseLock(incremented, _edgeMutate)
      } yield {
        released
      }

    logger.debug(s"[StatusCode]: $statusCode\n[Snapshot]: ${snapshotEdgeOpt.map(_.toLogString).getOrElse("None")}\n[Request]: ${edge.toLogString}\n[Pending]: ${snapshotEdgeOpt.map(_.pendingEdgeOpt.map(_.toLogString))}\n")

    snapshotEdgeOpt match {
      case None =>
        // no one ever did success on acquire lock.
        process(edgeUpdate, statusCode)
      case Some(snapshotEdge) =>
        // someone did success on acquire lock at least one.
        snapshotEdge.pendingEdgeOpt match {
          case None =>
            // not locked
            process(edgeUpdate, statusCode)
          case Some(pendingEdge) =>
            // locked
            if (pendingEdge.ts == edge.ts) {
              // self locked
              val oldSnapshotEdge = if (snapshotEdge.ts == pendingEdge.ts) None else Option(snapshotEdge)
              val (_, newEdgeUpdate) = Edge.buildOperation(oldSnapshotEdge, Seq(edge))
              process(newEdgeUpdate, statusCode)
            } else {
              logger.error(s"[OTHER]: ${pendingEdge.ts} vs ${edge.ts}")
              throw new PartialFailureException(edge, statusCode, "others is mutating.")
            }
        }
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
      def commit(_edges: Seq[Edge], statusCode: Byte): Future[Boolean] = {

        fetchInvertedAsync(_edges.head) flatMap { case (queryParam, snapshotEdgeOpt, isFailure) =>
          if (isFailure) throw new FetchTimeoutException(s"${_edges.head}")

          val (newEdge, edgeUpdate) = f(snapshotEdgeOpt, _edges)
//          if (edgeUpdate.newInvertedEdge.isEmpty) {
//            logger.debug(s"${newEdge.toLogString} drop.")
//            Future.successful(true)
//          } else {
            commitUpdate(newEdge, statusCode)(snapshotEdgeOpt, edgeUpdate).map { ret =>
              if (ret) {
                logger.info(s"[Success] commit: \n${_edges.map(_.toLogString)}")
              } else {
                throw new PartialFailureException(newEdge, 3, "commit failed.")
              }
              true
            }
//          }
        }
      }
      def retry(tryNum: Int)(edges: Seq[Edge], statusCode: Byte)(fn: (Seq[Edge], Byte) => Future[Boolean]): Future[Boolean] = {
        if (tryNum >= MaxRetryNum) {
          logger.error(s"commit failed after $MaxRetryNum")
          edges.foreach { edge =>
            ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = edge))
          }
          Future.successful(false)
        } else {
          val future = fn(edges, statusCode)
          future.onSuccess {
            case success =>
              logger.debug(s"Finished. [$tryNum]\n${edges.head.toLogString}\n")
          }
          future recoverWith {
            case FetchTimeoutException(retryEdge) =>
              logger.error(s"[Try: $tryNum], Fetch fail.\n${retryEdge}")
              retry(tryNum + 1)(edges, statusCode)(fn)

            case PartialFailureException(retryEdge, failedStatusCode, faileReason) =>
              val status = failedStatusCode match {
                case 0 => "AcquireLock failed."
                case 1 => "Mutation failed."
                case 2 => "Increment failed."
                case 3 => "ReleaseLock failed."
                case 4 => "Unknown"
              }

              Thread.sleep(Random.nextInt(MaxBackOff))
              logger.error(s"[Try: $tryNum], [Status: $status] partial fail.\n${retryEdge.toLogString}\nFailReason: ${faileReason}")
              retry(tryNum + 1)(Seq(retryEdge), failedStatusCode)(fn)
          }
        }
      }
      retry(1)(edges, 0)(commit)
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
