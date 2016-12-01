/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.storage


import org.apache.s2graph.core.GraphExceptions.{NoStackException, FetchTimeoutException}
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.parsers.WhereParser
import org.apache.s2graph.core.storage.serde.indexedge.wide.{IndexEdgeDeserializable, IndexEdgeSerializable}
import org.apache.s2graph.core.storage.serde.snapshotedge.wide.SnapshotEdgeDeserializable
import org.apache.s2graph.core.storage.serde.vertex.{VertexDeserializable, VertexSerializable}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.{DeferCache, Extensions, logger}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Random, Try}
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hbase.util.Bytes


abstract class Storage[Q, R](val graph: S2Graph,
                          val config: Config)(implicit ec: ExecutionContext) {
  import HBaseType._
  import S2Graph._

  val BackoffTimeout = graph.BackoffTimeout
  val MaxRetryNum = graph.MaxRetryNum
  val MaxBackOff = graph.MaxBackOff
  val FailProb = graph.FailProb
  val LockExpireDuration =  graph.LockExpireDuration
  val MaxSize = graph.MaxSize
  val ExpireAfterWrite = graph.ExpireAfterWrite
  val ExpireAfterAccess = graph.ExpireAfterAccess

  /** retry scheduler */
  val scheduledThreadPool = Executors.newSingleThreadScheduledExecutor()


  /**
   * Compatibility table
   * | label schema version | snapshot edge | index edge | vertex | note |
   * | v1 | serde.snapshotedge.wide | serde.indexedge.wide | serde.vertex | do not use this. this exist only for backward compatibility issue |
   * | v2 | serde.snapshotedge.wide | serde.indexedge.wide | serde.vertex | do not use this. this exist only for backward compatibility issue |
   * | v3 | serde.snapshotedge.tall | serde.indexedge.wide | serde.vertex | recommended with HBase. current stable schema |
   * | v4 | serde.snapshotedge.tall | serde.indexedge.tall | serde.vertex | experimental schema. use scanner instead of get |
   *
   */

  /**
   * create serializer that knows how to convert given snapshotEdge into kvs: Seq[SKeyValue]
   * so we can store this kvs.
   * @param snapshotEdge: snapshotEdge to serialize
   * @return serializer implementation for StorageSerializable which has toKeyValues return Seq[SKeyValue]
   */
  def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): Serializable[SnapshotEdge] = {
    snapshotEdge.schemaVer match {
      case VERSION1 | VERSION2 => new serde.snapshotedge.wide.SnapshotEdgeSerializable(snapshotEdge)
      case VERSION3 | VERSION4 => new serde.snapshotedge.tall.SnapshotEdgeSerializable(snapshotEdge)
      case _ => throw new RuntimeException(s"not supported version: ${snapshotEdge.schemaVer}")
    }
  }

  /**
   * create serializer that knows how to convert given indexEdge into kvs: Seq[SKeyValue]
   * @param indexEdge: indexEdge to serialize
   * @return serializer implementation
   */
  def indexEdgeSerializer(indexEdge: IndexEdge): Serializable[IndexEdge] = {
    indexEdge.schemaVer match {
      case VERSION1 | VERSION2 | VERSION3 => new IndexEdgeSerializable(indexEdge)
      case VERSION4 => new serde.indexedge.tall.IndexEdgeSerializable(indexEdge)
      case _ => throw new RuntimeException(s"not supported version: ${indexEdge.schemaVer}")

    }
  }

  /**
   * create serializer that knows how to convert given vertex into kvs: Seq[SKeyValue]
   * @param vertex: vertex to serialize
   * @return serializer implementation
   */
  def vertexSerializer(vertex: S2Vertex): Serializable[S2Vertex] = new VertexSerializable(vertex)

  /**
   * create deserializer that can parse stored CanSKeyValue into snapshotEdge.
   * note that each storage implementation should implement implicit type class
   * to convert storage dependent dataType into common SKeyValue type by implementing CanSKeyValue
   *
   * ex) Asynchbase use it's KeyValue class and CanSKeyValue object has implicit type conversion method.
   * if any storaage use different class to represent stored byte array,
   * then that storage implementation is responsible to provide implicit type conversion method on CanSKeyValue.
   * */

  val snapshotEdgeDeserializers: Map[String, Deserializable[SnapshotEdge]] = Map(
    VERSION1 -> new SnapshotEdgeDeserializable(graph),
    VERSION2 -> new SnapshotEdgeDeserializable(graph),
    VERSION3 -> new serde.snapshotedge.tall.SnapshotEdgeDeserializable(graph),
    VERSION4 -> new serde.snapshotedge.tall.SnapshotEdgeDeserializable(graph)
  )
  def snapshotEdgeDeserializer(schemaVer: String) =
    snapshotEdgeDeserializers.get(schemaVer).getOrElse(throw new RuntimeException(s"not supported version: ${schemaVer}"))

  /** create deserializer that can parse stored CanSKeyValue into indexEdge. */
  val indexEdgeDeserializers: Map[String, Deserializable[S2Edge]] = Map(
    VERSION1 -> new IndexEdgeDeserializable(graph),
    VERSION2 -> new IndexEdgeDeserializable(graph),
    VERSION3 -> new IndexEdgeDeserializable(graph),
    VERSION4 -> new serde.indexedge.tall.IndexEdgeDeserializable(graph)
  )

  def indexEdgeDeserializer(schemaVer: String) =
    indexEdgeDeserializers.get(schemaVer).getOrElse(throw new RuntimeException(s"not supported version: ${schemaVer}"))

  /** create deserializer that can parser stored CanSKeyValue into vertex. */
  val vertexDeserializer: Deserializable[S2Vertex] = new VertexDeserializable(graph)


  /**
   * decide how to store given key values Seq[SKeyValue] into storage using storage's client.
   * note that this should be return true on all success.
   * we assumes that each storage implementation has client as member variable.
   *
   *
   * @param cluster: where this key values should be stored.
   * @param kvs: sequence of SKeyValue that need to be stored in storage.
   * @param withWait: flag to control wait ack from storage.
   *                  note that in AsynchbaseStorage(which support asynchronous operations), even with true,
   *                  it never block thread, but rather submit work and notified by event loop when storage send ack back.
   * @return ack message from storage.
   */
  def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean): Future[Boolean]

//  def writeToStorage(kv: SKeyValue, withWait: Boolean): Future[Boolean]

  /**
   * fetch SnapshotEdge for given request from storage.
   * also storage datatype should be converted into SKeyValue.
   * note that return type is Sequence rather than single SKeyValue for simplicity,
   * even though there is assertions sequence.length == 1.
   * @param request
   * @return
   */
  def fetchSnapshotEdgeKeyValues(request: QueryRequest): Future[Seq[SKeyValue]]

  /**
   * write requestKeyValue into storage if the current value in storage that is stored matches.
   * note that we only use SnapshotEdge as place for lock, so this method only change SnapshotEdge.
   *
   * Most important thing is this have to be 'atomic' operation.
   * When this operation is mutating requestKeyValue's snapshotEdge, then other thread need to be
   * either blocked or failed on write-write conflict case.
   *
   * Also while this method is still running, then fetchSnapshotEdgeKeyValues should be synchronized to
   * prevent wrong data for read.
   *
   * Best is use storage's concurrency control(either pessimistic or optimistic) such as transaction,
   * compareAndSet to synchronize.
   *
   * for example, AsynchbaseStorage use HBase's CheckAndSet atomic operation to guarantee 'atomicity'.
   * for storage that does not support concurrency control, then storage implementation
   * itself can maintain manual locks that synchronize read(fetchSnapshotEdgeKeyValues)
   * and write(writeLock).
   * @param requestKeyValue
   * @param expectedOpt
   * @return
   */
  def writeLock(requestKeyValue: SKeyValue, expectedOpt: Option[SKeyValue]): Future[Boolean]

  /**
   * build proper request which is specific into storage to call fetchIndexEdgeKeyValues or fetchSnapshotEdgeKeyValues.
   * for example, Asynchbase use GetRequest, Scanner so this method is responsible to build
   * client request(GetRequest, Scanner) based on user provided query.
    *
    * @param queryRequest
   * @return
    */
  protected def buildRequest(queryRequest: QueryRequest, edge: S2Edge): Q

  /**
   * fetch IndexEdges for given queryParam in queryRequest.
   * this expect previous step starting score to propagate score into next step.
   * also parentEdges is necessary to return full bfs tree when query require it.
   *
   * note that return type is general type.
   * for example, currently we wanted to use Asynchbase
   * so single I/O return type should be Deferred[T].
   *
   * if we use native hbase client, then this return type can be Future[T] or just T.
    *
    * @param queryRequest
   * @param isInnerCall
   * @param parentEdges
   * @return
   */
  def fetch(queryRequest: QueryRequest,
            isInnerCall: Boolean,
            parentEdges: Seq[EdgeWithScore]): R

  /**
   * responsible to fire parallel fetch call into storage and create future that will return merged result.
   *
   * @param queryRequests
   * @param prevStepEdges
   * @return
   */
  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[StepResult]]

  /**
   * fetch Vertex for given request from storage.
    *
    * @param request
   * @return
   */
  def fetchVertexKeyValues(request: QueryRequest): Future[Seq[SKeyValue]]

  /**
   * decide how to apply given edges(indexProps values + Map(_count -> countVal)) into storage.
    *
    * @param edges
   * @param withWait
   * @return
   */
  def incrementCounts(edges: Seq[S2Edge], withWait: Boolean): Future[Seq[(Boolean, Long, Long)]]

  /**
   * this method need to be called when client shutdown. this is responsible to cleanUp the resources
   * such as client into storage.
   */
  def flush(): Unit = {
  }

  /**
   * create table on storage.
   * if storage implementation does not support namespace or table, then there is nothing to be done
    *
    * @param zkAddr
   * @param tableName
   * @param cfs
   * @param regionMultiplier
   * @param ttl
   * @param compressionAlgorithm
   */
  def createTable(zkAddr: String,
                  tableName: String,
                  cfs: List[String],
                  regionMultiplier: Int,
                  ttl: Option[Int],
                  compressionAlgorithm: String,
                  replicationScopeOpt: Option[Int] = None,
                  totalRegionCount: Option[Int] = None): Unit





  /** Public Interface */
  def getVertices(vertices: Seq[S2Vertex]): Future[Seq[S2Vertex]] = {
    def fromResult(kvs: Seq[SKeyValue],
                   version: String): Option[S2Vertex] = {
      if (kvs.isEmpty) None
      else vertexDeserializer.fromKeyValues(None, kvs, version, None)
//        .map(S2Vertex(graph, _))
    }

    val futures = vertices.map { vertex =>
      val queryParam = QueryParam.Empty
      val q = Query.toQuery(Seq(vertex), Seq(queryParam))
      val queryRequest = QueryRequest(q, stepIdx = -1, vertex, queryParam)
      fetchVertexKeyValues(queryRequest).map { kvs =>
        fromResult(kvs, vertex.serviceColumn.schemaVersion)
      } recoverWith { case ex: Throwable =>
        Future.successful(None)
      }
    }

    Future.sequence(futures).map { result => result.toList.flatten }
  }
  def mutateStrongEdges(_edges: Seq[S2Edge], withWait: Boolean): Future[Seq[Boolean]] = {

    val edgeWithIdxs = _edges.zipWithIndex
    val grouped = edgeWithIdxs.groupBy { case (edge, idx) =>
      (edge.innerLabel, edge.srcVertex.innerId, edge.tgtVertex.innerId)
    } toSeq

    val mutateEdges = grouped.map { case ((_, _, _), edgeGroup) =>
      val edges = edgeGroup.map(_._1)
      val idxs = edgeGroup.map(_._2)
      // After deleteAll, process others
      val mutateEdgeFutures = edges.toList match {
        case head :: tail =>
          val edgeFuture = mutateEdgesInner(edges, checkConsistency = true , withWait)

          //TODO: decide what we will do on failure on vertex put
          val puts = buildVertexPutsAsync(head)
          val vertexFuture = writeToStorage(head.innerLabel.hbaseZkAddr, puts, withWait)
          Seq(edgeFuture, vertexFuture)
        case Nil => Nil
      }

      val composed = for {
//        deleteRet <- Future.sequence(deleteAllFutures)
        mutateRet <- Future.sequence(mutateEdgeFutures)
      } yield mutateRet

      composed.map(_.forall(identity)).map { ret => idxs.map(idx => idx -> ret) }
    }

    Future.sequence(mutateEdges).map { squashedRets =>
      squashedRets.flatten.sortBy { case (idx, ret) => idx }.map(_._2)
    }
  }

  def mutateVertex(vertex: S2Vertex, withWait: Boolean): Future[Boolean] = {
    if (vertex.op == GraphUtil.operations("delete")) {
      writeToStorage(vertex.hbaseZkAddr,
        vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Delete)), withWait)
    } else if (vertex.op == GraphUtil.operations("deleteAll")) {
      logger.info(s"deleteAll for vertex is truncated. $vertex")
      Future.successful(true) // Ignore withWait parameter, because deleteAll operation may takes long time
    } else {
      writeToStorage(vertex.hbaseZkAddr, buildPutsAll(vertex), withWait)
    }
  }

  def mutateVertices(vertices: Seq[S2Vertex],
                     withWait: Boolean = false): Future[Seq[Boolean]] = {
    val futures = vertices.map { vertex => mutateVertex(vertex, withWait) }
    Future.sequence(futures)
  }


  def mutateEdgesInner(edges: Seq[S2Edge],
                       checkConsistency: Boolean,
                       withWait: Boolean): Future[Boolean] = {
    assert(edges.nonEmpty)
    // TODO:: remove after code review: unreachable code
    if (!checkConsistency) {

      val zkQuorum = edges.head.innerLabel.hbaseZkAddr
      val futures = edges.map { edge =>
        val (_, edgeUpdate) = S2Edge.buildOperation(None, Seq(edge))

        val mutations =
          indexedEdgeMutations(edgeUpdate) ++ snapshotEdgeMutations(edgeUpdate) ++ increments(edgeUpdate)


        writeToStorage(zkQuorum, mutations, withWait)
      }
      Future.sequence(futures).map { rets => rets.forall(identity) }
    } else {
      fetchSnapshotEdgeInner(edges.head).flatMap { case (queryParam, snapshotEdgeOpt, kvOpt) =>
        retry(1)(edges, 0, snapshotEdgeOpt)
      }
    }
  }

  def exponentialBackOff(tryNum: Int) = {
    // time slot is divided by 10 ms
    val slot = 10
    Random.nextInt(Math.min(BackoffTimeout, slot * Math.pow(2, tryNum)).toInt)
  }

  def retry(tryNum: Int)(edges: Seq[S2Edge], statusCode: Byte, fetchedSnapshotEdgeOpt: Option[S2Edge]): Future[Boolean] = {
    if (tryNum >= MaxRetryNum) {
      edges.foreach { edge =>
        logger.error(s"commit failed after $MaxRetryNum\n${edge.toLogString}")
      }

      Future.successful(false)
    } else {
      val future = commitUpdate(edges, statusCode, fetchedSnapshotEdgeOpt)
      future.onSuccess {
        case success =>
          logger.debug(s"Finished. [$tryNum]\n${edges.head.toLogString}\n")
      }
      future recoverWith {
        case FetchTimeoutException(retryEdge) =>
          logger.info(s"[Try: $tryNum], Fetch fail.\n${retryEdge}")
          /** fetch failed. re-fetch should be done */
          fetchSnapshotEdgeInner(edges.head).flatMap { case (queryParam, snapshotEdgeOpt, kvOpt) =>
            retry(tryNum + 1)(edges, statusCode, snapshotEdgeOpt)
          }

        case PartialFailureException(retryEdge, failedStatusCode, faileReason) =>
          val status = failedStatusCode match {
            case 0 => "AcquireLock failed."
            case 1 => "Mutation failed."
            case 2 => "Increment failed."
            case 3 => "ReleaseLock failed."
            case 4 => "Unknown"
          }
          logger.info(s"[Try: $tryNum], [Status: $status] partial fail.\n${retryEdge.toLogString}\nFailReason: ${faileReason}")

          /** retry logic */
          val promise = Promise[Boolean]
          val backOff = exponentialBackOff(tryNum)
          scheduledThreadPool.schedule(new Runnable {
            override def run(): Unit = {
              val future = if (failedStatusCode == 0) {
                // acquire Lock failed. other is mutating so this thead need to re-fetch snapshotEdge.
                /** fetch failed. re-fetch should be done */
                fetchSnapshotEdgeInner(edges.head).flatMap { case (queryParam, snapshotEdgeOpt, kvOpt) =>
                  retry(tryNum + 1)(edges, statusCode, snapshotEdgeOpt)
                }
              } else {
                // partial failure occur while self locked and mutating.
                //            assert(fetchedSnapshotEdgeOpt.nonEmpty)
                retry(tryNum + 1)(edges, failedStatusCode, fetchedSnapshotEdgeOpt)
              }
              promise.completeWith(future)
            }

          }, backOff, TimeUnit.MILLISECONDS)
          promise.future

        case ex: Exception =>
          logger.error("Unknown exception", ex)
          Future.successful(false)
      }
    }
  }

  protected def commitUpdate(edges: Seq[S2Edge],
                             statusCode: Byte,
                             fetchedSnapshotEdgeOpt: Option[S2Edge]): Future[Boolean] = {
//    Future.failed(new PartialFailureException(edges.head, 0, "ahahah"))
    assert(edges.nonEmpty)
//    assert(statusCode == 0 || fetchedSnapshotEdgeOpt.isDefined)

    statusCode match {
      case 0 =>
        fetchedSnapshotEdgeOpt match {
          case None =>
          /**
           * no one has never mutated this SN.
           * (squashedEdge, edgeMutate) = Edge.buildOperation(None, edges)
           * pendingE = squashedEdge.copy(statusCode = 1, lockTs = now, version = squashedEdge.ts + 1)
           * lock = (squashedEdge, pendingE)
           * releaseLock = (edgeMutate.newSnapshotEdge, None)
           */
            val (squashedEdge, edgeMutate) = S2Edge.buildOperation(fetchedSnapshotEdgeOpt, edges)

            assert(edgeMutate.newSnapshotEdge.isDefined)

            val lockTs = Option(System.currentTimeMillis())
            val pendingEdge = squashedEdge.copy(statusCode = 1, lockTs = lockTs, version = squashedEdge.ts + 1)
            val lockSnapshotEdge = squashedEdge.toSnapshotEdge.copy(pendingEdgeOpt = Option(pendingEdge))
            val releaseLockSnapshotEdge = edgeMutate.newSnapshotEdge.get.copy(statusCode = 0,
              pendingEdgeOpt = None, version = lockSnapshotEdge.version + 1)

            commitProcess(statusCode, squashedEdge, fetchedSnapshotEdgeOpt, lockSnapshotEdge, releaseLockSnapshotEdge, edgeMutate)

          case Some(snapshotEdge) =>
            snapshotEdge.pendingEdgeOpt match {
              case None =>
                /**
                 * others finished commit on this SN. but there is no contention.
                 * (squashedEdge, edgeMutate) = Edge.buildOperation(snapshotEdgeOpt, edges)
                 * pendingE = squashedEdge.copy(statusCode = 1, lockTs = now, version = snapshotEdge.version + 1) ?
                 * lock = (snapshotEdge, pendingE)
                 * releaseLock = (edgeMutate.newSnapshotEdge, None)
                 */
                val (squashedEdge, edgeMutate) = S2Edge.buildOperation(fetchedSnapshotEdgeOpt, edges)
                if (edgeMutate.newSnapshotEdge.isEmpty) {
                  logger.debug(s"drop this requests: \n${edges.map(_.toLogString).mkString("\n")}")
                  Future.successful(true)
                } else {
                  val lockTs = Option(System.currentTimeMillis())
                  val pendingEdge = squashedEdge.copy(statusCode = 1, lockTs = lockTs, version = snapshotEdge.version + 1)
                  val lockSnapshotEdge = snapshotEdge.toSnapshotEdge.copy(pendingEdgeOpt = Option(pendingEdge))
                  val releaseLockSnapshotEdge = edgeMutate.newSnapshotEdge.get.copy(statusCode = 0,
                    pendingEdgeOpt = None, version = lockSnapshotEdge.version + 1)
                  commitProcess(statusCode, squashedEdge, fetchedSnapshotEdgeOpt, lockSnapshotEdge, releaseLockSnapshotEdge, edgeMutate)
                }
              case Some(pendingEdge) =>
                val isLockExpired = pendingEdge.lockTs.get + LockExpireDuration < System.currentTimeMillis()
                if (isLockExpired) {
                  /**
                   * if pendingEdge.ts == snapshotEdge.ts =>
                   *    (squashedEdge, edgeMutate) = Edge.buildOperation(None, Seq(pendingEdge))
                   * else =>
                   *    (squashedEdge, edgeMutate) = Edge.buildOperation(snapshotEdgeOpt, Seq(pendingEdge))
                   * pendingE = squashedEdge.copy(statusCode = 1, lockTs = now, version = snapshotEdge.version + 1)
                   * lock = (snapshotEdge, pendingE)
                   * releaseLock = (edgeMutate.newSnapshotEdge, None)
                   */
                  logger.debug(s"${pendingEdge.toLogString} has been expired.")
                  val (squashedEdge, edgeMutate) =
                    if (pendingEdge.ts == snapshotEdge.ts) S2Edge.buildOperation(None, pendingEdge +: edges)
                    else S2Edge.buildOperation(fetchedSnapshotEdgeOpt, pendingEdge +: edges)

                  val lockTs = Option(System.currentTimeMillis())
                  val newPendingEdge = squashedEdge.copy(statusCode = 1, lockTs = lockTs, version = snapshotEdge.version + 1)
                  val lockSnapshotEdge = snapshotEdge.toSnapshotEdge.copy(pendingEdgeOpt = Option(newPendingEdge))
                  val releaseLockSnapshotEdge = edgeMutate.newSnapshotEdge.get.copy(statusCode = 0,
                    pendingEdgeOpt = None, version = lockSnapshotEdge.version + 1)

                  commitProcess(statusCode, squashedEdge, fetchedSnapshotEdgeOpt, lockSnapshotEdge, releaseLockSnapshotEdge, edgeMutate)
                } else {
                  /**
                   * others finished commit on this SN and there is currently contention.
                   * this can't be proceed so retry from re-fetch.
                   * throw EX
                   */
                  val (squashedEdge, _) = S2Edge.buildOperation(fetchedSnapshotEdgeOpt, edges)
                  Future.failed(new PartialFailureException(squashedEdge, 0, s"others[${pendingEdge.ts}] is mutating. me[${squashedEdge.ts}]"))
                }
            }

        }
      case _ =>

        /**
         * statusCode > 0 which means self locked and there has been partial failure either on mutate, increment, releaseLock
         */

        /**
         * this succeed to lock this SN. keep doing on commit process.
         * if SN.isEmpty =>
         * no one never succed to commit on this SN.
         * this is first mutation try on this SN.
         * (squashedEdge, edgeMutate) = Edge.buildOperation(SN, edges)
         * else =>
         * assert(SN.pengingEdgeOpt.isEmpty) no-fetch after acquire lock when self retrying.
         * there has been success commit on this SN.
         * (squashedEdge, edgeMutate) = Edge.buildOperation(SN, edges)
         * releaseLock = (edgeMutate.newSnapshotEdge, None)
         */
        val _edges =
          if (fetchedSnapshotEdgeOpt.isDefined && fetchedSnapshotEdgeOpt.get.pendingEdgeOpt.isDefined) fetchedSnapshotEdgeOpt.get.pendingEdgeOpt.get +: edges
          else edges
        val (squashedEdge, edgeMutate) = S2Edge.buildOperation(fetchedSnapshotEdgeOpt, _edges)
        val newVersion = fetchedSnapshotEdgeOpt.map(_.version).getOrElse(squashedEdge.ts) + 2
        val releaseLockSnapshotEdge = edgeMutate.newSnapshotEdge match {
          case None => squashedEdge.toSnapshotEdge.copy(statusCode = 0, pendingEdgeOpt = None, version = newVersion)
          case Some(newSnapshotEdge) => newSnapshotEdge.copy(statusCode = 0, pendingEdgeOpt = None, version = newVersion)
        }
        // lockSnapshotEdge will be ignored.
        commitProcess(statusCode, squashedEdge, fetchedSnapshotEdgeOpt, releaseLockSnapshotEdge, releaseLockSnapshotEdge, edgeMutate)
    }
  }
  /**
   * orchestrate commit process.
   * we separate into 4 step to avoid duplicating each step over and over.
    *
    * @param statusCode: current statusCode of this thread to process edges.
   * @param squashedEdge: squashed(in memory) final edge from input edges on same snapshotEdge.
   * @param fetchedSnapshotEdgeOpt: fetched snapshotEdge from storage before commit process begin.
   * @param lockSnapshotEdge: lockEdge that hold necessary data to lock this snapshotEdge for this thread.
   * @param releaseLockSnapshotEdge: releaseLockEdge that will remove lock by storing new final merged states
   *                               all from current request edges and fetched snapshotEdge.
   * @param edgeMutate: mutations for indexEdge and snapshotEdge.
   * @return
   */
  protected def commitProcess(statusCode: Byte,
                              squashedEdge: S2Edge,
                              fetchedSnapshotEdgeOpt:Option[S2Edge],
                              lockSnapshotEdge: SnapshotEdge,
                              releaseLockSnapshotEdge: SnapshotEdge,
                              edgeMutate: EdgeMutate): Future[Boolean] = {
    for {
      locked <- acquireLock(statusCode, squashedEdge, fetchedSnapshotEdgeOpt, lockSnapshotEdge)
      mutated <- commitIndexEdgeMutations(locked, statusCode, squashedEdge, edgeMutate)
      incremented <- commitIndexEdgeDegreeMutations(mutated, statusCode, squashedEdge, edgeMutate)
      lockReleased <- releaseLock(incremented, statusCode, squashedEdge, releaseLockSnapshotEdge)
    } yield lockReleased
  }

  case class PartialFailureException(edge: S2Edge, statusCode: Byte, failReason: String) extends NoStackException(failReason)

  protected def debug(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge) = {
    val msg = Seq(s"[$ret] [$phase]", s"${snapshotEdge.toLogString()}").mkString("\n")
    logger.debug(msg)
  }

  protected def debug(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge, edgeMutate: EdgeMutate) = {
    val msg = Seq(s"[$ret] [$phase]", s"${snapshotEdge.toLogString()}",
      s"${edgeMutate.toLogString}").mkString("\n")
    logger.debug(msg)
  }

  /**
   * try to acquire lock on storage for this given snapshotEdge(lockEdge).
    *
    * @param statusCode: current statusCode of this thread to process edges.
   * @param squashedEdge: squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
   * @param fetchedSnapshotEdgeOpt: fetched snapshot edge from storage.
   * @param lockEdge: lockEdge to build RPC request(compareAndSet) into Storage.
   * @return
   */
  protected def acquireLock(statusCode: Byte,
                            squashedEdge: S2Edge,
                            fetchedSnapshotEdgeOpt: Option[S2Edge],
                            lockEdge: SnapshotEdge): Future[Boolean] = {
    if (statusCode >= 1) {
      logger.debug(s"skip acquireLock: [$statusCode]\n${squashedEdge.toLogString}")
      Future.successful(true)
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) {
        Future.failed(new PartialFailureException(squashedEdge, 0, s"$p"))
      } else {
        val lockEdgePut = snapshotEdgeSerializer(lockEdge).toKeyValues.head
        val oldPut = fetchedSnapshotEdgeOpt.map(e => snapshotEdgeSerializer(e.toSnapshotEdge).toKeyValues.head)
        writeLock(lockEdgePut, oldPut).recoverWith { case ex: Exception =>
          logger.error(s"AcquireLock RPC Failed.")
          throw new PartialFailureException(squashedEdge, 0, "AcquireLock RPC Failed")
        }.map { ret =>
          if (ret) {
            val log = Seq(
              "\n",
              "=" * 50,
              s"[Success]: acquireLock",
              s"[RequestEdge]: ${squashedEdge.toLogString}",
              s"[LockEdge]: ${lockEdge.toLogString()}",
              s"[PendingEdge]: ${lockEdge.pendingEdgeOpt.map(_.toLogString).getOrElse("")}",
              "=" * 50, "\n").mkString("\n")

            logger.debug(log)
            //            debug(ret, "acquireLock", edge.toSnapshotEdge)
          } else {
            throw new PartialFailureException(squashedEdge, 0, "hbase fail.")
          }
          true
        }
      }
    }
  }


  /**
   * change this snapshot's state on storage from locked into committed by
   * storing new merged states on storage. merge state come from releaseLockEdge.
   * note that releaseLock return Future.failed on predicate failure.
    *
    * @param predicate: indicate if this releaseLock phase should be proceed or not.
   * @param statusCode: releaseLock do not use statusCode, only for debug.
   * @param squashedEdge: squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
   * @param releaseLockEdge: final merged states if all process goes well.
   * @return
   */
  protected def releaseLock(predicate: Boolean,
                            statusCode: Byte,
                            squashedEdge: S2Edge,
                            releaseLockEdge: SnapshotEdge): Future[Boolean] = {
    if (!predicate) {
      Future.failed(new PartialFailureException(squashedEdge, 3, "predicate failed."))
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) Future.failed(new PartialFailureException(squashedEdge, 3, s"$p"))
      else {
        val releaseLockEdgePuts = snapshotEdgeSerializer(releaseLockEdge).toKeyValues
        writeToStorage(squashedEdge.innerLabel.hbaseZkAddr, releaseLockEdgePuts, withWait = true).recoverWith {
          case ex: Exception =>
            logger.error(s"ReleaseLock RPC Failed.")
            throw new PartialFailureException(squashedEdge, 3, "ReleaseLock RPC Failed")
        }.map { ret =>
          if (ret) {
            debug(ret, "releaseLock", squashedEdge.toSnapshotEdge)
          } else {
            val msg = Seq("\nFATAL ERROR\n",
              "=" * 50,
              squashedEdge.toLogString,
              releaseLockEdgePuts,
              "=" * 50,
              "\n"
            )
            logger.error(msg.mkString("\n"))
            //          error(ret, "releaseLock", edge.toSnapshotEdge)
            throw new PartialFailureException(squashedEdge, 3, "hbase fail.")
          }
          true
        }
      }
    }
  }

  /**
   *
   * @param predicate: indicate if this commitIndexEdgeMutations phase should be proceed or not.
   * @param statusCode: current statusCode of this thread to process edges.
   * @param squashedEdge: squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
   * @param edgeMutate: actual collection of mutations. note that edgeMutate contains snapshotEdge mutations,
   *                  but in here, we only use indexEdge's mutations.
   * @return
   */
  protected def commitIndexEdgeMutations(predicate: Boolean,
                       statusCode: Byte,
                       squashedEdge: S2Edge,
                       edgeMutate: EdgeMutate): Future[Boolean] = {
    if (!predicate) Future.failed(new PartialFailureException(squashedEdge, 1, "predicate failed."))
    else {
      if (statusCode >= 2) {
        logger.debug(s"skip mutate: [$statusCode]\n${squashedEdge.toLogString}")
        Future.successful(true)
      } else {
        val p = Random.nextDouble()
        if (p < FailProb) Future.failed(new PartialFailureException(squashedEdge, 1, s"$p"))
        else
          writeToStorage(squashedEdge.innerLabel.hbaseZkAddr, indexedEdgeMutations(edgeMutate), withWait = true).map { ret =>
            if (ret) {
              debug(ret, "mutate", squashedEdge.toSnapshotEdge, edgeMutate)
            } else {
              throw new PartialFailureException(squashedEdge, 1, "hbase fail.")
            }
            true
          }
      }
    }
  }

  /**
   *
   * @param predicate: indicate if this commitIndexEdgeMutations phase should be proceed or not.
   * @param statusCode: current statusCode of this thread to process edges.
   * @param squashedEdge: squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
   * @param edgeMutate: actual collection of mutations. note that edgeMutate contains snapshotEdge mutations,
   *                  but in here, we only use indexEdge's degree mutations.
   * @return
   */
  protected def commitIndexEdgeDegreeMutations(predicate: Boolean,
                          statusCode: Byte,
                          squashedEdge: S2Edge,
                          edgeMutate: EdgeMutate): Future[Boolean] = {

    def _write(kvs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
      writeToStorage(squashedEdge.innerLabel.hbaseZkAddr, kvs, withWait = withWait).map { ret =>
        if (ret) {
          debug(ret, "increment", squashedEdge.toSnapshotEdge, edgeMutate)
        } else {
          throw new PartialFailureException(squashedEdge, 2, "hbase fail.")
        }
        true
      }
    }

    if (!predicate) Future.failed(new PartialFailureException(squashedEdge, 2, "predicate failed."))
    if (statusCode >= 3) {
      logger.debug(s"skip increment: [$statusCode]\n${squashedEdge.toLogString}")
      Future.successful(true)
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) Future.failed(new PartialFailureException(squashedEdge, 2, s"$p"))
      else {
        val incrs = increments(edgeMutate)
        _write(incrs, true)
      }
    }
  }

  /** end of methods for consistency */

  def mutateLog(snapshotEdgeOpt: Option[S2Edge], edges: Seq[S2Edge],
                newEdge: S2Edge, edgeMutate: EdgeMutate) =
    Seq("----------------------------------------------",
      s"SnapshotEdge: ${snapshotEdgeOpt.map(_.toLogString)}",
      s"requestEdges: ${edges.map(_.toLogString).mkString("\n")}",
      s"newEdge: ${newEdge.toLogString}",
      s"mutation: \n${edgeMutate.toLogString}",
      "----------------------------------------------").mkString("\n")


  /** Delete All */
  def deleteAllFetchedEdgesAsyncOld(stepInnerResult: StepResult,
                                              requestTs: Long,
                                              retryNum: Int): Future[Boolean] = {
    if (stepInnerResult.isEmpty) Future.successful(true)
    else {
      val head = stepInnerResult.edgeWithScores.head
      val zkQuorum = head.edge.innerLabel.hbaseZkAddr
      val futures = for {
        edgeWithScore <- stepInnerResult.edgeWithScores
      } yield {
          val edge = edgeWithScore.edge
          val score = edgeWithScore.score

          val edgeSnapshot = edge.copyEdge(propsWithTs = S2Edge.propsToState(edge.updatePropsWithTs()))
          val reversedSnapshotEdgeMutations = snapshotEdgeSerializer(edgeSnapshot.toSnapshotEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put))

          val edgeForward = edge.copyEdge(propsWithTs = S2Edge.propsToState(edge.updatePropsWithTs()))
          val forwardIndexedEdgeMutations = edgeForward.edgesWithIndex.flatMap { indexEdge =>
            indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
              buildIncrementsAsync(indexEdge, -1L)
          }

          /** reverted direction */
          val edgeRevert = edge.copyEdge(propsWithTs = S2Edge.propsToState(edge.updatePropsWithTs()))
          val reversedIndexedEdgesMutations = edgeRevert.duplicateEdge.edgesWithIndex.flatMap { indexEdge =>
            indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
              buildIncrementsAsync(indexEdge, -1L)
          }

          val mutations = reversedIndexedEdgesMutations ++ reversedSnapshotEdgeMutations ++ forwardIndexedEdgeMutations

          writeToStorage(zkQuorum, mutations, withWait = true)
        }

      Future.sequence(futures).map { rets => rets.forall(identity) }
    }
  }

  /** End Of Delete All */




  /** Parsing Logic: parse from kv from Storage into Edge */
  def toEdge[K: CanSKeyValue](kv: K,
                              queryRequest: QueryRequest,
                              cacheElementOpt: Option[S2Edge],
                              parentEdges: Seq[EdgeWithScore]): Option[S2Edge] = {
    logger.debug(s"toEdge: $kv")

    try {
      val queryOption = queryRequest.query.queryOption
      val queryParam = queryRequest.queryParam
      val schemaVer = queryParam.label.schemaVersion
      val indexEdgeOpt = indexEdgeDeserializer(schemaVer).fromKeyValues(Option(queryParam.label), Seq(kv), queryParam.label.schemaVersion, cacheElementOpt)
      if (!queryOption.returnTree) indexEdgeOpt.map(indexEdge => indexEdge.copy(parentEdges = parentEdges))
      else indexEdgeOpt
    } catch {
      case ex: Exception =>
        logger.error(s"Fail on toEdge: ${kv.toString}, ${queryRequest}", ex)
        None
    }
  }

  def toSnapshotEdge[K: CanSKeyValue](kv: K,
                                      queryRequest: QueryRequest,
                                      cacheElementOpt: Option[SnapshotEdge] = None,
                                      isInnerCall: Boolean,
                                      parentEdges: Seq[EdgeWithScore]): Option[S2Edge] = {
//        logger.debug(s"SnapshottoEdge: $kv")
    val queryParam = queryRequest.queryParam
    val schemaVer = queryParam.label.schemaVersion
    val snapshotEdgeOpt = snapshotEdgeDeserializer(schemaVer).fromKeyValues(Option(queryParam.label), Seq(kv), queryParam.label.schemaVersion, cacheElementOpt)

    if (isInnerCall) {
      snapshotEdgeOpt.flatMap { snapshotEdge =>
        val edge = snapshotEdge.toEdge.copy(parentEdges = parentEdges)
        if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
        else None
      }
    } else {
      snapshotEdgeOpt.flatMap { snapshotEdge =>
        if (snapshotEdge.allPropsDeleted) None
        else {
          val edge = snapshotEdge.toEdge.copy(parentEdges = parentEdges)
          if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
          else None
        }
      }
    }
  }

  val dummyCursor: Array[Byte] = Array.empty

  def toEdges[K: CanSKeyValue](kvs: Seq[K],
                               queryRequest: QueryRequest,
                               prevScore: Double = 1.0,
                               isInnerCall: Boolean,
                               parentEdges: Seq[EdgeWithScore],
                               startOffset: Int = 0,
                               len: Int = Int.MaxValue): StepResult = {

    val toSKeyValue = implicitly[CanSKeyValue[K]].toSKeyValue _

    if (kvs.isEmpty) StepResult.Empty.copy(cursors = Seq(dummyCursor))
    else {
      val queryOption = queryRequest.query.queryOption
      val queryParam = queryRequest.queryParam
      val labelWeight = queryRequest.labelWeight
      val nextStepOpt = queryRequest.nextStepOpt
      val where = queryParam.where.get
      val label = queryParam.label
      val isDefaultTransformer = queryParam.edgeTransformer.isDefault
      val first = kvs.head
      val kv = first
      val schemaVer = queryParam.label.schemaVersion
      val cacheElementOpt =
        if (queryParam.isSnapshotEdge) None
        else indexEdgeDeserializer(schemaVer).fromKeyValues(Option(queryParam.label), Seq(kv), queryParam.label.schemaVersion, None)

      val (degreeEdges, keyValues) = cacheElementOpt match {
        case None => (Nil, kvs)
        case Some(cacheElement) =>
          val head = cacheElement
          if (!head.isDegree) (Nil, kvs)
          else (Seq(EdgeWithScore(head, 1.0, label)), kvs.tail)
      }

      val lastCursor: Seq[Array[Byte]] = Seq(if (keyValues.nonEmpty) toSKeyValue(keyValues(keyValues.length - 1)).row else dummyCursor)

      if (!queryOption.ignorePrevStepCache) {
        val edgeWithScores = for {
          (kv, idx) <- keyValues.zipWithIndex if idx >= startOffset && idx < startOffset + len
          edge <- (if (queryParam.isSnapshotEdge) toSnapshotEdge(kv, queryRequest, None, isInnerCall, parentEdges) else toEdge(kv, queryRequest, cacheElementOpt, parentEdges)).toSeq
          if where == WhereParser.success || where.filter(edge)
          convertedEdge <- if (isDefaultTransformer) Seq(edge) else convertEdges(queryParam, edge, nextStepOpt)
        } yield {
            val score = edge.rank(queryParam.rank)
            EdgeWithScore(convertedEdge, score, label)
          }
        StepResult(edgeWithScores = edgeWithScores, grouped = Nil, degreeEdges = degreeEdges, cursors = lastCursor)
      } else {
        val degreeScore = 0.0

        val edgeWithScores = for {
          (kv, idx) <- keyValues.zipWithIndex if idx >= startOffset && idx < startOffset + len
          edge <- (if (queryParam.isSnapshotEdge) toSnapshotEdge(kv, queryRequest, None, isInnerCall, parentEdges) else toEdge(kv, queryRequest, cacheElementOpt, parentEdges)).toSeq
          if where == WhereParser.success || where.filter(edge)
          convertedEdge <- if (isDefaultTransformer) Seq(edge) else convertEdges(queryParam, edge, nextStepOpt)
        } yield {
            val edgeScore = edge.rank(queryParam.rank)
            val score = queryParam.scorePropagateOp match {
              case "plus" => edgeScore + prevScore
              case "divide" =>
                if ((prevScore + queryParam.scorePropagateShrinkage) == 0) 0
                else edgeScore / (prevScore + queryParam.scorePropagateShrinkage)
              case _ => edgeScore * prevScore
            }
            val tsVal = processTimeDecay(queryParam, edge)
            val newScore = degreeScore + score
            EdgeWithScore(convertedEdge.copy(parentEdges = parentEdges), score = newScore * labelWeight * tsVal, label = label)
          }

        val sampled =
          if (queryRequest.queryParam.sample >= 0) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
          else edgeWithScores

        val normalized = if (queryParam.shouldNormalize) normalize(sampled) else sampled

        StepResult(edgeWithScores = normalized, grouped = Nil, degreeEdges = degreeEdges, cursors = lastCursor)
      }
    }
  }

  /** End Of Parse Logic */

  protected def toRequestEdge(queryRequest: QueryRequest, parentEdges: Seq[EdgeWithScore]): S2Edge = {
    val srcVertex = queryRequest.vertex
    val queryParam = queryRequest.queryParam
    val tgtVertexIdOpt = queryParam.tgtVertexInnerIdOpt
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val propsWithTs = label.EmptyPropsWithTs

    tgtVertexIdOpt match {
      case Some(tgtVertexId) => // _to is given.
        /** we use toSnapshotEdge so dont need to swap src, tgt */
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        val tgt = InnerVal.convertVersion(tgtVertexId, tgtColumn.columnType, label.schemaVersion)
        val (srcVId, tgtVId) = (SourceVertexId(srcColumn, src), TargetVertexId(tgtColumn, tgt))
        val (srcV, tgtV) = (graph.newVertex(srcVId), graph.newVertex(tgtVId))

        graph.newEdge(srcV, tgtV, label, labelWithDir.dir, propsWithTs = propsWithTs)
      case None =>
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        val srcVId = SourceVertexId(srcColumn, src)
        val srcV = graph.newVertex(srcVId)

        graph.newEdge(srcV, srcV, label, labelWithDir.dir, propsWithTs = propsWithTs, parentEdges = parentEdges)
    }
  }

  protected def fetchSnapshotEdgeInner(edge: S2Edge): Future[(QueryParam, Option[S2Edge], Option[SKeyValue])] = {
    /** TODO: Fix this. currently fetchSnapshotEdge should not use future cache
      * so use empty cacheKey.
      * */
    val queryParam = QueryParam(labelName = edge.innerLabel.label,
      direction = GraphUtil.fromDirection(edge.labelWithDir.dir),
      tgtVertexIdOpt = Option(edge.tgtVertex.innerIdVal),
      cacheTTLInMillis = -1)
    val q = Query.toQuery(Seq(edge.srcVertex), Seq(queryParam))
    val queryRequest = QueryRequest(q, 0, edge.srcVertex, queryParam)
    //    val q = Query.toQuery(Seq(edge.srcVertex), queryParam)


    fetchSnapshotEdgeKeyValues(queryRequest).map { kvs =>
      val (edgeOpt, kvOpt) =
        if (kvs.isEmpty) (None, None)
        else {
          val snapshotEdgeOpt = toSnapshotEdge(kvs.head, queryRequest, isInnerCall = true, parentEdges = Nil)
          val _kvOpt = kvs.headOption
          (snapshotEdgeOpt, _kvOpt)
        }
      (queryParam, edgeOpt, kvOpt)
    } recoverWith { case ex: Throwable =>
      logger.error(s"fetchQueryParam failed. fallback return.", ex)
      throw new FetchTimeoutException(s"${edge.toLogString}")
    }
  }


  /** end of query */

  /** Mutation Builder */


  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] = {
    // skip sampling for delete operation
    val deleteMutations = edgeMutate.edgesToDeleteWithIndexOpt.flatMap { indexEdge =>
      indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete, durability = indexEdge.label.durability))
    }

    val insertMutations = edgeMutate.edgesToInsertWithIndexOpt.flatMap { indexEdge =>
      indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put, durability = indexEdge.label.durability))
    }

    deleteMutations ++ insertMutations
  }

  def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] =
    edgeMutate.newSnapshotEdge.map(e => snapshotEdgeSerializer(e).toKeyValues.map(_.copy(durability = e.label.durability))).getOrElse(Nil)

  def increments(edgeMutate: EdgeMutate): Seq[SKeyValue] = {
    (edgeMutate.edgesToDeleteWithIndexOptForDegree.isEmpty, edgeMutate.edgesToInsertWithIndexOptForDegree.isEmpty) match {
      case (true, true) =>
        /** when there is no need to update. shouldUpdate == false */
        Nil

      case (true, false) =>
        /** no edges to delete but there is new edges to insert so increase degree by 1 */
        edgeMutate.edgesToInsertWithIndexOptForDegree.flatMap(buildIncrementsAsync(_))

      case (false, true) =>
        /** no edges to insert but there is old edges to delete so decrease degree by 1 */
        edgeMutate.edgesToDeleteWithIndexOptForDegree.flatMap(buildIncrementsAsync(_, -1))

      case (false, false) =>
        /** update on existing edges so no change on degree */
        Nil
    }
  }

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] = {
    val newProps = indexedEdge.updatePropsWithTs()
    newProps.put(LabelMeta.degree.name, new S2Property(indexedEdge.toEdge, LabelMeta.degree, LabelMeta.degree.name, amount, indexedEdge.ts))
    val _indexedEdge = indexedEdge.copy(propsWithTs = newProps)
    indexEdgeSerializer(_indexedEdge).toKeyValues.map(_.copy(operation = SKeyValue.Increment, durability = _indexedEdge.label.durability))
  }

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] = {
    val newProps = indexedEdge.updatePropsWithTs()
    newProps.put(LabelMeta.degree.name, new S2Property(indexedEdge.toEdge, LabelMeta.degree, LabelMeta.degree.name, amount, indexedEdge.ts))
    val _indexedEdge = indexedEdge.copy(propsWithTs = newProps)
    indexEdgeSerializer(_indexedEdge).toKeyValues.map(_.copy(operation = SKeyValue.Increment, durability = _indexedEdge.label.durability))
  }

  //TODO: ServiceColumn do not have durability property yet.
  def buildDeleteBelongsToId(vertex: S2Vertex): Seq[SKeyValue] = {
    val kvs = vertexSerializer(vertex).toKeyValues
    val kv = kvs.head
    vertex.belongLabelIds.map { id =>
      kv.copy(qualifier = Bytes.toBytes(S2Vertex.toPropKey(id)), operation = SKeyValue.Delete)
    }
  }

  def buildVertexPutsAsync(edge: S2Edge): Seq[SKeyValue] = {
    val storeVertex = edge.innerLabel.extraOptions.get("storeVertex").map(_.as[Boolean]).getOrElse(false)

    if (storeVertex) {
      if (edge.op == GraphUtil.operations("delete"))
        buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
      else
        vertexSerializer(edge.srcForVertex).toKeyValues ++ vertexSerializer(edge.tgtForVertex).toKeyValues
    } else {
      Seq.empty
    }
  }

  def buildDegreePuts(edge: S2Edge, degreeVal: Long): Seq[SKeyValue] = {
    edge.property(LabelMeta.degree.name, degreeVal, edge.ts)
    val kvs = edge.edgesWithIndexValid.flatMap { indexEdge =>
      indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put, durability = indexEdge.label.durability))
    }

    kvs
  }

  def buildPutsAll(vertex: S2Vertex): Seq[SKeyValue] = {
    vertex.op match {
      case d: Byte if d == GraphUtil.operations("delete") => vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Delete))
      case _ => vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Put))
    }
  }

  def info: Map[String, String] = Map("className" -> this.getClass.getSimpleName)
}
