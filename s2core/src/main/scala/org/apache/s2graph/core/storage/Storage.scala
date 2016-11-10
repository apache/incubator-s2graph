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

import java.util.concurrent.{TimeUnit, Executors}

import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.s2graph.core.ExceptionHandler.{KafkaMessage, Key, Val}
import org.apache.s2graph.core.GraphExceptions.FetchTimeoutException
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.storage.serde.indexedge.wide.{IndexEdgeDeserializable, IndexEdgeSerializable}
import org.apache.s2graph.core.storage.serde.snapshotedge.wide.SnapshotEdgeDeserializable
import org.apache.s2graph.core.storage.serde.vertex.{VertexDeserializable, VertexSerializable}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.{Extensions, logger}

import scala.annotation.tailrec
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.{Random, Try}

abstract class Storage[R](val config: Config)(implicit ec: ExecutionContext) {
  import HBaseType._

  /** storage dependent configurations */
  val MaxRetryNum = config.getInt("max.retry.number")
  val MaxBackOff = config.getInt("max.back.off")
  val BackoffTimeout = config.getInt("back.off.timeout")
  val DeleteAllFetchSize = config.getInt("delete.all.fetch.size")
  val FailProb = config.getDouble("hbase.fail.prob")
  val LockExpireDuration = config.getInt("lock.expire.time")
  val MaxSize = config.getInt("future.cache.max.size")
  val ExpireAfterWrite = config.getInt("future.cache.expire.after.write")
  val ExpireAfterAccess = config.getInt("future.cache.expire.after.access")

  /** retry scheduler */
  val scheduledThreadPool = Executors.newSingleThreadScheduledExecutor()

  /** handle mutate failed */
  val exceptionHandler = new ExceptionHandler(config)

  val failTopic = s"mutateFailed_${config.getString("phase")}"

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
  def vertexSerializer(vertex: Vertex) = new VertexSerializable(vertex)

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
    VERSION1 -> new SnapshotEdgeDeserializable,
    VERSION2 -> new SnapshotEdgeDeserializable,
    VERSION3 -> new serde.snapshotedge.tall.SnapshotEdgeDeserializable,
    VERSION4 -> new serde.snapshotedge.tall.SnapshotEdgeDeserializable
  )
  def snapshotEdgeDeserializer(schemaVer: String) =
    snapshotEdgeDeserializers.get(schemaVer).getOrElse(throw new RuntimeException(s"not supported version: ${schemaVer}"))

  /** create deserializer that can parse stored CanSKeyValue into indexEdge. */
  val indexEdgeDeserializers: Map[String, Deserializable[IndexEdge]] = Map(
    VERSION1 -> new IndexEdgeDeserializable,
    VERSION2 -> new IndexEdgeDeserializable,
    VERSION3 -> new IndexEdgeDeserializable,
    VERSION4 -> new serde.indexedge.tall.IndexEdgeDeserializable
  )

  def indexEdgeDeserializer(schemaVer: String) =
    indexEdgeDeserializers.get(schemaVer).getOrElse(throw new RuntimeException(s"not supported version: ${schemaVer}"))

  /** create deserializer that can parser stored CanSKeyValue into vertex. */
  val vertexDeserializer = new VertexDeserializable


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
  def fetchSnapshotEdgeKeyValues(request: AnyRef): Future[Seq[SKeyValue]]

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
   * @param queryRequest
   * @return
   */
  def buildRequest(queryRequest: QueryRequest): AnyRef

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
   * @param queryRequest
   * @param prevStepScore
   * @param isInnerCall
   * @param parentEdges
   * @return
   */
  def fetch(queryRequest: QueryRequest,
            prevStepScore: Double,
            isInnerCall: Boolean,
            parentEdges: Seq[EdgeWithScore]): R

  /**
   * responsible to fire parallel fetch call into storage and create future that will return merged result.
   * @param queryRequestWithScoreLs
   * @param prevStepEdges
   * @return
   */
  def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]]

  /**
   * fetch Vertex for given request from storage.
   * @param request
   * @return
   */
  def fetchVertexKeyValues(request: AnyRef): Future[Seq[SKeyValue]]

  /**
   * decide how to apply given edges(indexProps values + Map(_count -> countVal)) into storage.
   * @param edges
   * @param withWait
   * @return
   */
  def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]]

  /**
   * this method need to be called when client shutdown. this is responsible to cleanUp the resources
   * such as client into storage.
   */
  def flush(): Unit = {
    exceptionHandler.shutdown()
  }

  /**
   * create table on storage.
   * if storage implementation does not support namespace or table, then there is nothing to be done
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
                  compressionAlgorithm: String): Unit





  /** Public Interface */

  def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = {
    def fromResult(queryParam: QueryParam,
                   kvs: Seq[SKeyValue],
                   version: String): Option[Vertex] = {
      if (kvs.isEmpty) None
      else vertexDeserializer.fromKeyValues(queryParam, kvs, version, None)
    }

    val futures = vertices.map { vertex =>
      val queryParam = QueryParam.Empty
      val q = Query.toQuery(Seq(vertex), queryParam)
      val queryRequest = QueryRequest(q, stepIdx = -1, vertex, queryParam)
      fetchVertexKeyValues(buildRequest(queryRequest)).map { kvs =>
        fromResult(queryParam, kvs, vertex.serviceColumn.schemaVersion)
      } recoverWith { case ex: Throwable =>
        Future.successful(None)
      }
    }

    Future.sequence(futures).map { result => result.toList.flatten }
  }

  def mutateElements(elements: Seq[GraphElement],
                     withWait: Boolean = false): Future[Seq[Boolean]] = {

    val edgeBuffer = ArrayBuffer[Edge]()
    val vertexBuffer = ArrayBuffer[Vertex]()

    elements.foreach {
      case e: Edge => edgeBuffer += e
      case v: Vertex => vertexBuffer += v
      case any@_ => logger.error(s"Unknown type: ${any}")
    }

    val edgeFuture = mutateEdges(edgeBuffer, withWait)
    val vertexFuture = mutateVertices(vertexBuffer, withWait)

    val graphFuture = for {
      edgesMutated <- edgeFuture
      verticesMutated <- vertexFuture
    } yield edgesMutated ++ verticesMutated

    graphFuture
  }

  def mutateEdges(edges: Seq[Edge], withWait: Boolean): Future[Seq[Boolean]] = {
    val (strongEdges, weakEdges) =
      edges.partition(e => e.label.consistencyLevel == "strong" && e.op != GraphUtil.operations("insertBulk"))

    val weakEdgesFutures = weakEdges.groupBy { e => e.label.hbaseZkAddr }.map { case (zkQuorum, edges) =>
      val mutations = edges.flatMap { edge =>
        val (_, edgeUpdate) =
          if (edge.op == GraphUtil.operations("delete")) Edge.buildDeleteBulk(None, edge)
          else Edge.buildOperation(None, Seq(edge))
        buildVertexPutsAsync(edge) ++ indexedEdgeMutations(edgeUpdate) ++
          snapshotEdgeMutations(edgeUpdate) ++ increments(edgeUpdate)
      }
      writeToStorage(zkQuorum, mutations, withWait)
    }
    val strongEdgesFutures = mutateStrongEdges(strongEdges, withWait)
    for {
      weak <- Future.sequence(weakEdgesFutures)
      strong <- strongEdgesFutures
    } yield {
      strong ++ weak
    }
  }

  def mutateStrongEdges(_edges: Seq[Edge], withWait: Boolean): Future[Seq[Boolean]] = {

    val grouped = _edges.groupBy { edge => (edge.label, edge.srcVertex.innerId, edge.tgtVertex.innerId) } toSeq

    val mutateEdges = grouped.map { case ((_, _, _), edgeGroup) =>
      val (deleteAllEdges, edges) = edgeGroup.partition(_.op == GraphUtil.operations("deleteAll"))

      // DeleteAll first
      val deleteAllFutures = deleteAllEdges.map { edge =>
        deleteAllAdjacentEdges(Seq(edge.srcVertex), Seq(edge.label), edge.labelWithDir.dir, edge.ts)
      }

      // After deleteAll, process others
      lazy val mutateEdgeFutures = edges.toList match {
        case head :: tail =>
          //          val strongConsistency = edges.head.label.consistencyLevel == "strong"
          //          if (strongConsistency) {
          val edgeFuture = mutateEdgesInner(edges, checkConsistency = true , withWait)

          //TODO: decide what we will do on failure on vertex put
          val puts = buildVertexPutsAsync(head)
          val vertexFuture = writeToStorage(head.label.hbaseZkAddr, puts, withWait)
          Seq(edgeFuture, vertexFuture)
        //          } else {
        //            edges.map { edge => mutateEdge(edge, withWait = withWait) }
        //          }
        case Nil => Nil
      }

      val composed = for {
        deleteRet <- Future.sequence(deleteAllFutures)
        mutateRet <- Future.sequence(mutateEdgeFutures)
      } yield deleteRet ++ mutateRet

      composed.map(_.forall(identity))
    }

    Future.sequence(mutateEdges)
  }

  def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = {
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

  def mutateVertices(vertices: Seq[Vertex],
                     withWait: Boolean = false): Future[Seq[Boolean]] = {
    val futures = vertices.map { vertex => mutateVertex(vertex, withWait) }
    Future.sequence(futures)
  }


  def mutateEdgesInner(edges: Seq[Edge],
                       checkConsistency: Boolean,
                       withWait: Boolean): Future[Boolean] = {
    assert(edges.nonEmpty)
    if (!checkConsistency) {
      val zkQuorum = edges.head.label.hbaseZkAddr
      val futures = edges.map { edge =>
        val (_, edgeUpdate) = Edge.buildOperation(None, Seq(edge))
        val mutations = indexedEdgeMutations(edgeUpdate) ++ snapshotEdgeMutations(edgeUpdate) ++ increments(edgeUpdate)
        writeToStorage(zkQuorum, mutations, withWait)
      }
      Future.sequence(futures).map { rets => rets.forall(identity) }
    } else {
      fetchSnapshotEdge(edges.head).flatMap { case (queryParam, snapshotEdgeOpt, kvOpt) =>
        retry(1)(edges, 0, snapshotEdgeOpt)
      }
    }
  }

  def exponentialBackOff(tryNum: Int) = {
    // time slot is divided by 10 ms
    val slot = 10
    Random.nextInt(Math.min(BackoffTimeout, slot * Math.pow(2, tryNum)).toInt)
  }

  def retry(tryNum: Int)(edges: Seq[Edge], statusCode: Byte, fetchedSnapshotEdgeOpt: Option[Edge]): Future[Boolean] = {
    if (tryNum >= MaxRetryNum) {
      edges.foreach { edge =>
        logger.error(s"commit failed after $MaxRetryNum\n${edge.toLogString}")

        val kafkaMessage = ExceptionHandler.toKafkaMessage(failTopic, element = edge)
        exceptionHandler.enqueue(kafkaMessage)
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
          /* fetch failed. re-fetch should be done */
          fetchSnapshotEdge(edges.head).flatMap { case (queryParam, snapshotEdgeOpt, kvOpt) =>
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

          /* retry logic */
          val promise = Promise[Boolean]
          val backOff = exponentialBackOff(tryNum)
          scheduledThreadPool.schedule(new Runnable {
            override def run(): Unit = {
              val future = if (failedStatusCode == 0) {
                // acquire Lock failed. other is mutating so this thead need to re-fetch snapshotEdge.
                /* fetch failed. re-fetch should be done */
                fetchSnapshotEdge(edges.head).flatMap { case (queryParam, snapshotEdgeOpt, kvOpt) =>
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

  protected def commitUpdate(edges: Seq[Edge],
                             statusCode: Byte,
                             fetchedSnapshotEdgeOpt: Option[Edge]): Future[Boolean] = {
//    Future.failed(new PartialFailureException(edges.head, 0, "ahahah"))
    assert(edges.nonEmpty)
//    assert(statusCode == 0 || fetchedSnapshotEdgeOpt.isDefined)

    statusCode match {
      case 0 =>
        fetchedSnapshotEdgeOpt match {
          case None =>
          /*
           * no one has never mutated this SN.
           * (squashedEdge, edgeMutate) = Edge.buildOperation(None, edges)
           * pendingE = squashedEdge.copy(statusCode = 1, lockTs = now, version = squashedEdge.ts + 1)
           * lock = (squashedEdge, pendingE)
           * releaseLock = (edgeMutate.newSnapshotEdge, None)
           */
            val (squashedEdge, edgeMutate) = Edge.buildOperation(fetchedSnapshotEdgeOpt, edges)

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
                /*
                 * others finished commit on this SN. but there is no contention.
                 * (squashedEdge, edgeMutate) = Edge.buildOperation(snapshotEdgeOpt, edges)
                 * pendingE = squashedEdge.copy(statusCode = 1, lockTs = now, version = snapshotEdge.version + 1) ?
                 * lock = (snapshotEdge, pendingE)
                 * releaseLock = (edgeMutate.newSnapshotEdge, None)
                 */
                val (squashedEdge, edgeMutate) = Edge.buildOperation(fetchedSnapshotEdgeOpt, edges)
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
                  /*
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
                    if (pendingEdge.ts == snapshotEdge.ts) Edge.buildOperation(None, pendingEdge +: edges)
                    else Edge.buildOperation(fetchedSnapshotEdgeOpt, pendingEdge +: edges)

                  val lockTs = Option(System.currentTimeMillis())
                  val newPendingEdge = squashedEdge.copy(statusCode = 1, lockTs = lockTs, version = snapshotEdge.version + 1)
                  val lockSnapshotEdge = snapshotEdge.toSnapshotEdge.copy(pendingEdgeOpt = Option(newPendingEdge))
                  val releaseLockSnapshotEdge = edgeMutate.newSnapshotEdge.get.copy(statusCode = 0,
                    pendingEdgeOpt = None, version = lockSnapshotEdge.version + 1)

                  commitProcess(statusCode, squashedEdge, fetchedSnapshotEdgeOpt, lockSnapshotEdge, releaseLockSnapshotEdge, edgeMutate)
                } else {
                  /*
                   * others finished commit on this SN and there is currently contention.
                   * this can't be proceed so retry from re-fetch.
                   * throw EX
                   */
                  val (squashedEdge, _) = Edge.buildOperation(fetchedSnapshotEdgeOpt, edges)
                  Future.failed(new PartialFailureException(squashedEdge, 0, s"others[${pendingEdge.ts}] is mutating. me[${squashedEdge.ts}]"))
                }
            }

        }
      case _ =>

        /*
         * statusCode > 0 which means self locked and there has been partial failure either on mutate, increment, releaseLock
         */

        /*
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
        val (squashedEdge, edgeMutate) = Edge.buildOperation(fetchedSnapshotEdgeOpt, _edges)
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
                              squashedEdge: Edge,
                              fetchedSnapshotEdgeOpt:Option[Edge],
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

  case class PartialFailureException(edge: Edge, statusCode: Byte, failReason: String) extends Exception

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
   * @param statusCode: current statusCode of this thread to process edges.
   * @param squashedEdge: squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
   * @param fetchedSnapshotEdgeOpt: fetched snapshot edge from storage.
   * @param lockEdge: lockEdge to build RPC request(compareAndSet) into Storage.
   * @return
   */
  protected def acquireLock(statusCode: Byte,
                            squashedEdge: Edge,
                            fetchedSnapshotEdgeOpt: Option[Edge],
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
   * @param predicate: indicate if this releaseLock phase should be proceed or not.
   * @param statusCode: releaseLock do not use statusCode, only for debug.
   * @param squashedEdge: squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
   * @param releaseLockEdge: final merged states if all process goes well.
   * @return
   */
  protected def releaseLock(predicate: Boolean,
                            statusCode: Byte,
                            squashedEdge: Edge,
                            releaseLockEdge: SnapshotEdge): Future[Boolean] = {
    if (!predicate) {
      Future.failed(new PartialFailureException(squashedEdge, 3, "predicate failed."))
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) Future.failed(new PartialFailureException(squashedEdge, 3, s"$p"))
      else {
        val releaseLockEdgePuts = snapshotEdgeSerializer(releaseLockEdge).toKeyValues
        writeToStorage(squashedEdge.label.hbaseZkAddr, releaseLockEdgePuts, withWait = true).recoverWith {
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
                       squashedEdge: Edge,
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
          writeToStorage(squashedEdge.label.hbaseZkAddr, indexedEdgeMutations(edgeMutate), withWait = true).map { ret =>
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
                          squashedEdge: Edge,
                          edgeMutate: EdgeMutate): Future[Boolean] = {
    if (!predicate) Future.failed(new PartialFailureException(squashedEdge, 2, "predicate failed."))
    if (statusCode >= 3) {
      logger.debug(s"skip increment: [$statusCode]\n${squashedEdge.toLogString}")
      Future.successful(true)
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) Future.failed(new PartialFailureException(squashedEdge, 2, s"$p"))
      else
        writeToStorage(squashedEdge.label.hbaseZkAddr, increments(edgeMutate), withWait = true).map { ret =>
          if (ret) {
            debug(ret, "increment", squashedEdge.toSnapshotEdge, edgeMutate)
          } else {
            throw new PartialFailureException(squashedEdge, 2, "hbase fail.")
          }
          true
        }
    }
  }




  /** end of methods for consistency */

  def mutateLog(snapshotEdgeOpt: Option[Edge], edges: Seq[Edge],
                newEdge: Edge, edgeMutate: EdgeMutate) =
    Seq("----------------------------------------------",
      s"SnapshotEdge: ${snapshotEdgeOpt.map(_.toLogString)}",
      s"requestEdges: ${edges.map(_.toLogString).mkString("\n")}",
      s"newEdge: ${newEdge.toLogString}",
      s"mutation: \n${edgeMutate.toLogString}",
      "----------------------------------------------").mkString("\n")


  /** Delete All */
  protected def deleteAllFetchedEdgesAsyncOld(queryRequest: QueryRequest,
                                              queryResult: QueryResult,
                                              requestTs: Long,
                                              retryNum: Int): Future[Boolean] = {
    val queryParam = queryRequest.queryParam
    val zkQuorum = queryParam.label.hbaseZkAddr
    val futures = for {
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
    } yield {
        /* reverted direction */
        val reversedIndexedEdgesMutations = edge.duplicateEdge.edgesWithIndex.flatMap { indexEdge =>
          indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
            buildIncrementsAsync(indexEdge, -1L)
        }
        val reversedSnapshotEdgeMutations = snapshotEdgeSerializer(edge.toSnapshotEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put))
        val forwardIndexedEdgeMutations = edge.edgesWithIndex.flatMap { indexEdge =>
          indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
            buildIncrementsAsync(indexEdge, -1L)
        }
        val mutations = reversedIndexedEdgesMutations ++ reversedSnapshotEdgeMutations ++ forwardIndexedEdgeMutations
        writeToStorage(zkQuorum, mutations, withWait = true)
      }

    Future.sequence(futures).map { rets => rets.forall(identity) }
  }

  protected def buildEdgesToDelete(queryRequestWithResultLs: QueryRequestWithResult, requestTs: Long): QueryResult = {
    val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResultLs).get
    val edgeWithScoreLs = queryResult.edgeWithScoreLs.filter { edgeWithScore =>
      (edgeWithScore.edge.ts < requestTs) && !edgeWithScore.edge.isDegree
    }.map { edgeWithScore =>
      val label = queryRequest.queryParam.label
      val (newOp, newVersion, newPropsWithTs) = label.consistencyLevel match {
        case "strong" =>
          val _newPropsWithTs = edgeWithScore.edge.propsWithTs ++
            Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(requestTs, requestTs, label.schemaVersion))
          (GraphUtil.operations("delete"), requestTs, _newPropsWithTs)
        case _ =>
          val oldEdge = edgeWithScore.edge
          (oldEdge.op, oldEdge.version, oldEdge.propsWithTs)
      }

      val copiedEdge =
        edgeWithScore.edge.copy(op = newOp, version = newVersion, propsWithTs = newPropsWithTs)

      val edgeToDelete = edgeWithScore.copy(edge = copiedEdge)
//      logger.debug(s"delete edge from deleteAll: ${edgeToDelete.edge.toLogString}")
      edgeToDelete
    }

    queryResult.copy(edgeWithScoreLs = edgeWithScoreLs)
  }

  protected def deleteAllFetchedEdgesLs(queryRequestWithResultLs: Seq[QueryRequestWithResult], requestTs: Long): Future[(Boolean, Boolean)] = {
    val queryResultLs = queryRequestWithResultLs.map(_.queryResult)
    queryResultLs.foreach { queryResult =>
      if (queryResult.isFailure) throw new RuntimeException("fetched result is fallback.")
    }
    val futures = for {
      queryRequestWithResult <- queryRequestWithResultLs
      (queryRequest, _) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      deleteQueryResult = buildEdgesToDelete(queryRequestWithResult, requestTs)
      if deleteQueryResult.edgeWithScoreLs.nonEmpty
    } yield {
        val label = queryRequest.queryParam.label
        label.schemaVersion match {
          case HBaseType.VERSION3 | HBaseType.VERSION4 =>
            if (label.consistencyLevel == "strong") {
              /*
               * read: snapshotEdge on queryResult = O(N)
               * write: N x (relatedEdges x indices(indexedEdge) + 1(snapshotEdge))
               */
              mutateEdges(deleteQueryResult.edgeWithScoreLs.map(_.edge), withWait = true).map(_.forall(identity))
            } else {
              deleteAllFetchedEdgesAsyncOld(queryRequest, deleteQueryResult, requestTs, MaxRetryNum)
            }
          case _ =>

            /*
             * read: x
             * write: N x ((1(snapshotEdge) + 2(1 for incr, 1 for delete) x indices)
             */
            deleteAllFetchedEdgesAsyncOld(queryRequest, deleteQueryResult, requestTs, MaxRetryNum)
        }
      }

    if (futures.isEmpty) {
      // all deleted.
      Future.successful(true -> true)
    } else {
      Future.sequence(futures).map { rets => false -> rets.forall(identity) }
    }
  }

  protected def fetchAndDeleteAll(query: Query, requestTs: Long): Future[(Boolean, Boolean)] = {
    val future = for {
      queryRequestWithResultLs <- getEdges(query)
      (allDeleted, ret) <- deleteAllFetchedEdgesLs(queryRequestWithResultLs, requestTs)
    } yield {
//        logger.debug(s"fetchAndDeleteAll: ${allDeleted}, ${ret}")
        (allDeleted, ret)
      }

    Extensions.retryOnFailure(MaxRetryNum) {
      future
    } {
      logger.error(s"fetch and deleteAll failed.")
      (true, false)
    }

  }

  def deleteAllAdjacentEdges(srcVertices: Seq[Vertex],
                             labels: Seq[Label],
                             dir: Int,
                             ts: Long): Future[Boolean] = {

    def enqueueLogMessage() = {
      val kafkaMessages = for {
        vertice <- srcVertices
        id = vertice.innerId.toIdString()
        label <- labels
      } yield {
          val tsv = Seq(ts, "deleteAll", "e", id, id, label.label, "{}", GraphUtil.fromOp(dir.toByte)).mkString("\t")
          ExceptionHandler.toKafkaMessage(failTopic, tsv)
        }

      kafkaMessages.foreach(exceptionHandler.enqueue)
    }

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
    val retryFuture = Extensions.retryOnSuccess(MaxRetryNum) {
      fetchAndDeleteAll(q, requestTs)
    } { case (allDeleted, deleteSuccess) =>
      allDeleted && deleteSuccess
    }.map { case (allDeleted, deleteSuccess) => allDeleted && deleteSuccess }

    retryFuture onFailure {
      case ex =>
        logger.error(s"[Error]: deleteAllAdjacentEdges failed.")
        enqueueLogMessage()
    }

    retryFuture
  }

  /** End Of Delete All */




  /** Parsing Logic: parse from kv from Storage into Edge */
  def toEdge[K: CanSKeyValue](kv: K,
                              queryParam: QueryParam,
                              cacheElementOpt: Option[IndexEdge],
                              parentEdges: Seq[EdgeWithScore]): Option[Edge] = {
//        logger.debug(s"toEdge: $kv")
    try {
      val schemaVer = queryParam.label.schemaVersion
      val indexEdgeOpt = indexEdgeDeserializer(schemaVer).fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, cacheElementOpt)
      indexEdgeOpt.map(indexEdge => indexEdge.toEdge.copy(parentEdges = parentEdges))
    } catch {
      case ex: Exception =>
        logger.error(s"Fail on toEdge: ${kv.toString}, ${queryParam}", ex)
        None
    }
  }

  def toSnapshotEdge[K: CanSKeyValue](kv: K,
                                      queryParam: QueryParam,
                                      cacheElementOpt: Option[SnapshotEdge] = None,
                                      isInnerCall: Boolean,
                                      parentEdges: Seq[EdgeWithScore]): Option[Edge] = {
//        logger.debug(s"SnapshottoEdge: $kv")
    val schemaVer = queryParam.label.schemaVersion
    val snapshotEdgeOpt = snapshotEdgeDeserializer(schemaVer).fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, cacheElementOpt)

    if (isInnerCall) {
      snapshotEdgeOpt.flatMap { snapshotEdge =>
        val edge = snapshotEdge.toEdge.copy(parentEdges = parentEdges)
        if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
        else None
      }
    } else {
      snapshotEdgeOpt.flatMap { snapshotEdge =>
        if (Edge.allPropsDeleted(snapshotEdge.props)) None
        else {
          val edge = snapshotEdge.toEdge.copy(parentEdges = parentEdges)
          if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
          else None
        }
      }
    }
  }

  def toEdges[K: CanSKeyValue](kvs: Seq[K],
                               queryParam: QueryParam,
                               prevScore: Double = 1.0,
                               isInnerCall: Boolean,
                               parentEdges: Seq[EdgeWithScore]): Seq[EdgeWithScore] = {
    if (kvs.isEmpty) Seq.empty
    else {
      val first = kvs.head
      val kv = first
      val schemaVer = queryParam.label.schemaVersion
      val cacheElementOpt =
        if (queryParam.isSnapshotEdge) None
        else indexEdgeDeserializer(schemaVer).fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, None)

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
            case "divide" =>
              if ((prevScore + queryParam.scorePropagateShrinkage) == 0) 0
              else edge.rank(queryParam.rank) / (prevScore + queryParam.scorePropagateShrinkage)
            case _ => edge.rank(queryParam.rank) * prevScore
          }
        EdgeWithScore(edge, currentScore)
      }
    }
  }

  /** End Of Parse Logic */

//  /** methods for consistency */
//  protected def writeAsyncSimple(zkQuorum: String, elementRpcs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
//    if (elementRpcs.isEmpty) {
//      Future.successful(true)
//    } else {
//      val futures = elementRpcs.map { rpc => writeToStorage(rpc, withWait) }
//      Future.sequence(futures).map(_.forall(identity))
//    }
//  }


  //  def futureCache[T] = Cache[Long, (Long, T)]

  protected def toRequestEdge(queryRequest: QueryRequest): Edge = {
    val srcVertex = queryRequest.vertex
    //    val tgtVertexOpt = queryRequest.tgtVertexOpt
    val edgeCf = Serializable.edgeCf
    val queryParam = queryRequest.queryParam
    val tgtVertexIdOpt = queryParam.tgtVertexInnerIdOpt
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val (srcInnerId, tgtInnerId) = tgtVertexIdOpt match {
      case Some(tgtVertexId) => // _to is given.
        /* we use toSnapshotEdge so dont need to swap src, tgt */
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
    Edge(srcV, tgtV, labelWithDir, propsWithTs = propsWithTs)
  }



  protected def fetchSnapshotEdge(edge: Edge): Future[(QueryParam, Option[Edge], Option[SKeyValue])] = {
    val labelWithDir = edge.labelWithDir
    val queryParam = QueryParam(labelWithDir)
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(edge.tgtVertex.innerId))
    val q = Query.toQuery(Seq(edge.srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, edge.srcVertex, _queryParam)

    fetchSnapshotEdgeKeyValues(buildRequest(queryRequest)).map { kvs =>
      val (edgeOpt, kvOpt) =
        if (kvs.isEmpty) (None, None)
        else {
          val _edgeOpt = toEdges(kvs, queryParam, 1.0, isInnerCall = true, parentEdges = Nil).headOption.map(_.edge)
          val _kvOpt = kvs.headOption
          (_edgeOpt, _kvOpt)
        }
      (queryParam, edgeOpt, kvOpt)
    } recoverWith { case ex: Throwable =>
      logger.error(s"fetchQueryParam failed. fallback return.", ex)
      throw new FetchTimeoutException(s"${edge.toLogString}")
    }
  }

  protected def fetchStep(orgQuery: Query, queryRequestWithResultsLs: Seq[QueryRequestWithResult]): Future[Seq[QueryRequestWithResult]] = {
    if (queryRequestWithResultsLs.isEmpty) Future.successful(Nil)
    else {
      val queryRequest = queryRequestWithResultsLs.head.queryRequest
      val q = orgQuery
      val queryResultsLs = queryRequestWithResultsLs.map(_.queryResult)

      val stepIdx = queryRequest.stepIdx + 1

      val prevStepOpt = if (stepIdx > 0) Option(q.steps(stepIdx - 1)) else None
      val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.DefaultThreshold)
      val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
      val step = q.steps(stepIdx)
      val alreadyVisited =
        if (stepIdx == 0) Map.empty[(LabelWithDirection, Vertex), Boolean]
        else Graph.alreadyVisitedVertices(queryResultsLs)

      val groupedBy = queryResultsLs.flatMap { queryResult =>
        queryResult.edgeWithScoreLs.map { case edgeWithScore =>
          edgeWithScore.edge.tgtVertex -> edgeWithScore
        }
      }.groupBy { case (vertex, edgeWithScore) => vertex }

      val groupedByFiltered = for {
        (vertex, edgesWithScore) <- groupedBy
        aggregatedScore = edgesWithScore.map(_._2.score).sum if aggregatedScore >= prevStepThreshold
      } yield vertex -> aggregatedScore

      val prevStepTgtVertexIdEdges = for {
        (vertex, edgesWithScore) <- groupedBy
      } yield vertex.id -> edgesWithScore.map { case (vertex, edgeWithScore) => edgeWithScore }

      val nextStepSrcVertices = if (prevStepLimit >= 0) {
        groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
      } else {
        groupedByFiltered.toSeq
      }

      val queryRequests = for {
        (vertex, prevStepScore) <- nextStepSrcVertices
        queryParam <- step.queryParams
      } yield (QueryRequest(q, stepIdx, vertex, queryParam), prevStepScore)

      Graph.filterEdges(fetches(queryRequests, prevStepTgtVertexIdEdges), alreadyVisited)(ec)
    }
  }

  protected def fetchStepFuture(orgQuery: Query, queryRequestWithResultLsFuture: Future[Seq[QueryRequestWithResult]]): Future[Seq[QueryRequestWithResult]] = {
    for {
      queryRequestWithResultLs <- queryRequestWithResultLsFuture
      ret <- fetchStep(orgQuery, queryRequestWithResultLs)
    } yield ret
  }

  def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = {
    val fallback = {
      val queryRequest = QueryRequest(query = q, stepIdx = 0, q.vertices.head, queryParam = QueryParam.Empty)
      Future.successful(q.vertices.map(v => QueryRequestWithResult(queryRequest, QueryResult())))
    }
    Try {

      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        fallback
      } else {
        // current stepIdx = -1
        val startQueryResultLs = QueryResult.fromVertices(q)
        q.steps.foldLeft(Future.successful(startQueryResultLs)) { case (acc, step) =>
            fetchStepFuture(q, acc)
//          fetchStepFuture(q, acc).map { stepResults =>
//            step.queryParams.zip(stepResults).foreach { case (qParam, queryRequestWithResult)  =>
//              val cursor = Base64.getEncoder.encodeToString(queryRequestWithResult.queryResult.tailCursor)
//              qParam.cursorOpt = Option(cursor)
//            }
//            stepResults
//          }
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        fallback
    } get
  }

  def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = {
    val ts = System.currentTimeMillis()
    val futures = for {
      (srcVertex, tgtVertex, queryParam) <- params
      propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(ts, ts, queryParam.label.schemaVersion))
      edge = Edge(srcVertex, tgtVertex, queryParam.labelWithDir, propsWithTs = propsWithTs)
    } yield {
        fetchSnapshotEdge(edge).map { case (queryParam, edgeOpt, kvOpt) =>
          val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(edge.tgtVertex.innerId))
          val q = Query.toQuery(Seq(edge.srcVertex), _queryParam)
          val queryRequest = QueryRequest(q, 0, edge.srcVertex, _queryParam)
          val queryResult = QueryResult(edgeOpt.toSeq.map(e => EdgeWithScore(e, 1.0)))
          QueryRequestWithResult(queryRequest, queryResult)
        }
      }

    Future.sequence(futures)
  }



  @tailrec
  final def randomInt(sampleNumber: Int, range: Int, set: Set[Int] = Set.empty[Int]): Set[Int] = {
    if (range < sampleNumber || set.size == sampleNumber) set
    else randomInt(sampleNumber, range, set + Random.nextInt(range))
  }

  protected def sample(queryRequest: QueryRequest, edges: Seq[EdgeWithScore], n: Int): Seq[EdgeWithScore] = {
    if (edges.size <= n){
      edges
    }else{
      val plainEdges = if (queryRequest.queryParam.offset == 0) {
        edges.tail
      } else edges

      val randoms = randomInt(n, plainEdges.size)
      var samples = List.empty[EdgeWithScore]
      var idx = 0
      plainEdges.foreach { e =>
        if (randoms.contains(idx)) samples = e :: samples
        idx += 1
      }
      samples.toSeq
    }

  }
  /** end of query */

  /** Mutation Builder */


  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] = {
    val deleteMutations = edgeMutate.edgesToDelete.flatMap { indexEdge =>
      indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete))
    }
    val insertMutations = edgeMutate.edgesToInsert.flatMap { indexEdge =>
      indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put))
    }

    deleteMutations ++ insertMutations
  }

  def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] =
    edgeMutate.newSnapshotEdge.map(e => snapshotEdgeSerializer(e).toKeyValues).getOrElse(Nil)

  def increments(edgeMutate: EdgeMutate): Seq[SKeyValue] =
    (edgeMutate.edgesToDelete.isEmpty, edgeMutate.edgesToInsert.isEmpty) match {
      case (true, true) =>

        /* when there is no need to update. shouldUpdate == false */
        List.empty
      case (true, false) =>

        /* no edges to delete but there is new edges to insert so increase degree by 1 */
        edgeMutate.edgesToInsert.flatMap { e => buildIncrementsAsync(e) }
      case (false, true) =>

        /* no edges to insert but there is old edges to delete so decrease degree by 1 */
        edgeMutate.edgesToDelete.flatMap { e => buildIncrementsAsync(e, -1L) }
      case (false, false) =>

        /* update on existing edges so no change on degree */
        List.empty
    }

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] = {
    val newProps = indexedEdge.props ++ Map(LabelMeta.degreeSeq -> InnerValLikeWithTs.withLong(amount, indexedEdge.ts, indexedEdge.schemaVer))
    val _indexedEdge = indexedEdge.copy(props = newProps)
    indexEdgeSerializer(_indexedEdge).toKeyValues.map(_.copy(operation = SKeyValue.Increment))
  }

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] = {
    val newProps = indexedEdge.props ++ Map(LabelMeta.countSeq -> InnerValLikeWithTs.withLong(amount, indexedEdge.ts, indexedEdge.schemaVer))
    val _indexedEdge = indexedEdge.copy(props = newProps)
    indexEdgeSerializer(_indexedEdge).toKeyValues.map(_.copy(operation = SKeyValue.Increment))
  }
  def buildDeleteBelongsToId(vertex: Vertex): Seq[SKeyValue] = {
    val kvs = vertexSerializer(vertex).toKeyValues
    val kv = kvs.head
    vertex.belongLabelIds.map { id =>
      kv.copy(qualifier = Bytes.toBytes(Vertex.toPropKey(id)), operation = SKeyValue.Delete)
    }
  }

  def buildVertexPutsAsync(edge: Edge): Seq[SKeyValue] =
    if (edge.op == GraphUtil.operations("delete"))
      buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
    else
      vertexSerializer(edge.srcForVertex).toKeyValues ++ vertexSerializer(edge.tgtForVertex).toKeyValues

  def buildPutsAll(vertex: Vertex): Seq[SKeyValue] = {
    vertex.op match {
      case d: Byte if d == GraphUtil.operations("delete") => vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Delete))
      case _ => vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Put))
    }
  }
}
