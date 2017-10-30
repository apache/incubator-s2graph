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

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.s2graph.core.GraphExceptions.{FetchTimeoutException, NoStackException}
import org.apache.s2graph.core._
import org.apache.s2graph.core.utils.logger

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

class WriteWriteConflictResolver(graph: S2Graph,
                                 serDe: StorageSerDe,
                                 io: StorageIO,
                                 mutator: StorageWritable,
                                 fetcher: StorageReadable) {

  val BackoffTimeout = graph.BackoffTimeout
  val MaxRetryNum = graph.MaxRetryNum
  val MaxBackOff = graph.MaxBackOff
  val FailProb = graph.FailProb
  val LockExpireDuration = graph.LockExpireDuration
  val MaxSize = graph.MaxSize
  val ExpireAfterWrite = graph.ExpireAfterWrite
  val ExpireAfterAccess = graph.ExpireAfterAccess

  /** retry scheduler */
  val scheduledThreadPool = Executors.newSingleThreadScheduledExecutor()

  protected def exponentialBackOff(tryNum: Int) = {
    // time slot is divided by 10 ms
    val slot = 10
    Random.nextInt(Math.min(BackoffTimeout, slot * Math.pow(2, tryNum)).toInt)
  }

  def retry(tryNum: Int)(edges: Seq[S2Edge], statusCode: Byte, fetchedSnapshotEdgeOpt: Option[S2Edge])(implicit ec: ExecutionContext): Future[Boolean] = {
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
          /* fetch failed. re-fetch should be done */
          fetcher.fetchSnapshotEdgeInner(edges.head).flatMap { case (snapshotEdgeOpt, kvOpt) =>
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
                fetcher.fetchSnapshotEdgeInner(edges.head).flatMap { case (snapshotEdgeOpt, kvOpt) =>
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
                             fetchedSnapshotEdgeOpt: Option[S2Edge])(implicit ec: ExecutionContext): Future[Boolean] = {
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
                /*
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
                    if (pendingEdge.ts == snapshotEdge.ts) S2Edge.buildOperation(None, pendingEdge +: edges)
                    else S2Edge.buildOperation(fetchedSnapshotEdgeOpt, pendingEdge +: edges)

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
                  val (squashedEdge, _) = S2Edge.buildOperation(fetchedSnapshotEdgeOpt, edges)
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
    * @param statusCode              : current statusCode of this thread to process edges.
    * @param squashedEdge            : squashed(in memory) final edge from input edges on same snapshotEdge.
    * @param fetchedSnapshotEdgeOpt  : fetched snapshotEdge from storage before commit process begin.
    * @param lockSnapshotEdge        : lockEdge that hold necessary data to lock this snapshotEdge for this thread.
    * @param releaseLockSnapshotEdge : releaseLockEdge that will remove lock by storing new final merged states
    *                                all from current request edges and fetched snapshotEdge.
    * @param edgeMutate              : mutations for indexEdge and snapshotEdge.
    * @return
    */
  protected def commitProcess(statusCode: Byte,
                              squashedEdge: S2Edge,
                              fetchedSnapshotEdgeOpt: Option[S2Edge],
                              lockSnapshotEdge: SnapshotEdge,
                              releaseLockSnapshotEdge: SnapshotEdge,
                              edgeMutate: EdgeMutate)(implicit ec: ExecutionContext): Future[Boolean] = {
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
    * @param statusCode             : current statusCode of this thread to process edges.
    * @param squashedEdge           : squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
    * @param fetchedSnapshotEdgeOpt : fetched snapshot edge from storage.
    * @param lockEdge               : lockEdge to build RPC request(compareAndSet) into Storage.
    * @return
    */
  protected def acquireLock(statusCode: Byte,
                            squashedEdge: S2Edge,
                            fetchedSnapshotEdgeOpt: Option[S2Edge],
                            lockEdge: SnapshotEdge)(implicit ec: ExecutionContext): Future[Boolean] = {
    if (statusCode >= 1) {
      logger.debug(s"skip acquireLock: [$statusCode]\n${squashedEdge.toLogString}")
      Future.successful(true)
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) {
        Future.failed(new PartialFailureException(squashedEdge, 0, s"$p"))
      } else {
        val lockEdgePut = serDe.snapshotEdgeSerializer(lockEdge).toKeyValues.head
        val oldPut = fetchedSnapshotEdgeOpt.map(e => serDe.snapshotEdgeSerializer(e.toSnapshotEdge).toKeyValues.head)
        mutator.writeLock(lockEdgePut, oldPut).recoverWith { case ex: Exception =>
          logger.error(s"AcquireLock RPC Failed.")
          throw new PartialFailureException(squashedEdge, 0, "AcquireLock RPC Failed")
        }.map { ret =>
          if (ret.isSuccess) {
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
    * @param predicate       : indicate if this releaseLock phase should be proceed or not.
    * @param statusCode      : releaseLock do not use statusCode, only for debug.
    * @param squashedEdge    : squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
    * @param releaseLockEdge : final merged states if all process goes well.
    * @return
    */
  protected def releaseLock(predicate: Boolean,
                            statusCode: Byte,
                            squashedEdge: S2Edge,
                            releaseLockEdge: SnapshotEdge)(implicit ec: ExecutionContext): Future[Boolean] = {
    if (!predicate) {
      Future.failed(new PartialFailureException(squashedEdge, 3, "predicate failed."))
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) Future.failed(new PartialFailureException(squashedEdge, 3, s"$p"))
      else {
        val releaseLockEdgePuts = serDe.snapshotEdgeSerializer(releaseLockEdge).toKeyValues
        mutator.writeToStorage(squashedEdge.innerLabel.hbaseZkAddr, releaseLockEdgePuts, withWait = true).recoverWith {
          case ex: Exception =>
            logger.error(s"ReleaseLock RPC Failed.")
            throw new PartialFailureException(squashedEdge, 3, "ReleaseLock RPC Failed")
        }.map { ret =>
          if (ret.isSuccess) {
            debug(ret.isSuccess, "releaseLock", squashedEdge.toSnapshotEdge)
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
    * @param predicate    : indicate if this commitIndexEdgeMutations phase should be proceed or not.
    * @param statusCode   : current statusCode of this thread to process edges.
    * @param squashedEdge : squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
    * @param edgeMutate   : actual collection of mutations. note that edgeMutate contains snapshotEdge mutations,
    *                     but in here, we only use indexEdge's mutations.
    * @return
    */
  protected def commitIndexEdgeMutations(predicate: Boolean,
                                         statusCode: Byte,
                                         squashedEdge: S2Edge,
                                         edgeMutate: EdgeMutate)(implicit ec: ExecutionContext): Future[Boolean] = {
    if (!predicate) Future.failed(new PartialFailureException(squashedEdge, 1, "predicate failed."))
    else {
      if (statusCode >= 2) {
        logger.debug(s"skip mutate: [$statusCode]\n${squashedEdge.toLogString}")
        Future.successful(true)
      } else {
        val p = Random.nextDouble()
        if (p < FailProb) Future.failed(new PartialFailureException(squashedEdge, 1, s"$p"))
        else
          mutator.writeToStorage(squashedEdge.innerLabel.hbaseZkAddr, io.indexedEdgeMutations(edgeMutate), withWait = true).map { ret =>
            if (ret.isSuccess) {
              debug(ret.isSuccess, "mutate", squashedEdge.toSnapshotEdge, edgeMutate)
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
    * @param predicate    : indicate if this commitIndexEdgeMutations phase should be proceed or not.
    * @param statusCode   : current statusCode of this thread to process edges.
    * @param squashedEdge : squashed(in memory) final edge from input edges on same snapshotEdge. only for debug
    * @param edgeMutate   : actual collection of mutations. note that edgeMutate contains snapshotEdge mutations,
    *                     but in here, we only use indexEdge's degree mutations.
    * @return
    */
  protected def commitIndexEdgeDegreeMutations(predicate: Boolean,
                                               statusCode: Byte,
                                               squashedEdge: S2Edge,
                                               edgeMutate: EdgeMutate)(implicit ec: ExecutionContext): Future[Boolean] = {

    def _write(kvs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
      mutator.writeToStorage(squashedEdge.innerLabel.hbaseZkAddr, kvs, withWait = withWait).map { ret =>
        if (ret.isSuccess) {
          debug(ret.isSuccess, "increment", squashedEdge.toSnapshotEdge, edgeMutate)
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
        val (bufferIncr, nonBufferIncr) = io.increments(edgeMutate.deepCopy)

        if (bufferIncr.nonEmpty) _write(bufferIncr, withWait = false)
        _write(nonBufferIncr, withWait = true)
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

}
