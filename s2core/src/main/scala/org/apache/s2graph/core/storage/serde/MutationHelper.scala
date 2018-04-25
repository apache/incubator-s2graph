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

package org.apache.s2graph.core.storage.serde

import org.apache.s2graph.core.schema.LabelMeta
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.utils.logger

import scala.concurrent.{ExecutionContext, Future}

class MutationHelper(storage: Storage) {
  val serDe = storage.serDe
  val io = storage.io
  val fetcher = storage.fetcher
  val mutator = storage.mutator
  val conflictResolver = storage.conflictResolver

  private def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] =
    mutator.writeToStorage(cluster, kvs, withWait)

  def deleteAllFetchedEdgesAsyncOld(stepInnerResult: StepResult,
                                    requestTs: Long,
                                    retryNum: Int)(implicit ec: ExecutionContext): Future[Boolean] = {
    if (stepInnerResult.isEmpty) Future.successful(true)
    else {
      val head = stepInnerResult.edgeWithScores.head
      val zkQuorum = head.edge.innerLabel.hbaseZkAddr
      val futures = for {
        edgeWithScore <- stepInnerResult.edgeWithScores
      } yield {
        val edge = edgeWithScore.edge

        val edgeSnapshot = edge.copyEdgeWithState(S2Edge.propsToState(edge.updatePropsWithTs()))
        val reversedSnapshotEdgeMutations = serDe.snapshotEdgeSerializer(edgeSnapshot.toSnapshotEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put))

        val edgeForward = edge.copyEdgeWithState(S2Edge.propsToState(edge.updatePropsWithTs()))
        val forwardIndexedEdgeMutations = edgeForward.edgesWithIndex.flatMap { indexEdge =>
          serDe.indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
            io.buildIncrementsAsync(indexEdge, -1L)
        }

        /* reverted direction */
        val edgeRevert = edge.copyEdgeWithState(S2Edge.propsToState(edge.updatePropsWithTs()))
        val reversedIndexedEdgesMutations = edgeRevert.duplicateEdge.edgesWithIndex.flatMap { indexEdge =>
          serDe.indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
            io.buildIncrementsAsync(indexEdge, -1L)
        }

        val mutations = reversedIndexedEdgesMutations ++ reversedSnapshotEdgeMutations ++ forwardIndexedEdgeMutations

        writeToStorage(zkQuorum, mutations, withWait = true)
      }

      Future.sequence(futures).map { rets => rets.forall(_.isSuccess) }
    }
  }

  def mutateVertex(zkQuorum: String, vertex: S2VertexLike, withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] = {
    if (vertex.op == GraphUtil.operations("delete")) {
      writeToStorage(zkQuorum,
        serDe.vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Delete)), withWait)
    } else if (vertex.op == GraphUtil.operations("deleteAll")) {
      logger.info(s"deleteAll for vertex is truncated. $vertex")
      Future.successful(MutateResponse.Success) // Ignore withWait parameter, because deleteAll operation may takes long time
    } else {
      writeToStorage(zkQuorum, io.buildPutsAll(vertex), withWait)
    }
  }

  def mutateWeakEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[(Int, Boolean)]] = {
    val mutations = _edges.flatMap { edge =>
      val (_, edgeUpdate) =
        if (edge.getOp() == GraphUtil.operations("delete")) S2Edge.buildDeleteBulk(None, edge)
        else S2Edge.buildOperation(None, Seq(edge))

      val (bufferIncr, nonBufferIncr) = io.increments(edgeUpdate.deepCopy)

      if (bufferIncr.nonEmpty) storage.writeToStorage(zkQuorum, bufferIncr, withWait = false)
      io.buildVertexPutsAsync(edge) ++ io.indexedEdgeMutations(edgeUpdate.deepCopy) ++ io.snapshotEdgeMutations(edgeUpdate.deepCopy) ++ nonBufferIncr
    }

    writeToStorage(zkQuorum, mutations, withWait).map { ret =>
      _edges.zipWithIndex.map { case (edge, idx) =>
        idx -> ret.isSuccess
      }
    }
  }

  def mutateStrongEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[Boolean]] = {
    def mutateEdgesInner(edges: Seq[S2EdgeLike],
                         checkConsistency: Boolean,
                         withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] = {
      assert(edges.nonEmpty)
      // TODO:: remove after code review: unreachable code
      if (!checkConsistency) {

        val futures = edges.map { edge =>
          val (_, edgeUpdate) = S2Edge.buildOperation(None, Seq(edge))

          val (bufferIncr, nonBufferIncr) = io.increments(edgeUpdate.deepCopy)
          val mutations =
            io.indexedEdgeMutations(edgeUpdate.deepCopy) ++ io.snapshotEdgeMutations(edgeUpdate.deepCopy) ++ nonBufferIncr

          if (bufferIncr.nonEmpty) writeToStorage(zkQuorum, bufferIncr, withWait = false)

          writeToStorage(zkQuorum, mutations, withWait)
        }
        Future.sequence(futures).map { rets => new MutateResponse(rets.forall(_.isSuccess)) }
      } else {
        fetcher.fetchSnapshotEdgeInner(edges.head).flatMap { case (snapshotEdgeOpt, kvOpt) =>
          conflictResolver.retry(1)(edges, 0, snapshotEdgeOpt).map(new MutateResponse(_))
        }
      }
    }

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
          val edgeFuture = mutateEdgesInner(edges, checkConsistency = true, withWait)

          //TODO: decide what we will do on failure on vertex put
          val puts = io.buildVertexPutsAsync(head)
          val vertexFuture = writeToStorage(head.innerLabel.hbaseZkAddr, puts, withWait)
          Seq(edgeFuture, vertexFuture)
        case Nil => Nil
      }

      val composed = for {
      //        deleteRet <- Future.sequence(deleteAllFutures)
        mutateRet <- Future.sequence(mutateEdgeFutures)
      } yield mutateRet

      composed.map(_.forall(_.isSuccess)).map { ret => idxs.map(idx => idx -> ret) }
    }

    Future.sequence(mutateEdges).map { squashedRets =>
      squashedRets.flatten.sortBy { case (idx, ret) => idx }.map(_._2)
    }
  }

  def incrementCounts(zkQuorum: String, edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[MutateResponse]] = {
    val futures = for {
      edge <- edges
    } yield {
      val kvs = for {
        relEdge <- edge.relatedEdges
        edgeWithIndex <- EdgeMutate.filterIndexOption(relEdge.edgesWithIndexValid)
      } yield {
        val countWithTs = edge.propertyValueInner(LabelMeta.count)
        val countVal = countWithTs.innerVal.toString().toLong
        io.buildIncrementsCountAsync(edgeWithIndex, countVal).head
      }
      writeToStorage(zkQuorum, kvs, withWait = withWait)
    }

    Future.sequence(futures)
  }

  def updateDegree(zkQuorum: String, edge: S2EdgeLike, degreeVal: Long = 0)(implicit ec: ExecutionContext): Future[MutateResponse] = {
    val kvs = io.buildDegreePuts(edge, degreeVal)

    mutator.writeToStorage(zkQuorum, kvs, withWait = true)
  }
}
