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


import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.serde.{Deserializable, MutationHelper}
import org.apache.s2graph.core.storage.serde.indexedge.tall.IndexEdgeDeserializable
import org.apache.s2graph.core.types._

import scala.concurrent.{ExecutionContext, Future}

abstract class Storage(val graph: S2GraphLike,
                          val config: Config) {
  /* Storage backend specific resource management */
  val management: StorageManagement

  /* Physically store given KeyValue into backend storage. */
  val mutator: StorageWritable

  /*
   * Given QueryRequest/Vertex/Edge, fetch KeyValue from storage
   * then convert them into Edge/Vertex
   */
  val reader: StorageReadable

  /*
   * Serialize Edge/Vertex, to common KeyValue, SKeyValue that
   * can be stored aligned to backend storage's physical schema.
   * Also Deserialize storage backend's KeyValue to SKeyValue.
   */
  val serDe: StorageSerDe

  /*
   * Common helper to translate SKeyValue to Edge/Vertex and vice versa.
   * Note that it require storage backend specific implementation for serialize/deserialize.
   */
  lazy val io: StorageIO = new StorageIO(graph, serDe)

  /*
   * Common helper to resolve write-write conflict on snapshot edge with same EdgeId.
   * Note that it require storage backend specific implementations for
   * all of StorageWritable, StorageReadable, StorageSerDe, StorageIO
   */
  lazy val conflictResolver: WriteWriteConflictResolver = new WriteWriteConflictResolver(graph, serDe, io, mutator, reader)

  lazy val mutationHelper: MutationHelper = new MutationHelper(this)

  /** Mutation **/
  def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] =
    mutator.writeToStorage(cluster, kvs, withWait)

  def writeLock(requestKeyValue: SKeyValue, expectedOpt: Option[SKeyValue])(implicit ec: ExecutionContext): Future[MutateResponse] =
    mutator.writeLock(requestKeyValue, expectedOpt)

  /** Fetch **/
  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] =
    reader.fetches(queryRequests, prevStepEdges)

  def fetchVertices(vertices: Seq[S2VertexLike])(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] =
    reader.fetchVertices(vertices)

  def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] = reader.fetchEdgesAll()

  def fetchVerticesAll()(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = reader.fetchVerticesAll()

  def fetchSnapshotEdgeInner(edge: S2EdgeLike)(implicit ec: ExecutionContext): Future[(Option[S2EdgeLike], Option[SKeyValue])] =
    reader.fetchSnapshotEdgeInner(edge)

  /** Management **/
  def flush(): Unit = management.flush()

  def createTable(config: Config, tableNameStr: String): Unit = management.createTable(config, tableNameStr)

  def truncateTable(config: Config, tableNameStr: String): Unit = management.truncateTable(config, tableNameStr)

  def deleteTable(config: Config, tableNameStr: String): Unit = management.deleteTable(config, tableNameStr)

  def shutdown(): Unit = management.shutdown()

  def info: Map[String, String] = Map("className" -> this.getClass.getSimpleName)

  def deleteAllFetchedEdgesAsyncOld(stepInnerResult: StepResult,
                                    requestTs: Long,
                                    retryNum: Int)(implicit ec: ExecutionContext): Future[Boolean] =
    mutationHelper.deleteAllFetchedEdgesAsyncOld(stepInnerResult, requestTs, retryNum)

  def mutateVertex(zkQuorum: String, vertex: S2VertexLike, withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] =
    mutationHelper.mutateVertex(zkQuorum: String, vertex, withWait)

  def mutateStrongEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[Boolean]] =
    mutationHelper.mutateStrongEdges(zkQuorum, _edges, withWait)


  def mutateWeakEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[(Int, Boolean)]] =
    mutationHelper.mutateWeakEdges(zkQuorum, _edges, withWait)

  def incrementCounts(zkQuorum: String, edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[MutateResponse]] =
    mutationHelper.incrementCounts(zkQuorum, edges, withWait)

  def updateDegree(zkQuorum: String, edge: S2EdgeLike, degreeVal: Long = 0)(implicit ec: ExecutionContext): Future[MutateResponse] =
    mutationHelper.updateDegree(zkQuorum, edge, degreeVal)
}
