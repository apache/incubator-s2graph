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
import org.apache.s2graph.core.storage.serde.Deserializable
import org.apache.s2graph.core.storage.serde.indexedge.tall.IndexEdgeDeserializable
import org.apache.s2graph.core.types._
import scala.concurrent.{ExecutionContext, Future}

abstract class Storage(val graph: S2Graph,
                          val config: Config) {
  /* Storage backend specific resource management */
  val management: StorageManagement

  /* Physically store given KeyValue into backend storage. */
  val mutator: StorageWritable

  /*
   * Given QueryRequest/Vertex/Edge, fetch KeyValue from storage
   * then convert them into Edge/Vertex
   */
  val fetcher: StorageReadable

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
  lazy val conflictResolver: WriteWriteConflictResolver = new WriteWriteConflictResolver(graph, serDe, io, mutator, fetcher)

  /** IO **/
  def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): serde.Serializable[SnapshotEdge] =
    serDe.snapshotEdgeSerializer(snapshotEdge)

  def indexEdgeSerializer(indexEdge: IndexEdge): serde.Serializable[IndexEdge] =
    serDe.indexEdgeSerializer(indexEdge)

  def vertexSerializer(vertex: S2Vertex): serde.Serializable[S2Vertex] =
    serDe.vertexSerializer(vertex)

  def snapshotEdgeDeserializer(schemaVer: String): Deserializable[SnapshotEdge] =
    serDe.snapshotEdgeDeserializer(schemaVer)

  def indexEdgeDeserializer(schemaVer: String): IndexEdgeDeserializable =
    serDe.indexEdgeDeserializer(schemaVer)

  def vertexDeserializer(schemaVer: String): Deserializable[S2Vertex] =
    serDe.vertexDeserializer(schemaVer)

  /** Mutation Builder */
  def increments(edgeMutate: EdgeMutate): (Seq[SKeyValue], Seq[SKeyValue]) =
    io.increments(edgeMutate)

  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] =
    io.indexedEdgeMutations(edgeMutate)

  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] =
    io.buildIncrementsAsync(indexedEdge, amount)

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] =
    io.buildIncrementsCountAsync(indexedEdge, amount)

  def buildVertexPutsAsync(edge: S2Edge): Seq[SKeyValue] =
    io.buildVertexPutsAsync(edge)

  def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] =
    io.snapshotEdgeMutations(edgeMutate)

  def buildDegreePuts(edge: S2Edge, degreeVal: Long): Seq[SKeyValue] =
    io.buildDegreePuts(edge, degreeVal)

  def buildPutsAll(vertex: S2Vertex): Seq[SKeyValue] =
    io.buildPutsAll(vertex)

  /** Mutation **/

  def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] =
    mutator.writeToStorage(cluster, kvs, withWait)

  def writeLock(requestKeyValue: SKeyValue, expectedOpt: Option[SKeyValue])(implicit ec: ExecutionContext): Future[MutateResponse] =
    mutator.writeLock(requestKeyValue, expectedOpt)

  /** Fetch **/
  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] =
    fetcher.fetches(queryRequests, prevStepEdges)

  def fetchVertices(vertices: Seq[S2Vertex])(implicit ec: ExecutionContext): Future[Seq[S2Vertex]] =
    fetcher.fetchVertices(vertices)

  def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2Edge]] = fetcher.fetchEdgesAll()

  def fetchVerticesAll()(implicit ec: ExecutionContext): Future[Seq[S2Vertex]] = fetcher.fetchVerticesAll()

  def fetchSnapshotEdgeInner(edge: S2Edge)(implicit ec: ExecutionContext): Future[(Option[S2Edge], Option[SKeyValue])] =
    fetcher.fetchSnapshotEdgeInner(edge)

  /** Conflict Resolver **/
  def retry(tryNum: Int)(edges: Seq[S2Edge], statusCode: Byte, fetchedSnapshotEdgeOpt: Option[S2Edge])(implicit ec: ExecutionContext): Future[Boolean] =
    conflictResolver.retry(tryNum)(edges, statusCode, fetchedSnapshotEdgeOpt)

  /** Management **/

  def flush(): Unit = management.flush()

  def createTable(config: Config, tableNameStr: String): Unit = management.createTable(config, tableNameStr)

  def truncateTable(config: Config, tableNameStr: String): Unit = management.truncateTable(config, tableNameStr)

  def deleteTable(config: Config, tableNameStr: String): Unit = management.deleteTable(config, tableNameStr)

  def shutdown(): Unit = management.shutdown()

  def info: Map[String, String] = Map("className" -> this.getClass.getSimpleName)
}
