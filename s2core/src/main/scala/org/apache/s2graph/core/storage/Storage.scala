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

abstract class Storage(val graph: S2GraphLike,
                          val config: Config) {
  /* Storage backend specific resource management */
  val management: StorageManagement

  /*
     * Serialize Edge/Vertex, to common KeyValue, SKeyValue that
     * can be stored aligned to backend storage's physical schema.
     * Also Deserialize storage backend's KeyValue to SKeyValue.
     */
  val serDe: StorageSerDe

  val edgeFetcher: EdgeFetcher

  val vertexFetcher: VertexFetcher

  val edgeMutator: EdgeMutator

  val vertexMutator: VertexMutator

  /*
   * Common helper to translate SKeyValue to Edge/Vertex and vice versa.
   * Note that it require storage backend specific implementation for serialize/deserialize.
   */
  lazy val io: StorageIO = new StorageIO(graph, serDe)


  /** Management **/
  def flush(): Unit = management.flush()

  def createTable(config: Config, tableNameStr: String): Unit = management.createTable(config, tableNameStr)

  def truncateTable(config: Config, tableNameStr: String): Unit = management.truncateTable(config, tableNameStr)

  def deleteTable(config: Config, tableNameStr: String): Unit = management.deleteTable(config, tableNameStr)

  def shutdown(): Unit = management.shutdown()

  def info: Map[String, String] = Map("className" -> this.getClass.getSimpleName)

}
