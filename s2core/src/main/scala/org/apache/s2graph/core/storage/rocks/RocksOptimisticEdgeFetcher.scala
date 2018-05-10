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

package org.apache.s2graph.core.storage.rocks

import com.typesafe.config.Config
import org.apache.s2graph.core.{QueryRequest, S2EdgeLike, S2GraphLike}
import org.apache.s2graph.core.storage.{OptimisticEdgeFetcher, SKeyValue, StorageIO, StorageSerDe}
import org.rocksdb.RocksDB

import scala.concurrent.{ExecutionContext, Future}

class RocksOptimisticEdgeFetcher(val graph: S2GraphLike,
                                 val config: Config,
                                 val db: RocksDB,
                                 val vdb: RocksDB,
                                 val serDe: StorageSerDe,
                                 val io: StorageIO) extends OptimisticEdgeFetcher {

  override protected def fetchKeyValues(queryRequest: QueryRequest, edge: S2EdgeLike)(implicit ec: ExecutionContext): Future[Seq[SKeyValue]] = {
    val request = RocksStorage.buildRequest(graph, serDe, queryRequest, edge)

    RocksStorage.fetchKeyValues(vdb, db, request)
  }
}
