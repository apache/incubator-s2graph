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

package org.apache.s2graph.core.storage.hbase

import com.typesafe.config.Config
import org.apache.s2graph.core.schema.ServiceColumn
import org.apache.s2graph.core.storage.serde.Serializable
import org.apache.s2graph.core.storage.{StorageIO, StorageSerDe}
import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.core.utils.Extensions
import org.apache.s2graph.core.{S2Graph, S2GraphLike, VertexBulkFetcher}
import org.hbase.async.HBaseClient

import scala.concurrent.{ExecutionContext, Future}

class AsynchbaseVertexBulkFetcher(val graph: S2GraphLike,
                                  val config: Config,
                                  val client: HBaseClient,
                                  val serDe: StorageSerDe,
                                  val io: StorageIO) extends VertexBulkFetcher {

  import AsynchbaseStorage._
  import Extensions.DeferOps

  import scala.collection.JavaConverters._

  override def fetchVerticesAll()(implicit ec: ExecutionContext) = {
    val futures = ServiceColumn.findAll().groupBy(_.service.hTableName).toSeq.map { case (hTableName, columns) =>
      val distinctColumns = columns.toSet
      val scan = AsynchbasePatcher.newScanner(client, hTableName)
      scan.setFamily(Serializable.vertexCf)
      scan.setMaxVersions(1)

      scan.nextRows(S2Graph.FetchAllLimit).toFuture(emptyKeyValuesLs).map {
        case null => Seq.empty
        case kvsLs =>
          kvsLs.asScala.flatMap { kvs =>
            serDe.vertexDeserializer(schemaVer = HBaseType.DEFAULT_VERSION).fromKeyValues(kvs.asScala, None)
              .filter(v => distinctColumns(v.serviceColumn))
          }
      }
    }
    Future.sequence(futures).map(_.flatten)
  }

}
