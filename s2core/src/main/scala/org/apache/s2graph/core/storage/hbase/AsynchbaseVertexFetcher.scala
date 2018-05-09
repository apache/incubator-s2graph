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
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema.ServiceColumn
import org.apache.s2graph.core.storage.serde.Serializable
import org.apache.s2graph.core.storage.{SKeyValue, StorageIO, StorageSerDe}
import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.core.utils.Extensions
import org.hbase.async.HBaseClient

import scala.concurrent.{ExecutionContext, Future}

class AsynchbaseVertexFetcher(val graph: S2GraphLike,
                              val config: Config,
                              val client: HBaseClient,
                              val serDe: StorageSerDe,
                              val io: StorageIO) extends VertexFetcher  {
  import AsynchbaseStorage._
  import Extensions.DeferOps
  import scala.collection.JavaConverters._


  private def fetchKeyValues(queryRequest: QueryRequest, vertex: S2VertexLike)(implicit ec: ExecutionContext): Future[Seq[SKeyValue]] = {
    val rpc = buildRequest(serDe, queryRequest, vertex)
    AsynchbaseStorage.fetchKeyValues(client, rpc)
  }

  override def fetchVertices(vertices: Seq[S2VertexLike])(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = {
    def fromResult(kvs: Seq[SKeyValue], version: String): Seq[S2VertexLike] = {
      if (kvs.isEmpty) Nil
      else serDe.vertexDeserializer(version).fromKeyValues(kvs, None).toSeq
    }

    val futures = vertices.map { vertex =>
      val queryParam = QueryParam.Empty
      val q = Query.toQuery(Seq(vertex), Seq(queryParam))
      val queryRequest = QueryRequest(q, stepIdx = -1, vertex, queryParam)

      fetchKeyValues(queryRequest, vertex).map { kvs =>
        fromResult(kvs, vertex.serviceColumn.schemaVersion)
      } recoverWith {
        case ex: Throwable => Future.successful(Nil)
      }
    }

    Future.sequence(futures).map(_.flatten)
  }

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
