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

import java.util

import com.typesafe.config.Config
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.storage.serde.Serializable
import org.apache.s2graph.core.{EdgeBulkFetcher, S2EdgeLike, S2Graph, S2GraphLike}
import org.apache.s2graph.core.storage.{CanSKeyValue, StorageIO, StorageSerDe}
import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.core.utils.{CanDefer, Extensions}
import org.hbase.async.{HBaseClient, KeyValue}

import scala.concurrent.{ExecutionContext, Future}

class AsynchbaseEdgeBulkFetcher(val graph: S2GraphLike,
                                val config: Config,
                                val client: HBaseClient,
                                val serDe: StorageSerDe,
                                val io: StorageIO) extends EdgeBulkFetcher {
  import Extensions.DeferOps
  import CanDefer._
  import scala.collection.JavaConverters._
  import AsynchbaseStorage._

  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] = {
    val futures = Label.findAll().groupBy(_.hbaseTableName).toSeq.map { case (hTableName, labels) =>
      val distinctLabels = labels.toSet
      val scan = AsynchbasePatcher.newScanner(client, hTableName)
      scan.setFamily(Serializable.edgeCf)
      scan.setMaxVersions(1)

      scan.nextRows(S2Graph.FetchAllLimit).toFuture(emptyKeyValuesLs).map {
        case null => Seq.empty
        case kvsLs =>
          kvsLs.asScala.flatMap { kvs =>
            kvs.asScala.flatMap { kv =>
              val sKV = implicitly[CanSKeyValue[KeyValue]].toSKeyValue(kv)

              serDe.indexEdgeDeserializer(schemaVer = HBaseType.DEFAULT_VERSION)
                .fromKeyValues(Seq(kv), None)
                .filter(e => distinctLabels(e.innerLabel) && e.getDirection() == "out" && !e.isDegree)
            }
          }
      }
    }

    Future.sequence(futures).map(_.flatten)
  }
}
