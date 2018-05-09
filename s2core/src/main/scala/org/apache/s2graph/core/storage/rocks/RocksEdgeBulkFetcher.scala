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
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.{EdgeBulkFetcher, S2EdgeLike, S2GraphLike}
import org.apache.s2graph.core.storage.{SKeyValue, StorageIO, StorageSerDe}
import org.apache.s2graph.core.types.HBaseType
import org.rocksdb.RocksDB

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

class RocksEdgeBulkFetcher(val graph: S2GraphLike,
                           val config: Config,
                           val db: RocksDB,
                           val vdb: RocksDB,
                           val serDe: StorageSerDe,
                           val io: StorageIO) extends EdgeBulkFetcher  {
  import RocksStorage._

  override def fetchEdgesAll()(implicit ec: ExecutionContext) = {
    val edges = new ArrayBuffer[S2EdgeLike]()
    Label.findAll().groupBy(_.hbaseTableName).toSeq.foreach { case (hTableName, labels) =>
      val distinctLabels = labels.toSet

      val iter = db.newIterator()
      try {
        iter.seekToFirst()
        while (iter.isValid) {
          val kv = SKeyValue(table, iter.key(), SKeyValue.EdgeCf, qualifier, iter.value, System.currentTimeMillis())

          serDe.indexEdgeDeserializer(schemaVer = HBaseType.DEFAULT_VERSION).fromKeyValues(Seq(kv), None)
            .filter(e => distinctLabels(e.innerLabel) && e.getDirection() == "out" && !e.isDegree)
            .foreach { edge =>
              edges += edge
            }


          iter.next()
        }

      } finally {
        iter.close()
      }
    }

    Future.successful(edges)
  }
}
