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
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.schema.ServiceColumn
import org.apache.s2graph.core.{S2GraphLike, S2VertexLike, VertexBulkFetcher}
import org.apache.s2graph.core.storage.{SKeyValue, StorageIO, StorageSerDe}
import org.apache.s2graph.core.types.HBaseType
import org.rocksdb.RocksDB

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

class RocksVertexBulkFetcher(val graph: S2GraphLike,
                             val config: Config,
                             val db: RocksDB,
                             val vdb: RocksDB,
                             val serDe: StorageSerDe,
                             val io: StorageIO) extends VertexBulkFetcher {
  import RocksStorage._

  override def fetchVerticesAll()(implicit ec: ExecutionContext) = {
    import scala.collection.mutable

    val vertices = new ArrayBuffer[S2VertexLike]()
    ServiceColumn.findAll().groupBy(_.service.hTableName).toSeq.foreach { case (hTableName, columns) =>
      val distinctColumns = columns.toSet

      val iter = vdb.newIterator()
      val buffer = mutable.ListBuffer.empty[SKeyValue]
      var oldVertexIdBytes = Array.empty[Byte]
      var minusPos = 0

      try {
        iter.seekToFirst()
        while (iter.isValid) {
          val row = iter.key()
          if (!Bytes.equals(oldVertexIdBytes, 0, oldVertexIdBytes.length - minusPos, row, 0, row.length - 1)) {
            if (buffer.nonEmpty)
              serDe.vertexDeserializer(schemaVer = HBaseType.DEFAULT_VERSION).fromKeyValues(buffer, None)
                .filter(v => distinctColumns(v.serviceColumn))
                .foreach { vertex =>
                  vertices += vertex
                }

            oldVertexIdBytes = row
            minusPos = 1
            buffer.clear()
          }
          val kv = SKeyValue(table, iter.key(), SKeyValue.VertexCf, qualifier, iter.value(), System.currentTimeMillis())
          buffer += kv

          iter.next()
        }
        if (buffer.nonEmpty)
          serDe.vertexDeserializer(schemaVer = HBaseType.DEFAULT_VERSION).fromKeyValues(buffer, None)
            .filter(v => distinctColumns(v.serviceColumn))
            .foreach { vertex =>
              vertices += vertex
            }

      } finally {
        iter.close()
      }
    }

    Future.successful(vertices)
  }
}
