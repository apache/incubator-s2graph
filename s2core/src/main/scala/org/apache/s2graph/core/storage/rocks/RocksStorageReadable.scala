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

import java.util.Base64

import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema.{Label, ServiceColumn}
import org.apache.s2graph.core.storage.rocks.RocksHelper.{GetRequest, RocksRPC, ScanWithRange}
import org.apache.s2graph.core.storage.serde.StorageSerializable
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.types.{HBaseType, VertexId}
import org.rocksdb.RocksDB

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

class RocksStorageReadable(val graph: S2GraphLike,
                           val config: Config,
                           val db: RocksDB,
                           val vdb: RocksDB,
                           val serDe: StorageSerDe,
                           override val io: StorageIO) extends StorageReadable {

  private val table = Array.emptyByteArray
  private val qualifier = Array.emptyByteArray

  private def buildRequest(queryRequest: QueryRequest, edge: S2EdgeLike): RocksRPC = {
    queryRequest.queryParam.tgtVertexInnerIdOpt match {
      case None => // indexEdges
        val queryParam = queryRequest.queryParam
        val indexEdgeOpt = edge.edgesWithIndex.filter(edgeWithIndex => edgeWithIndex.labelIndex.seq == queryParam.labelOrderSeq).headOption
        val indexEdge = indexEdgeOpt.getOrElse(throw new RuntimeException(s"Can`t find index for query $queryParam"))
        val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
        val labelWithDirBytes = indexEdge.labelWithDir.bytes
        val labelIndexSeqWithIsInvertedBytes = StorageSerializable.labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

        val baseKey = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)

        val (intervalMaxBytes, intervalMinBytes) = queryParam.buildInterval(Option(edge))
        val (startKey, stopKey) =
          if (queryParam.intervalOpt.isDefined) {
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Base64.getDecoder.decode(cursor)
              case None => Bytes.add(baseKey, intervalMaxBytes)
            }
            (_startKey, Bytes.add(baseKey, intervalMinBytes))
          } else {
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Base64.getDecoder.decode(cursor)
              case None => baseKey
            }
            (_startKey, Bytes.add(baseKey, Array.fill(1)(-1)))
          }

        Right(ScanWithRange(SKeyValue.EdgeCf, startKey, stopKey, queryParam.innerOffset, queryParam.innerLimit))

      case Some(tgtId) => // snapshotEdge
        val kv = serDe.snapshotEdgeSerializer(graph.elementBuilder.toRequestEdge(queryRequest, Nil).toSnapshotEdge).toKeyValues.head
        Left(GetRequest(SKeyValue.EdgeCf, kv.row))
    }
  }

  private def buildRequest(queryRequest: QueryRequest, vertex: S2VertexLike): RocksRPC = {
    val startKey = vertex.id.bytes
    val stopKey = Bytes.add(startKey, Array.fill(1)(Byte.MaxValue))

    Right(ScanWithRange(SKeyValue.VertexCf, startKey, stopKey, 0, Byte.MaxValue))
//    val kv = serDe.vertexSerializer(vertex).toKeyValues.head
//    Left(GetRequest(SKeyValue.VertexCf, kv.row))
  }

  override def fetches(queryRequests: Seq[QueryRequest], prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val futures = for {
      queryRequest <- queryRequests
    } yield {
      val parentEdges = prevStepEdges.getOrElse(queryRequest.vertex.id, Nil)
      val edge = graph.elementBuilder.toRequestEdge(queryRequest, parentEdges)
      val rpc = buildRequest(queryRequest, edge)
      fetchKeyValues(rpc).map { kvs =>
        val queryParam = queryRequest.queryParam
        val stepResult = io.toEdges(kvs, queryRequest, queryRequest.prevStepScore, false, parentEdges)
        val edgeWithScores = stepResult.edgeWithScores.filter { case edgeWithScore =>
          val edge = edgeWithScore.edge
          val duration = queryParam.durationOpt.getOrElse((Long.MinValue, Long.MaxValue))
          edge.ts >= duration._1 && edge.ts < duration._2
        }

        stepResult.copy(edgeWithScores = edgeWithScores)
      }
    }

    Future.sequence(futures)
  }

  private def fetchKeyValues(rpc: RocksRPC)(implicit ec: ExecutionContext): Future[Seq[SKeyValue]] = {
    rpc match {
      case Left(GetRequest(cf, key)) =>
        val _db = if (Bytes.equals(cf, SKeyValue.VertexCf)) vdb else db
        val v = _db.get(key)

        val kvs =
          if (v == null) Seq.empty
          else Seq(SKeyValue(table, key, cf, qualifier, v, System.currentTimeMillis()))

        Future.successful(kvs)
      case Right(ScanWithRange(cf, startKey, stopKey, offset, limit)) =>
        val _db = if (Bytes.equals(cf, SKeyValue.VertexCf)) vdb else db
        val kvs = new ArrayBuffer[SKeyValue]()
        val iter = _db.newIterator()

        try {
          var idx = 0
          iter.seek(startKey)
          val (startOffset, len) = (offset, limit)
          while (iter.isValid && Bytes.compareTo(iter.key, stopKey) <= 0 && idx < startOffset + len) {
            if (idx >= startOffset) {
              kvs += SKeyValue(table, iter.key, cf, qualifier, iter.value, System.currentTimeMillis())
            }

            iter.next()
            idx += 1
          }
        } finally {
          iter.close()
        }
        Future.successful(kvs)
    }
  }

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

  override def fetchKeyValues(queryRequest: QueryRequest, edge: S2EdgeLike)(implicit ec: ExecutionContext) = {
    fetchKeyValues(buildRequest(queryRequest, edge))
  }

  override def fetchKeyValues(queryRequest: QueryRequest, vertex: S2VertexLike)(implicit ec: ExecutionContext) = {
    fetchKeyValues(buildRequest(queryRequest, vertex))
  }
}
