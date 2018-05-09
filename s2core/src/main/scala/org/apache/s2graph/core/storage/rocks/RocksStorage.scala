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
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.hash.Hashing
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.rocks.RocksHelper.{GetRequest, RocksRPC, ScanWithRange}
import org.apache.s2graph.core.storage.serde.StorageSerializable
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.utils.logger
import org.rocksdb._
import org.rocksdb.util.SizeUnit

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

object RocksStorage {
  val table = Array.emptyByteArray
  val qualifier = Array.emptyByteArray

  RocksDB.loadLibrary()

  val LockExpireDuration = 60000
  val FilePathKey = "rocks.storage.file.path"
  val StorageModeKey = "rocks.storage.mode"
  val TtlKey = "rocks.storage.ttl"
  val ReadOnlyKey = "rocks.storage.read.only"
  val VertexPostfix = "vertex"
  val EdgePostfix = "edge"

  val dbPool = CacheBuilder.newBuilder()
    .concurrencyLevel(8)
    .expireAfterAccess(LockExpireDuration, TimeUnit.MILLISECONDS)
    .expireAfterWrite(LockExpireDuration, TimeUnit.MILLISECONDS)
    .maximumSize(1000 * 10 * 10 * 10 * 10)
    .build[String, (RocksDB, RocksDB)]

//  val writeOptions = new WriteOptions()

  val tableOptions = new BlockBasedTableConfig
  tableOptions
    .setBlockCacheSize(1 * SizeUnit.GB)
    .setCacheIndexAndFilterBlocks(true)
    .setHashIndexAllowCollision(false)

  val options = new Options
  options
    .setCreateIfMissing(true)
    .setMergeOperatorName("uint64add")
    .setTableCacheNumshardbits(5)
    .setIncreaseParallelism(8)
    .setArenaBlockSize(1024 * 32)
    .setTableFormatConfig(tableOptions)
    .setWriteBufferSize(1024 * 1024 * 512)

  def configKey(config: Config): Long =
    Hashing.murmur3_128().hashBytes(config.toString.getBytes("UTF-8")).asLong()

  def getFilePath(config: Config): String = config.getString(FilePathKey)

  def getOrElseUpdate(config: Config): (RocksDB, RocksDB) = {
    val path = config.getString(FilePathKey)
    val storageMode = Try { config.getString(StorageModeKey) }.getOrElse("test")
    val ttl = Try { config.getInt(TtlKey) }.getOrElse(-1)
    val readOnly = Try { config.getBoolean(ReadOnlyKey) } getOrElse(false)

    val newPair = (
      openDatabase(path, options, VertexPostfix, storageMode, ttl, readOnly),
      openDatabase(path, options, EdgePostfix, storageMode, ttl, readOnly)
    )
    dbPool.asMap().putIfAbsent(path, newPair) match {
      case null => newPair
      case old =>
        newPair._1.close()
        newPair._2.close()
        old
    }
  }

  def shutdownAll(): Unit = {
    import scala.collection.JavaConversions._
    dbPool.asMap.foreach { case (k, (vdb, edb)) =>
        vdb.close()
        edb.close()
    }
  }

  private def openDatabase(path: String,
                           options: Options,
                           postfix: String = "",
                           storageMode: String = "production",
                           ttl: Int = -1,
                           readOnly: Boolean = false): RocksDB = {
    try {
      val filePath = s"${path}_${postfix}"
      logger.info(s"Open RocksDB: ${filePath}")

      storageMode match {
        case "test" => RocksDB.open(options, s"${filePath}_${Random.nextInt()}")
        case _ =>
          if (ttl < 0) {
            if (readOnly) RocksDB.openReadOnly(options, filePath)
            else RocksDB.open(options, filePath)
          } else {
            TtlDB.open(options, filePath, ttl, readOnly)
          }
      }
    } catch {
      case e: RocksDBException =>
        logger.error(s"initialize rocks db storage failed.", e)
        throw e
    }
  }

  def buildRequest(graph: S2GraphLike, serDe: StorageSerDe, queryRequest: QueryRequest, edge: S2EdgeLike): RocksRPC = {
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

  def buildRequest(queryRequest: QueryRequest, vertex: S2VertexLike): RocksRPC = {
    val startKey = vertex.id.bytes
    val stopKey = Bytes.add(startKey, Array.fill(1)(Byte.MaxValue))

    Right(ScanWithRange(SKeyValue.VertexCf, startKey, stopKey, 0, Byte.MaxValue))
  }

  def fetchKeyValues(vdb: RocksDB, db: RocksDB, rpc: RocksRPC)(implicit ec: ExecutionContext): Future[Seq[SKeyValue]] = {
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
}

class RocksStorage(override val graph: S2GraphLike,
                   override val config: Config) extends Storage(graph, config) {

  import RocksStorage._
  var closed = false

  lazy val (vdb, db) = getOrElseUpdate(config)

  val cacheLoader = new CacheLoader[String, ReentrantLock] {
    override def load(key: String) = new ReentrantLock()
  }

  val lockMap: LoadingCache[String, ReentrantLock] = CacheBuilder.newBuilder()
    .concurrencyLevel(8)
    .expireAfterAccess(graph.LockExpireDuration, TimeUnit.MILLISECONDS)
    .expireAfterWrite(graph.LockExpireDuration, TimeUnit.MILLISECONDS)
    .maximumSize(1000 * 10 * 10 * 10 * 10)
    .build[String, ReentrantLock](cacheLoader)

  private lazy val optimisticEdgeFetcher = new RocksOptimisticEdgeFetcher(graph, config, db, vdb, serDe, io)
  private lazy val optimisticMutator = new RocksOptimisticMutator(graph, serDe, optimisticEdgeFetcher, db, vdb, lockMap)

  override val management: StorageManagement = new RocksStorageManagement(config, vdb, db)
  override val serDe: StorageSerDe = new RocksStorageSerDe(graph)

  override val edgeFetcher: EdgeFetcher = new RocksEdgeFetcher(graph, config, db, vdb, serDe, io)
  override val vertexFetcher: VertexFetcher = new RocksVertexFetcher(graph, config, db, vdb, serDe, io)

  override val edgeMutator: EdgeMutator = new DefaultOptimisticEdgeMutator(graph, serDe, optimisticEdgeFetcher, optimisticMutator, io)
  override val vertexMutator: VertexMutator = new DefaultOptimisticVertexMutator(graph, serDe, optimisticEdgeFetcher, optimisticMutator, io)
}
