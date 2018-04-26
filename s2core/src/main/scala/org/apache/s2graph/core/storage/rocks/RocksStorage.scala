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

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.hash.Hashing
import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.Storage
import org.apache.s2graph.core.storage.rocks.RocksHelper.RocksRPC
import org.apache.s2graph.core.utils.logger
import org.rocksdb._
import org.rocksdb.util.SizeUnit

import scala.util.{Random, Try}

object RocksStorage {

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

  override val management = new RocksStorageManagement(config, vdb, db)

  override val mutator = new RocksStorageWritable(db, vdb, lockMap)

  override val serDe = new RocksStorageSerDe(graph)

  override val reader = new RocksStorageReadable(graph, config, db, vdb, serDe, io)
}
