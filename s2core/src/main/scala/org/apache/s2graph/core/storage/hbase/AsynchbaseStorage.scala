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
import java.util.Base64
import java.util.concurrent.{ExecutorService, Executors}

import com.stumbleupon.async.Deferred
import com.typesafe.config.Config
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.storage.serde._
import org.apache.s2graph.core.types.{HBaseType, VertexId}
import org.apache.s2graph.core.utils._
import org.hbase.async.FilterList.Operator.MUST_PASS_ALL
import org.hbase.async._

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}


object AsynchbaseStorage {
  import Extensions.DeferOps
  import CanDefer._

  val vertexCf = Serializable.vertexCf
  val edgeCf = Serializable.edgeCf

  val emptyKVs = new util.ArrayList[KeyValue]()
  val emptyKeyValues = new util.ArrayList[KeyValue]()
  val emptyKeyValuesLs = new util.ArrayList[util.ArrayList[KeyValue]]()
  val emptyStepResult = new util.ArrayList[StepResult]()

  AsynchbasePatcher.init()


  def makeClient(config: Config, overrideKv: (String, String)*) = {
    val asyncConfig: org.hbase.async.Config =
      if (config.hasPath("hbase.security.auth.enable") && config.getBoolean("hbase.security.auth.enable")) {
        val krb5Conf = config.getString("java.security.krb5.conf")
        val jaas = config.getString("java.security.auth.login.config")

        System.setProperty("java.security.krb5.conf", krb5Conf)
        System.setProperty("java.security.auth.login.config", jaas)
        new org.hbase.async.Config()
      } else {
        new org.hbase.async.Config()
      }

    for (entry <- config.entrySet() if entry.getKey.contains("hbase")) {
      asyncConfig.overrideConfig(entry.getKey, entry.getValue.unwrapped().toString)
    }

    for ((k, v) <- overrideKv) {
      asyncConfig.overrideConfig(k, v)
    }

    val client = new HBaseClient(asyncConfig)
    logger.info(s"Asynchbase: ${client.getConfig.dumpConfiguration()}")
    client
  }

  def shutdown(client: HBaseClient): Unit = {
    client.shutdown().join()
  }

  case class ScanWithRange(scan: Scanner, offset: Int, limit: Int)

  type AsyncRPC = Either[GetRequest, ScanWithRange]

  def initLocalHBase(config: Config,
                     overwrite: Boolean = true): ExecutorService = {
    import java.io.{File, IOException}
    import java.net.Socket

    lazy val hbaseExecutor = {
      val executor = Executors.newSingleThreadExecutor()

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = {
          executor.shutdown()
        }
      })

      val hbaseAvailable = try {
        val (host, port) = config.getString("hbase.zookeeper.quorum").split(":") match {
          case Array(h, p) => (h, p.toInt)
          case Array(h) => (h, 2181)
        }

        val socket = new Socket(host, port)
        socket.close()
        logger.info(s"HBase is available.")
        true
      } catch {
        case e: IOException =>
          logger.info(s"HBase is not available.")
          false
      }

      if (!hbaseAvailable) {
        // start HBase
        executor.submit(new Runnable {
          override def run(): Unit = {
            logger.info(s"HMaster starting...")
            val ts = System.currentTimeMillis()
            val cwd = new File(".").getAbsolutePath
            if (overwrite) {
              val dataDir = new File(s"$cwd/storage/s2graph")
              FileUtils.deleteDirectory(dataDir)
            }

            System.setProperty("proc_master", "")
            System.setProperty("hbase.log.dir", s"$cwd/storage/s2graph/hbase/")
            System.setProperty("hbase.log.file", s"$cwd/storage/s2graph/hbase.log")
            System.setProperty("hbase.tmp.dir", s"$cwd/storage/s2graph/hbase/")
            System.setProperty("hbase.home.dir", "")
            System.setProperty("hbase.id.str", "s2graph")
            System.setProperty("hbase.root.logger", "INFO,RFA")

            org.apache.hadoop.hbase.master.HMaster.main(Array[String]("start"))
            logger.info(s"HMaster startup finished: ${System.currentTimeMillis() - ts}")
          }
        })
      }

      executor
    }

    hbaseExecutor
  }

  def fetchKeyValues(client: HBaseClient, rpc: AsyncRPC)(implicit ec: ExecutionContext): Future[Seq[SKeyValue]] = {
    val defer = fetchKeyValuesInner(client, rpc)
    defer.toFuture(emptyKeyValues).map { kvsArr =>
      kvsArr.map { kv =>
        implicitly[CanSKeyValue[KeyValue]].toSKeyValue(kv)
      }
    }
  }

  def fetchKeyValuesInner(client: HBaseClient, rpc: AsyncRPC)(implicit ec: ExecutionContext): Deferred[util.ArrayList[KeyValue]] = {
    rpc match {
      case Left(get) => client.get(get)
      case Right(ScanWithRange(scanner, offset, limit)) =>
        val fallbackFn: (Exception => util.ArrayList[KeyValue]) = { ex =>
          logger.error(s"fetchKeyValuesInner failed.", ex)
          scanner.close()
          emptyKeyValues
        }

        scanner.nextRows().mapWithFallback(new util.ArrayList[util.ArrayList[KeyValue]]())(fallbackFn) { kvsLs =>
          val ls = new util.ArrayList[KeyValue]
          if (kvsLs == null) {
          } else {
            kvsLs.foreach { kvs =>
              if (kvs != null) kvs.foreach { kv => ls.add(kv) }
              else {

              }
            }
          }

          scanner.close()
          val toIndex = Math.min(ls.size(), offset + limit)
          new util.ArrayList[KeyValue](ls.subList(offset, toIndex))
        }
      case _ => Deferred.fromError(new RuntimeException(s"fetchKeyValues failed. $rpc"))
    }
  }

  def toCacheKeyBytes(hbaseRpc: AsyncRPC): Array[Byte] = {
    /* with version 4, request's type is (Scanner, (Int, Int)). otherwise GetRequest. */
    hbaseRpc match {
      case Left(getRequest) => getRequest.key
      case Right(ScanWithRange(scanner, offset, limit)) =>
        Bytes.add(scanner.getCurrentKey, Bytes.add(Bytes.toBytes(offset), Bytes.toBytes(limit)))
      case _ =>
        logger.error(s"toCacheKeyBytes failed. not supported class type. $hbaseRpc")
        throw new RuntimeException(s"toCacheKeyBytes: $hbaseRpc")
    }
  }

  def buildRequest(serDe: StorageSerDe, vertex: S2VertexLike) = {
    val kvs = serDe.vertexSerializer(vertex).toKeyValues
    val get = new GetRequest(vertex.hbaseTableName.getBytes, kvs.head.row, Serializable.vertexCf)
    //      get.setTimeout(this.singleGetTimeout.toShort)
    get.setFailfast(true)
    get.maxVersions(1)

    Left(get)
  }

  def buildRequest(client: HBaseClient, serDe: StorageSerDe, queryRequest: QueryRequest, edge: S2EdgeLike) = {
    val queryParam = queryRequest.queryParam
    val label = queryParam.label

    val serializer = if (queryParam.tgtVertexInnerIdOpt.isDefined) {
      val snapshotEdge = edge.toSnapshotEdge
      serDe.snapshotEdgeSerializer(snapshotEdge)
    } else {
      val indexEdge = edge.toIndexEdge(queryParam.labelOrderSeq)
      serDe.indexEdgeSerializer(indexEdge)
    }

    val rowKey = serializer.toRowKey
    val (minTs, maxTs) = queryParam.durationOpt.getOrElse((0L, Long.MaxValue))

    val (intervalMaxBytes, intervalMinBytes) = queryParam.buildInterval(Option(edge))

    label.schemaVersion match {
      case HBaseType.VERSION4 if queryParam.tgtVertexInnerIdOpt.isEmpty =>
        val scanner = AsynchbasePatcher.newScanner(client, label.hbaseTableName)
        scanner.setFamily(SKeyValue.EdgeCf)

        /*
         * TODO: remove this part.
         */
        val indexEdgeOpt = edge.edgesWithIndex.find(edgeWithIndex => edgeWithIndex.labelIndex.seq == queryParam.labelOrderSeq)
        val indexEdge = indexEdgeOpt.getOrElse(throw new RuntimeException(s"Can`t find index for query $queryParam"))

        val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
        val labelWithDirBytes = indexEdge.labelWithDir.bytes
        val labelIndexSeqWithIsInvertedBytes = StorageSerializable.labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)
        val baseKey = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)

        val (startKey, stopKey) =
          if (queryParam.intervalOpt.isDefined) {
            // interval is set.
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Base64.getDecoder.decode(cursor)
              case None => Bytes.add(baseKey, intervalMaxBytes)
            }
            (_startKey , Bytes.add(baseKey, intervalMinBytes))
          } else {
            /*
             * note: since propsToBytes encode size of property map at first byte, we are sure about max value here
             */
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Base64.getDecoder.decode(cursor)
              case None => baseKey
            }
            (_startKey, Bytes.add(baseKey, Array.fill(1)(-1)))
          }

        scanner.setStartKey(startKey)
        scanner.setStopKey(stopKey)

        if (queryParam.limit == Int.MinValue) logger.debug(s"MinValue: $queryParam")

        scanner.setMaxVersions(1)
        // TODO: exclusive condition innerOffset with cursorOpt
        if (queryParam.cursorOpt.isDefined) {
          scanner.setMaxNumRows(queryParam.limit)
        } else {
          scanner.setMaxNumRows(queryParam.innerOffset + queryParam.innerLimit)
        }
        scanner.setMaxTimestamp(maxTs)
        scanner.setMinTimestamp(minTs)
        scanner.setRpcTimeout(queryParam.rpcTimeout)

        // SET option for this rpc properly.
        if (queryParam.cursorOpt.isDefined) Right(ScanWithRange(scanner, 0, queryParam.limit))
        else Right(ScanWithRange(scanner, 0, queryParam.innerOffset + queryParam.innerLimit))

      case _ =>
        val get = if (queryParam.tgtVertexInnerIdOpt.isDefined) {
          new GetRequest(label.hbaseTableName.getBytes, rowKey, SKeyValue.EdgeCf, serializer.toQualifier)
        } else {
          new GetRequest(label.hbaseTableName.getBytes, rowKey, SKeyValue.EdgeCf)
        }

        get.maxVersions(1)
        get.setFailfast(true)
        get.setMinTimestamp(minTs)
        get.setMaxTimestamp(maxTs)
        get.setTimeout(queryParam.rpcTimeout)

        val pagination = new ColumnPaginationFilter(queryParam.innerLimit, queryParam.innerOffset)
        val columnRangeFilterOpt = queryParam.intervalOpt.map { interval =>
          new ColumnRangeFilter(intervalMaxBytes, true, intervalMinBytes, true)
        }
        get.setFilter(new FilterList(pagination +: columnRangeFilterOpt.toSeq, MUST_PASS_ALL))
        Left(get)
    }
  }
}


class AsynchbaseStorage(override val graph: S2GraphLike,
                        override val config: Config) extends Storage(graph, config) {

  /**
    * since some runtime environment such as spark cluster has issue with guava version, that is used in Asynchbase.
    * to fix version conflict, make this as lazy val for clients that don't require hbase client.
    */
  val client = AsynchbaseStorage.makeClient(config)
  val clientWithFlush = AsynchbaseStorage.makeClient(config, "hbase.rpcs.buffered_flush_interval" -> "0")
  val clients = Seq(client, clientWithFlush)

//  private lazy val _fetcher = new AsynchbaseStorageFetcher(graph, config, client, serDe, io)

  private lazy val optimisticEdgeFetcher = new AsynchbaseOptimisticEdgeFetcher(client, serDe, io)
  private lazy val optimisticMutator = new AsynchbaseOptimisticMutator(graph, serDe, optimisticEdgeFetcher, client, clientWithFlush)

  override val management: StorageManagement = new AsynchbaseStorageManagement(config, clients)
  override val serDe: StorageSerDe = new AsynchbaseStorageSerDe(graph)

  override val edgeFetcher: EdgeFetcher = new AsynchbaseEdgeFetcher(graph, config, client, serDe, io)
  override val vertexFetcher: VertexFetcher = new AsynchbaseVertexFetcher(graph, config, client, serDe, io)

  override val edgeMutator: EdgeMutator = new DefaultOptimisticEdgeMutator(graph, serDe, optimisticEdgeFetcher, optimisticMutator, io)
  override val vertexMutator: VertexMutator = new DefaultOptimisticVertexMutator(graph, serDe, optimisticEdgeFetcher, optimisticMutator, io)

}
