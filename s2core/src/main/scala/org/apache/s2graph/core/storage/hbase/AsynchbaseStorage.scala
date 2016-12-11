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

import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Durability}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, LabelMeta, ServiceColumn}
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorage.{AsyncRPC, ScanWithRange}
import org.apache.s2graph.core.types.{HBaseType, VertexId}
import org.apache.s2graph.core.utils._
import org.hbase.async.FilterList.Operator.MUST_PASS_ALL
import org.hbase.async._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try
import scala.util.hashing.MurmurHash3


object AsynchbaseStorage {
  val vertexCf = Serializable.vertexCf
  val edgeCf = Serializable.edgeCf
  val emptyKVs = new util.ArrayList[KeyValue]()

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

  case class ScanWithRange(scan: Scanner, offset: Int, limit: Int)
  type AsyncRPC = Either[GetRequest, ScanWithRange]
}


class AsynchbaseStorage(override val graph: S2Graph,
                        override val config: Config)(implicit ec: ExecutionContext)
  extends Storage[AsyncRPC, Deferred[StepResult]](graph, config) {

  import Extensions.DeferOps

  /**
   * Asynchbase client setup.
   * note that we need two client, one for bulk(withWait=false) and another for withWait=true
   */
  private val clientFlushInterval = config.getInt("hbase.rpcs.buffered_flush_interval").toString().toShort

  /**
   * since some runtime environment such as spark cluster has issue with guava version, that is used in Asynchbase.
   * to fix version conflict, make this as lazy val for clients that don't require hbase client.
   */
  lazy val client = AsynchbaseStorage.makeClient(config)
  lazy val clientWithFlush = AsynchbaseStorage.makeClient(config, "hbase.rpcs.buffered_flush_interval" -> "0")
  lazy val clients = Seq(client, clientWithFlush)

  private val emptyKeyValues = new util.ArrayList[KeyValue]()
  private val emptyKeyValuesLs = new util.ArrayList[util.ArrayList[KeyValue]]()
  private val emptyStepResult = new util.ArrayList[StepResult]()

  private def client(withWait: Boolean): HBaseClient = if (withWait) clientWithFlush else client

  import CanDefer._

  /** Future Cache to squash request */
  lazy private val futureCache = new DeferCache[StepResult, Deferred, Deferred](config, StepResult.Empty, "AsyncHbaseFutureCache", useMetric = true)

  /** Simple Vertex Cache */
  lazy private val vertexCache = new DeferCache[Seq[SKeyValue], Promise, Future](config, Seq.empty[SKeyValue])

  private val zkQuorum = config.getString("hbase.zookeeper.quorum")
  private val zkQuorumSlave =
    if (config.hasPath("hbase.slave.zookeeper.quorum")) Option(config.getString("hbase.slave.zookeeper.quorum"))
    else None

  /** v4 max next row size */
  private val v4_max_num_rows = 10000
  private def getV4MaxNumRows(limit : Int): Int = {
    if (limit < v4_max_num_rows) limit
    else v4_max_num_rows
  }

  /**
   * fire rpcs into proper hbase cluster using client and
   * return true on all mutation success. otherwise return false.
   */
  override def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
    if (kvs.isEmpty) Future.successful(true)
    else {
      val _client = client(withWait)
      val (increments, putAndDeletes) = kvs.partition(_.operation == SKeyValue.Increment)

      /** Asynchbase IncrementRequest does not implement HasQualifiers */
      val incrementsFutures = increments.map { kv =>
        val inc = new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value))
        val defer = _client.atomicIncrement(inc)
        val future = defer.toFuture(Long.box(0)).map(_ => true).recover { case ex: Exception =>
          logger.error(s"mutation failed. $kv", ex)
          false
        }
        if (withWait) future else Future.successful(true)
      }

      /** PutRequest and DeleteRequest accept byte[][] qualifiers/values. */
      val othersFutures = putAndDeletes.groupBy { kv =>
        (kv.table.toSeq, kv.row.toSeq, kv.cf.toSeq, kv.operation, kv.timestamp)
      }.map { case ((table, row, cf, operation, timestamp), groupedKeyValues) =>

        val durability = groupedKeyValues.head.durability
        val qualifiers = new ArrayBuffer[Array[Byte]]()
        val values = new ArrayBuffer[Array[Byte]]()

        groupedKeyValues.foreach { kv =>
          if (kv.qualifier != null) qualifiers += kv.qualifier
          if (kv.value != null) values += kv.value
        }
        val defer = operation match {
          case SKeyValue.Put =>
            val put = new PutRequest(table.toArray, row.toArray, cf.toArray, qualifiers.toArray, values.toArray, timestamp)
            put.setDurable(durability)
            _client.put(put)
          case SKeyValue.Delete =>
            val delete =
              if (qualifiers.isEmpty)
                new DeleteRequest(table.toArray, row.toArray, cf.toArray, timestamp)
              else
                new DeleteRequest(table.toArray, row.toArray, cf.toArray, qualifiers.toArray, timestamp)
            delete.setDurable(durability)
            _client.delete(delete)
        }
        if (withWait) {
          defer.toFuture(new AnyRef()).map(_ => true).recover { case ex: Exception =>
            groupedKeyValues.foreach { kv => logger.error(s"mutation failed. $kv", ex) }
            false
          }
        } else Future.successful(true)
      }
      for {
        incrementRets <- Future.sequence(incrementsFutures)
        otherRets <- Future.sequence(othersFutures)
      } yield (incrementRets ++ otherRets).forall(identity)
    }
  }

  private def fetchKeyValues(rpc: AsyncRPC): Future[Seq[SKeyValue]] = {
    val defer = fetchKeyValuesInner(rpc)
    defer.toFuture(emptyKeyValues).map { kvsArr =>
      kvsArr.map { kv =>
        implicitly[CanSKeyValue[KeyValue]].toSKeyValue(kv)
      }
    }
  }

  override def fetchSnapshotEdgeKeyValues(queryRequest: QueryRequest): Future[Seq[SKeyValue]] = {
    val edge = toRequestEdge(queryRequest, Nil)
    val rpc = buildRequest(queryRequest, edge)
    fetchKeyValues(rpc)
  }

  /**
   * since HBase natively provide CheckAndSet on storage level, implementation becomes simple.
   * @param rpc: key value that is need to be stored on storage.
   * @param expectedOpt: last valid value for rpc's KeyValue.value from fetching.
   * @return return true if expected value matches and our rpc is successfully applied, otherwise false.
   *         note that when some other thread modified same cell and have different value on this KeyValue,
   *         then HBase atomically return false.
   */
  override def writeLock(rpc: SKeyValue, expectedOpt: Option[SKeyValue]): Future[Boolean] = {
    val put = new PutRequest(rpc.table, rpc.row, rpc.cf, rpc.qualifier, rpc.value, rpc.timestamp)
    val expected = expectedOpt.map(_.value).getOrElse(Array.empty)
    client(withWait = true).compareAndSet(put, expected).map(true.booleanValue())(ret => ret.booleanValue()).toFuture(true)
  }


  /**
   * given queryRequest, build storage specific RPC Request.
   * In HBase case, we either build Scanner or GetRequest.
   *
   * IndexEdge layer:
   *    Tall schema(v4): use scanner.
   *    Wide schema(label's schema version in v1, v2, v3): use GetRequest with columnRangeFilter
   *                                                       when query is given with itnerval option.
   * SnapshotEdge layer:
   *    Tall schema(v3, v4): use GetRequest without column filter.
   *    Wide schema(label's schema version in v1, v2): use GetRequest with columnRangeFilter.
   * Vertex layer:
   *    all version: use GetRequest without column filter.
   * @param queryRequest
   * @return Scanner or GetRequest with proper setup with StartKey, EndKey, RangeFilter.
   */
  override def buildRequest(queryRequest: QueryRequest, edge: S2Edge): AsyncRPC = {
    import Serializable._
    val queryParam = queryRequest.queryParam
    val label = queryParam.label

    val serializer = if (queryParam.tgtVertexInnerIdOpt.isDefined) {
      val snapshotEdge = edge.toSnapshotEdge
      snapshotEdgeSerializer(snapshotEdge)
    } else {
      val indexEdge = edge.toIndexEdge(queryParam.labelOrderSeq)
      indexEdgeSerializer(indexEdge)
    }

    val rowKey = serializer.toRowKey
    val (minTs, maxTs) = queryParam.durationOpt.getOrElse((0L, Long.MaxValue))

    val (intervalMaxBytes, intervalMinBytes) = queryParam.buildInterval(Option(edge))

    label.schemaVersion match {
      case HBaseType.VERSION4 if queryParam.tgtVertexInnerIdOpt.isEmpty =>
        val scanner = AsynchbasePatcher.newScanner(client, label.hbaseTableName)
        scanner.setFamily(edgeCf)

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
            /**
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
          scanner.setMaxNumRows(getV4MaxNumRows(queryParam.limit))
        } else {
          scanner.setMaxNumRows(getV4MaxNumRows(queryParam.innerOffset + queryParam.innerLimit))
        }
        scanner.setMaxTimestamp(maxTs)
        scanner.setMinTimestamp(minTs)
        scanner.setRpcTimeout(queryParam.rpcTimeout)

        // SET option for this rpc properly.
        if (queryParam.cursorOpt.isDefined) Right(ScanWithRange(scanner, 0, queryParam.limit))
        else Right(ScanWithRange(scanner, 0, queryParam.innerOffset + queryParam.innerLimit))

      case _ =>
        val get = if (queryParam.tgtVertexInnerIdOpt.isDefined) {
          new GetRequest(label.hbaseTableName.getBytes, rowKey, edgeCf, serializer.toQualifier)
        } else {
          new GetRequest(label.hbaseTableName.getBytes, rowKey, edgeCf)
        }

        get.maxVersions(1)
        get.setFailfast(true)
        get.setMinTimestamp(minTs)
        get.setMaxTimestamp(maxTs)
        get.setTimeout(queryParam.rpcTimeout)

        val pagination = new ColumnPaginationFilter(queryParam.limit, queryParam.offset)
        val columnRangeFilterOpt = queryParam.intervalOpt.map { interval =>
          new ColumnRangeFilter(intervalMaxBytes, true, intervalMinBytes, true)
        }
        get.setFilter(new FilterList(pagination +: columnRangeFilterOpt.toSeq, MUST_PASS_ALL))
        Left(get)
    }
  }

  /**
   * we are using future cache to squash requests into same key on storage.
   *
   * @param queryRequest
   * @param isInnerCall
   * @param parentEdges
   * @return we use Deferred here since it has much better performrance compared to scala.concurrent.Future.
   *         seems like map, flatMap on scala.concurrent.Future is slower than Deferred's addCallback
   */
  override def fetch(queryRequest: QueryRequest,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Deferred[StepResult] = {

    def fetchInner(hbaseRpc: AsyncRPC): Deferred[StepResult] = {
      val prevStepScore = queryRequest.prevStepScore
      val fallbackFn: (Exception => StepResult) = { ex =>
        logger.error(s"fetchInner failed. fallback return. $hbaseRpc}", ex)
        StepResult.Failure
      }

      val queryParam = queryRequest.queryParam
      fetchKeyValuesInner(hbaseRpc).mapWithFallback(emptyKeyValues)(fallbackFn) { kvs =>
        val (startOffset, len) = queryParam.label.schemaVersion match {
          case HBaseType.VERSION4 =>
            val offset = if (queryParam.cursorOpt.isDefined) 0 else queryParam.offset
            (offset, queryParam.limit)
          case _ => (0, kvs.length)
        }

        toEdges(kvs, queryRequest, prevStepScore, isInnerCall, parentEdges, startOffset, len)
      }
    }

    val queryParam = queryRequest.queryParam
    val cacheTTL = queryParam.cacheTTLInMillis
    /** with version 4, request's type is (Scanner, (Int, Int)). otherwise GetRequest. */

    val edge = toRequestEdge(queryRequest, parentEdges)
    val request = buildRequest(queryRequest, edge)
    val (intervalMaxBytes, intervalMinBytes) = queryParam.buildInterval(Option(edge))
    val requestCacheKey = Bytes.add(toCacheKeyBytes(request), intervalMaxBytes, intervalMinBytes)

    if (cacheTTL <= 0) fetchInner(request)
    else {
      val cacheKeyBytes = Bytes.add(queryRequest.query.queryOption.cacheKeyBytes, requestCacheKey)

//      val cacheKeyBytes = toCacheKeyBytes(request)
      val cacheKey = queryParam.toCacheKey(cacheKeyBytes)
      futureCache.getOrElseUpdate(cacheKey, cacheTTL)(fetchInner(request))
    }
  }

  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[StepResult]] = {
    val defers: Seq[Deferred[StepResult]] = for {
      queryRequest <- queryRequests
    } yield {
        val queryOption = queryRequest.query.queryOption
        val queryParam = queryRequest.queryParam
        val shouldBuildParents = queryOption.returnTree || queryParam.whereHasParent
        val parentEdges = if (shouldBuildParents) prevStepEdges.getOrElse(queryRequest.vertex.id, Nil) else Nil
        fetch(queryRequest, isInnerCall = false, parentEdges)
      }

    val grouped: Deferred[util.ArrayList[StepResult]] = Deferred.groupInOrder(defers)
    grouped.map(emptyStepResult) { queryResults: util.ArrayList[StepResult] =>
      queryResults.toSeq
    }.toFuture(emptyStepResult)
  }


  def fetchVertexKeyValues(request: QueryRequest): Future[Seq[SKeyValue]] = {
    val edge = toRequestEdge(request, Nil)
    fetchKeyValues(buildRequest(request, edge))
  }


  def fetchVertexKeyValues(request: AsyncRPC): Future[Seq[SKeyValue]] = fetchKeyValues(request)

  /**
   * when withWait is given, we use client with flushInterval set to 0.
   * if we are not using this, then we are adding extra wait time as much as flushInterval in worst case.
   *
   * @param edges
   * @param withWait
   * @return
   */
  override def incrementCounts(edges: Seq[S2Edge], withWait: Boolean): Future[Seq[(Boolean, Long, Long)]] = {

    val _client = client(withWait)
    val defers: Seq[Deferred[(Boolean, Long, Long)]] = for {
      edge <- edges
    } yield {
        val futures: List[Deferred[(Boolean, Long, Long)]] = for {
          relEdge <- edge.relatedEdges
          edgeWithIndex <- relEdge.edgesWithIndexValid
        } yield {
          val countWithTs = edge.propertyValueInner(LabelMeta.count)
          val countVal = countWithTs.innerVal.toString().toLong
          val kv = buildIncrementsCountAsync(edgeWithIndex, countVal).head
          val request = new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value))
          val fallbackFn: (Exception => (Boolean, Long, Long)) = { ex =>
            logger.error(s"mutation failed. $request", ex)
            (false, -1L, -1L)
          }
          val defer = _client.bufferAtomicIncrement(request).mapWithFallback(0L)(fallbackFn) { resultCount: java.lang.Long =>
            (true, resultCount.longValue(), countVal)
          }
          if (withWait) defer
          else Deferred.fromResult((true, -1L, -1L))
        }

        val grouped: Deferred[util.ArrayList[(Boolean, Long, Long)]] = Deferred.group(futures)
        grouped.map(new util.ArrayList[(Boolean, Long, Long)]()) { resultLs => resultLs.head }
      }

    val grouped: Deferred[util.ArrayList[(Boolean, Long, Long)]] = Deferred.groupInOrder(defers)
    grouped.toFuture(new util.ArrayList[(Boolean, Long, Long)]()).map(_.toSeq)
  }


  override def flush(): Unit = clients.foreach { client =>
    super.flush()
    val timeout = Duration((clientFlushInterval + 10) * 20, duration.MILLISECONDS)
    Await.result(client.flush().toFuture(new AnyRef), timeout)
  }


  override def createTable(_zkAddr: String,
                           tableName: String,
                           cfs: List[String],
                           regionMultiplier: Int,
                           ttl: Option[Int],
                           compressionAlgorithm: String,
                           replicationScopeOpt: Option[Int] = None,
                           totalRegionCount: Option[Int] = None): Unit = {
    /** TODO: Decide if we will allow each app server to connect to multiple hbase cluster */
    for {
      zkAddr <- Seq(zkQuorum) ++ zkQuorumSlave.toSeq
    } {
      logger.info(s"create table: $tableName on $zkAddr, $cfs, $regionMultiplier, $compressionAlgorithm")
      val admin = getAdmin(zkAddr)
      val regionCount = totalRegionCount.getOrElse(admin.getClusterStatus.getServersSize * regionMultiplier)
      try {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
          val desc = new HTableDescriptor(TableName.valueOf(tableName))
          desc.setDurability(Durability.ASYNC_WAL)
          for (cf <- cfs) {
            val columnDesc = new HColumnDescriptor(cf)
              .setCompressionType(Algorithm.valueOf(compressionAlgorithm.toUpperCase))
              .setBloomFilterType(BloomType.ROW)
              .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
              .setMaxVersions(1)
              .setTimeToLive(2147483647)
              .setMinVersions(0)
              .setBlocksize(32768)
              .setBlockCacheEnabled(true)
            if (ttl.isDefined) columnDesc.setTimeToLive(ttl.get)
            if (replicationScopeOpt.isDefined) columnDesc.setScope(replicationScopeOpt.get)
            desc.addFamily(columnDesc)
          }

          if (regionCount <= 1) admin.createTable(desc)
          else admin.createTable(desc, getStartKey(regionCount), getEndKey(regionCount), regionCount)
        } else {
          logger.info(s"$zkAddr, $tableName, $cfs already exist.")
        }
      } catch {
        case e: Throwable =>
          logger.error(s"$zkAddr, $tableName failed with $e", e)
          throw e
      } finally {
        admin.close()
        admin.getConnection.close()
      }
    }
  }


  /** Asynchbase implementation override default getVertices to use future Cache */
  override def getVertices(vertices: Seq[S2Vertex]): Future[Seq[S2Vertex]] = {
    def fromResult(kvs: Seq[SKeyValue],
                   version: String): Option[S2Vertex] = {

      if (kvs.isEmpty) None
      else vertexDeserializer.fromKeyValues(kvs, None)
//        .map(S2Vertex(graph, _))
    }

    val futures = vertices.map { vertex =>
      val kvs = vertexSerializer(vertex).toKeyValues
      val get = new GetRequest(vertex.hbaseTableName.getBytes, kvs.head.row, Serializable.vertexCf)
      //      get.setTimeout(this.singleGetTimeout.toShort)
      get.setFailfast(true)
      get.maxVersions(1)

      val cacheKey = MurmurHash3.stringHash(get.toString)
      vertexCache.getOrElseUpdate(cacheKey, cacheTTL = 10000)(fetchVertexKeyValues(Left(get))).map { kvs =>
        fromResult(kvs, vertex.serviceColumn.schemaVersion)
      }
    }

    Future.sequence(futures).map { result => result.toList.flatten }
  }

  override def fetchEdgesAll(): Future[Seq[S2Edge]] = {
    val futures = Label.findAll().groupBy(_.hbaseTableName).toSeq.map { case (hTableName, labels) =>
      val scan = AsynchbasePatcher.newScanner(client, hTableName)
      scan.setFamily(Serializable.edgeCf)
      scan.setMaxVersions(1)

      scan.nextRows(10000).toFuture(emptyKeyValuesLs).map { kvsLs =>
        kvsLs.flatMap { kvs =>
          kvs.flatMap { kv =>
            indexEdgeDeserializer.fromKeyValues(Seq(kv), None)
          }
        }
      }
    }

    Future.sequence(futures).map(_.flatten)
  }

  override def fetchVerticesAll(): Future[Seq[S2Vertex]] = {
    val futures = ServiceColumn.findAll().groupBy(_.service.hTableName).toSeq.map { case (hTableName, columns) =>
      val scan = AsynchbasePatcher.newScanner(client, hTableName)
      scan.setFamily(Serializable.vertexCf)
      scan.setMaxVersions(1)

      scan.nextRows(10000).toFuture(emptyKeyValuesLs).map { kvsLs =>
        kvsLs.flatMap { kvs =>
          vertexDeserializer.fromKeyValues(kvs, None)
        }
      }
    }
    Future.sequence(futures).map(_.flatten)
  }

  class V4ResultHandler(scanner: Scanner, defer: Deferred[util.ArrayList[KeyValue]], offset: Int, limit : Int) extends Callback[Object, util.ArrayList[util.ArrayList[KeyValue]]] {
    val results = new util.ArrayList[KeyValue]()
    var offsetCount = 0

    override def call(kvsLs: util.ArrayList[util.ArrayList[KeyValue]]): Object = {
      try {
        if (kvsLs == null) {
          defer.callback(results)
          Try(scanner.close())
        } else {
          val curRet = new util.ArrayList[KeyValue]()
          kvsLs.foreach(curRet.addAll(_))
          val prevOffset = offsetCount
          offsetCount += curRet.size()

          val nextRet = if(offsetCount > offset){
            if(prevOffset < offset ) {
              curRet.subList(offset - prevOffset, curRet.size())
            } else{
              curRet
            }
          } else{
            emptyKeyValues
          }

          val needCount = limit - results.size()
          if (needCount >= nextRet.size()) {
            results.addAll(nextRet)
          } else {
            results.addAll(nextRet.subList(0, needCount))
          }

          if (results.size() < limit) {
            scanner.nextRows().addCallback(this)
          } else {
            defer.callback(results)
            Try(scanner.close())
          }
        }
      } catch{
        case ex: Exception =>
          logger.error(s"fetchKeyValuesInner failed.", ex)
          defer.callback(ex)
          Try(scanner.close())
      }
    }
  }

  /**
   * Private Methods which is specific to Asynchbase implementation.
   */
  private def fetchKeyValuesInner(rpc: AsyncRPC): Deferred[util.ArrayList[KeyValue]] = {
    rpc match {
      case Left(get) => client.get(get)
      case Right(ScanWithRange(scanner, offset, limit)) =>
        val deferred = new Deferred[util.ArrayList[KeyValue]]()
        scanner.nextRows().addCallback(new V4ResultHandler(scanner, deferred, offset, limit))
        deferred
      case _ => Deferred.fromError(new RuntimeException(s"fetchKeyValues failed. $rpc"))
    }
  }

  private def toCacheKeyBytes(hbaseRpc: AsyncRPC): Array[Byte] = {
    /** with version 4, request's type is (Scanner, (Int, Int)). otherwise GetRequest. */
    hbaseRpc match {
      case Left(getRequest) => getRequest.key
      case Right(ScanWithRange(scanner, offset, limit)) =>
        Bytes.add(scanner.getCurrentKey, Bytes.add(Bytes.toBytes(offset), Bytes.toBytes(limit)))
      case _ =>
        logger.error(s"toCacheKeyBytes failed. not supported class type. $hbaseRpc")
        throw new RuntimeException(s"toCacheKeyBytes: $hbaseRpc")
    }
  }

  private def getSecureClusterAdmin(zkAddr: String) = {
    val jaas = config.getString("java.security.auth.login.config")
    val krb5Conf = config.getString("java.security.krb5.conf")
    val realm = config.getString("realm")
    val principal = config.getString("principal")
    val keytab = config.getString("keytab")

    System.setProperty("java.security.auth.login.config", jaas)
    System.setProperty("java.security.krb5.conf", krb5Conf)
    // System.setProperty("sun.security.krb5.debug", "true")
    // System.setProperty("sun.security.spnego.debug", "true")
    val conf = new Configuration(true)
    val hConf = HBaseConfiguration.create(conf)

    hConf.set("hbase.zookeeper.quorum", zkAddr)

    hConf.set("hadoop.security.authentication", "Kerberos")
    hConf.set("hbase.security.authentication", "Kerberos")
    hConf.set("hbase.master.kerberos.principal", "hbase/_HOST@" + realm)
    hConf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@" + realm)

    System.out.println("Connecting secure cluster, using keytab\n")
    UserGroupInformation.setConfiguration(hConf)
    UserGroupInformation.loginUserFromKeytab(principal, keytab)
    val currentUser = UserGroupInformation.getCurrentUser()
    System.out.println("current user : " + currentUser + "\n")

    // get table list
    val conn = ConnectionFactory.createConnection(hConf)
    conn.getAdmin
  }

  /**
   * following configuration need to come together to use secured hbase cluster.
   * 1. set hbase.security.auth.enable = true
   * 2. set file path to jaas file java.security.auth.login.config
   * 3. set file path to kerberos file java.security.krb5.conf
   * 4. set realm
   * 5. set principal
   * 6. set file path to keytab
   * @param zkAddr
   * @return
   */
  private def getAdmin(zkAddr: String) = {
    if (config.hasPath("hbase.security.auth.enable") && config.getBoolean("hbase.security.auth.enable")) {
      getSecureClusterAdmin(zkAddr)
    } else {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", zkAddr)
      val conn = ConnectionFactory.createConnection(conf)
      conn.getAdmin
    }
  }

  private def enableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).enableTable(TableName.valueOf(tableName))
  }

  private def disableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).disableTable(TableName.valueOf(tableName))
  }

  private def dropTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).disableTable(TableName.valueOf(tableName))
    getAdmin(zkAddr).deleteTable(TableName.valueOf(tableName))
  }

  private def getStartKey(regionCount: Int): Array[Byte] = {
    Bytes.toBytes((Int.MaxValue / regionCount))
  }

  private def getEndKey(regionCount: Int): Array[Byte] = {
    Bytes.toBytes((Int.MaxValue / regionCount * (regionCount - 1)))
  }
}
