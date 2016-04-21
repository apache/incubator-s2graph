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

import com.stumbleupon.async.Deferred
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Durability}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.types.{HBaseType, VertexId}
import org.apache.s2graph.core.utils._
import org.hbase.async.FilterList.Operator.MUST_PASS_ALL
import org.hbase.async._

import scala.collection.JavaConversions._
import scala.collection.{Map, Seq}
import scala.concurrent.duration.Duration
import scala.concurrent._
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
}


class AsynchbaseStorage(override val config: Config)(implicit ec: ExecutionContext)
  extends Storage[Deferred[QueryRequestWithResult]](config) {

  import Extensions.DeferOps

  /**
   * Asynchbase client setup.
   * note that we need two client, one for bulk(withWait=false) and another for withWait=true
   */
  val configWithFlush = config.withFallback(ConfigFactory.parseMap(Map("hbase.rpcs.buffered_flush_interval" -> "0")))
  val client = AsynchbaseStorage.makeClient(config)

  private val clientWithFlush = AsynchbaseStorage.makeClient(config, "hbase.rpcs.buffered_flush_interval" -> "0")
  private val clients = Seq(client, clientWithFlush)
  private val clientFlushInterval = config.getInt("hbase.rpcs.buffered_flush_interval").toString().toShort
  private val emptyKeyValues = new util.ArrayList[KeyValue]()
  private def client(withWait: Boolean): HBaseClient = if (withWait) clientWithFlush else client

  import CanDefer._

  /** Future Cache to squash request */
  private val futureCache = new DeferCache[QueryResult, Deferred, Deferred](config, QueryResult(), "FutureCache", useMetric = true)

  /** Simple Vertex Cache */
  private val vertexCache = new DeferCache[Seq[SKeyValue], Promise, Future](config, Seq.empty[SKeyValue])


  /**
   * fire rpcs into proper hbase cluster using client and
   * return true on all mutation success. otherwise return false.
   */
  override def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
    if (kvs.isEmpty) Future.successful(true)
    else {
      val _client = client(withWait)
      val futures = kvs.map { kv =>
        val _defer = kv.operation match {
          case SKeyValue.Put => _client.put(new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp))
          case SKeyValue.Delete =>
            if (kv.qualifier == null) _client.delete(new DeleteRequest(kv.table, kv.row, kv.cf, kv.timestamp))
            else _client.delete(new DeleteRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.timestamp))
          case SKeyValue.Increment =>
            _client.atomicIncrement(new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value)))
        }
        val future = _defer.withCallback { ret => true }.recoverWith { ex =>
          logger.error(s"mutation failed. $kv", ex)
          false
        }.toFuture

        if (withWait) future else Future.successful(true)
      }

      Future.sequence(futures).map(_.forall(identity))
    }
  }


  override def fetchSnapshotEdgeKeyValues(hbaseRpc: AnyRef): Future[Seq[SKeyValue]] = {
    val defer = fetchKeyValuesInner(hbaseRpc)
    defer.toFuture.map { kvsArr =>
      kvsArr.map { kv =>
        implicitly[CanSKeyValue[KeyValue]].toSKeyValue(kv)
      } toSeq
    }
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
    client(withWait = true).compareAndSet(put, expected).withCallback(ret => ret.booleanValue()).toFuture
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
  override def buildRequest(queryRequest: QueryRequest): AnyRef = {
    import Serializable._
    val queryParam = queryRequest.queryParam
    val label = queryParam.label
    val edge = toRequestEdge(queryRequest)

    val serializer = if (queryParam.tgtVertexInnerIdOpt.isDefined) {
      val snapshotEdge = edge.toSnapshotEdge
      snapshotEdgeSerializer(snapshotEdge)
    } else {
      val indexEdge = IndexEdge(edge.srcVertex, edge.tgtVertex, edge.labelWithDir,
        edge.op, edge.version, queryParam.labelOrderSeq, edge.propsWithTs)
      indexEdgeSerializer(indexEdge)
    }

    val (rowKey, qualifier) = (serializer.toRowKey, serializer.toQualifier)

    val (minTs, maxTs) = queryParam.duration.getOrElse((0L, Long.MaxValue))

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
        //        val labelIndexSeqWithIsInvertedStopBytes =  StorageSerializable.labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = true)
        val baseKey = Bytes.add(srcIdBytes, labelWithDirBytes, Bytes.add(labelIndexSeqWithIsInvertedBytes, Array.fill(1)(edge.op)))
        val (startKey, stopKey) =
          if (queryParam.columnRangeFilter != null) {
            // interval is set.
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Bytes.add(Base64.getDecoder.decode(cursor), Array.fill(1)(0))
              case None => Bytes.add(baseKey, queryParam.columnRangeFilterMinBytes)
            }
            (_startKey, Bytes.add(baseKey, queryParam.columnRangeFilterMaxBytes))
          } else {
            /*
             * note: since propsToBytes encode size of property map at first byte, we are sure about max value here
             */
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Bytes.add(Base64.getDecoder.decode(cursor), Array.fill(1)(0))
              case None => baseKey
            }
            (_startKey, Bytes.add(baseKey, Array.fill(1)(-1)))
          }
//                logger.debug(s"[StartKey]: ${startKey.toList}")
//                logger.debug(s"[StopKey]: ${stopKey.toList}")

        scanner.setStartKey(startKey)
        scanner.setStopKey(stopKey)

        if (queryParam.limit == Int.MinValue) logger.debug(s"MinValue: $queryParam")

        scanner.setMaxVersions(1)
        scanner.setMaxNumRows(queryParam.limit)
        scanner.setMaxTimestamp(maxTs)
        scanner.setMinTimestamp(minTs)
        scanner.setRpcTimeout(queryParam.rpcTimeoutInMillis)
        // SET option for this rpc properly.
        scanner
      case _ =>
        val get = if (queryParam.tgtVertexInnerIdOpt.isDefined) {
          new GetRequest(label.hbaseTableName.getBytes, rowKey, edgeCf, qualifier)
        } else {
          new GetRequest(label.hbaseTableName.getBytes, rowKey, edgeCf)
        }

        get.maxVersions(1)
        get.setFailfast(true)
        get.setMinTimestamp(minTs)
        get.setMaxTimestamp(maxTs)
        get.setTimeout(queryParam.rpcTimeoutInMillis)

        val pagination = new ColumnPaginationFilter(queryParam.limit, queryParam.offset)
        get.setFilter(new FilterList(pagination +: Option(queryParam.columnRangeFilter).toSeq, MUST_PASS_ALL))

        get
    }
  }

  /**
   * we are using future cache to squash requests into same key on storage.
   *
   * @param queryRequest
   * @param prevStepScore
   * @param isInnerCall
   * @param parentEdges
   * @return we use Deferred here since it has much better performrance compared to scala.concurrent.Future.
   *         seems like map, flatMap on scala.concurrent.Future is slower than Deferred's addCallback
   */
  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Deferred[QueryRequestWithResult] = {

    def fetchInner(hbaseRpc: AnyRef): Deferred[QueryResult] = {
      fetchKeyValuesInner(hbaseRpc).withCallback { kvs =>
        val edgeWithScores = toEdges(kvs, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
        val resultEdgesWithScores = if (queryRequest.queryParam.sample >= 0) {
          sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
        } else edgeWithScores
        QueryResult(resultEdgesWithScores, tailCursor = kvs.lastOption.map(_.key).getOrElse(Array.empty[Byte]))
//        QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores, tailCursor = kvs.lastOption.map(_.key).getOrElse(Array.empty)))

      } recoverWith { ex =>
        logger.error(s"fetchInner failed. fallback return. $hbaseRpc}", ex)
        QueryResult(isFailure = true)
//        QueryRequestWithResult(queryRequest, QueryResult(isFailure = true))
      }
    }

    val queryParam = queryRequest.queryParam
    val cacheTTL = queryParam.cacheTTLInMillis
    val request = buildRequest(queryRequest)

    val defer =
      if (cacheTTL <= 0) fetchInner(request)
      else {
        val cacheKeyBytes = Bytes.add(queryRequest.query.cacheKeyBytes, toCacheKeyBytes(request))
        val cacheKey = queryParam.toCacheKey(cacheKeyBytes)
        futureCache.getOrElseUpdate(cacheKey, cacheTTL)(fetchInner(request))
    }
    defer withCallback { queryResult => QueryRequestWithResult(queryRequest, queryResult)}
  }


  override def fetches(queryRequestWithScoreLs: scala.Seq[(QueryRequest, Double)],
                       prevStepEdges: Predef.Map[VertexId, scala.Seq[EdgeWithScore]]): Future[scala.Seq[QueryRequestWithResult]] = {
    val defers: Seq[Deferred[QueryRequestWithResult]] = for {
      (queryRequest, prevStepScore) <- queryRequestWithScoreLs
      parentEdges <- prevStepEdges.get(queryRequest.vertex.id)
    } yield fetch(queryRequest, prevStepScore, isInnerCall = false, parentEdges)

    val grouped: Deferred[util.ArrayList[QueryRequestWithResult]] = Deferred.group(defers)
    grouped withCallback {
      queryResults: util.ArrayList[QueryRequestWithResult] =>
        queryResults.toIndexedSeq
    } toFuture
  }


  def fetchVertexKeyValues(request: AnyRef): Future[Seq[SKeyValue]] = fetchSnapshotEdgeKeyValues(request)


  /**
   * when withWait is given, we use client with flushInterval set to 0.
   * if we are not using this, then we are adding extra wait time as much as flushInterval in worst case.
   *
   * @param edges
   * @param withWait
   * @return
   */
  override def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = {
    val _client = client(withWait)
    val defers: Seq[Deferred[(Boolean, Long)]] = for {
      edge <- edges
    } yield {
        val edgeWithIndex = edge.edgesWithIndex.head
        val countWithTs = edge.propsWithTs(LabelMeta.countSeq)
        val countVal = countWithTs.innerVal.toString().toLong
        val kv = buildIncrementsCountAsync(edgeWithIndex, countVal).head
        val request = new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value))
        val defer = _client.bufferAtomicIncrement(request) withCallback { resultCount: java.lang.Long =>
          (true, resultCount.longValue())
        } recoverWith { ex =>
          logger.error(s"mutation failed. $request", ex)
          (false, -1L)
        }
        if (withWait) defer
        else Deferred.fromResult((true, -1L))
      }

    val grouped: Deferred[util.ArrayList[(Boolean, Long)]] = Deferred.groupInOrder(defers)
    grouped.toFuture.map(_.toSeq)
  }


  override def flush(): Unit = clients.foreach { client =>
    super.flush()
    val timeout = Duration((clientFlushInterval + 10) * 20, duration.MILLISECONDS)
    Await.result(client.flush().toFuture, timeout)
  }


  override def createTable(zkAddr: String,
                           tableName: String,
                           cfs: List[String],
                           regionMultiplier: Int,
                           ttl: Option[Int],
                           compressionAlgorithm: String): Unit = {
    logger.info(s"create table: $tableName on $zkAddr, $cfs, $regionMultiplier, $compressionAlgorithm")
    val admin = getAdmin(zkAddr)
    val regionCount = admin.getClusterStatus.getServersSize * regionMultiplier
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      try {
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
          desc.addFamily(columnDesc)
        }

        if (regionCount <= 1) admin.createTable(desc)
        else admin.createTable(desc, getStartKey(regionCount), getEndKey(regionCount), regionCount)
      } catch {
        case e: Throwable =>
          logger.error(s"$zkAddr, $tableName failed with $e", e)
          throw e
      }
    } else {
      logger.info(s"$zkAddr, $tableName, $cfs already exist.")
    }
  }


  /** Asynchbase implementation override default getVertices to use future Cache */
  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = {
    def fromResult(queryParam: QueryParam,
                   kvs: Seq[SKeyValue],
                   version: String): Option[Vertex] = {

      if (kvs.isEmpty) None
      else vertexDeserializer.fromKeyValues(queryParam, kvs, version, None)
    }

    val futures = vertices.map { vertex =>
      val kvs = vertexSerializer(vertex).toKeyValues
      val get = new GetRequest(vertex.hbaseTableName.getBytes, kvs.head.row, Serializable.vertexCf)
      //      get.setTimeout(this.singleGetTimeout.toShort)
      get.setFailfast(true)
      get.maxVersions(1)

      val cacheKey = MurmurHash3.stringHash(get.toString)
      vertexCache.getOrElseUpdate(cacheKey, cacheTTL = 10000)(fetchVertexKeyValues(get)).map { kvs =>
        fromResult(QueryParam.Empty, kvs, vertex.serviceColumn.schemaVersion)
      }
    }

    Future.sequence(futures).map { result => result.toList.flatten }
  }





  /**
   * Private Methods which is specific to Asynchbase implementation.
   */
  private def fetchKeyValuesInner(rpc: AnyRef): Deferred[util.ArrayList[KeyValue]] = {
    rpc match {
      case getRequest: GetRequest => client.get(getRequest)
      case scanner: Scanner =>
        scanner.nextRows().withCallback { kvsLs =>
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
          ls
        }.recoverWith { ex =>
          logger.error(s"fetchKeyValuesInner failed.", ex)
          scanner.close()
          emptyKeyValues
        }
      case _ => Deferred.fromError(new RuntimeException(s"fetchKeyValues failed. $rpc"))
    }
  }

  private def toCacheKeyBytes(hbaseRpc: AnyRef): Array[Byte] = {
    hbaseRpc match {
      case getRequest: GetRequest => getRequest.key()
      case scanner: Scanner => scanner.getCurrentKey()
      case _ =>
        logger.error(s"toCacheKeyBytes failed. not supported class type. $hbaseRpc")
        Array.empty[Byte]
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
