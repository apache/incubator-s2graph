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
 *
 */

package org.apache.s2graph.core.storage.hbase

import java.util
import java.util.Base64

import com.stumbleupon.async.Deferred
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, ServiceColumn}
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.storage.serde._
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorage.{AsyncRPC, ScanWithRange}
import org.apache.s2graph.core.types.{HBaseType, VertexId}
import org.apache.s2graph.core.utils.{CanDefer, DeferCache, Extensions, logger}
import org.hbase.async.FilterList.Operator.MUST_PASS_ALL
import org.hbase.async._

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

class AsynchbaseStorageReadable(val graph: S2Graph,
                                val config: Config,
                                val client: HBaseClient,
                                val serDe: StorageSerDe,
                                override val io: StorageIO) extends StorageReadable {
  import Extensions.DeferOps
  import CanDefer._

  private val emptyKeyValues = new util.ArrayList[KeyValue]()
  private val emptyKeyValuesLs = new util.ArrayList[util.ArrayList[KeyValue]]()
  private val emptyStepResult = new util.ArrayList[StepResult]()

  /** Future Cache to squash request */
  lazy private val futureCache = new DeferCache[StepResult, Deferred, Deferred](config, StepResult.Empty, "AsyncHbaseFutureCache", useMetric = true)
  /** v4 max next row size */
  private val v4_max_num_rows = 10000
  private def getV4MaxNumRows(limit : Int): Int = {
    if (limit < v4_max_num_rows) limit
    else v4_max_num_rows
  }

  /**
    * build proper request which is specific into storage to call fetchIndexEdgeKeyValues or fetchSnapshotEdgeKeyValues.
    * for example, Asynchbase use GetRequest, Scanner so this method is responsible to build
    * client request(GetRequest, Scanner) based on user provided query.
    *
    * @param queryRequest
    * @return
    */
  private def buildRequest(queryRequest: QueryRequest, edge: S2Edge) = {
    import Serializable._
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
    *
    * @param queryRequest
    * @param vertex
    * @return
    */
  private def buildRequest(queryRequest: QueryRequest, vertex: S2Vertex) = {
    val kvs = serDe.vertexSerializer(vertex).toKeyValues
    val get = new GetRequest(vertex.hbaseTableName.getBytes, kvs.head.row, Serializable.vertexCf)
    //      get.setTimeout(this.singleGetTimeout.toShort)
    get.setFailfast(true)
    get.maxVersions(1)

    Left(get)
  }

  override def fetchKeyValues(queryRequest: QueryRequest, edge: S2Edge)(implicit ec: ExecutionContext) = {
    val rpc = buildRequest(queryRequest, edge)
    fetchKeyValues(rpc)
  }

  override def fetchKeyValues(queryRequest: QueryRequest, vertex: S2Vertex)(implicit ec: ExecutionContext) = {
    val rpc = buildRequest(queryRequest, vertex)
    fetchKeyValues(rpc)
  }

  /**
    * responsible to fire parallel fetch call into storage and create future that will return merged result.
    *
    * @param queryRequests
    * @param prevStepEdges
    * @return
    */
  override def fetches(queryRequests: Seq[QueryRequest], prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext) = {
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

  def fetchKeyValues(rpc: AsyncRPC)(implicit ec: ExecutionContext) = {
    val defer = fetchKeyValuesInner(rpc)
    defer.toFuture(emptyKeyValues).map { kvsArr =>
      kvsArr.map { kv =>
        implicitly[CanSKeyValue[KeyValue]].toSKeyValue(kv)
      }
    }
  }

  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2Edge]] = {
    val futures = Label.findAll().groupBy(_.hbaseTableName).toSeq.map { case (hTableName, labels) =>
      val distinctLabels = labels.toSet
      val scan = AsynchbasePatcher.newScanner(client, hTableName)
      scan.setFamily(Serializable.edgeCf)
      scan.setMaxVersions(1)

      scan.nextRows(S2Graph.FetchAllLimit).toFuture(emptyKeyValuesLs).map {
        case null => Seq.empty
        case kvsLs =>
          kvsLs.flatMap { kvs =>
            kvs.flatMap { kv =>
              val sKV = implicitly[CanSKeyValue[KeyValue]].toSKeyValue(kv)

              serDe.indexEdgeDeserializer(schemaVer = HBaseType.DEFAULT_VERSION)
                .fromKeyValues(Seq(kv), None)
                .filter(e => distinctLabels(e.innerLabel) && e.direction == "out" && !e.isDegree)
            }
          }
      }
    }

    Future.sequence(futures).map(_.flatten)
  }

  override def fetchVertices(vertices: Seq[S2Vertex])(implicit ec: ExecutionContext) = {
    def fromResult(kvs: Seq[SKeyValue], version: String): Seq[S2Vertex] = {
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
          kvsLs.flatMap { kvs =>
            serDe.vertexDeserializer(schemaVer = HBaseType.DEFAULT_VERSION).fromKeyValues(kvs, None)
              .filter(v => distinctColumns(v.serviceColumn))
          }
      }
    }
    Future.sequence(futures).map(_.flatten)
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
  private def fetch(queryRequest: QueryRequest,
                    isInnerCall: Boolean,
                    parentEdges: Seq[EdgeWithScore])(implicit ec: ExecutionContext): Deferred[StepResult] = {

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

        io.toEdges(kvs, queryRequest, prevStepScore, isInnerCall, parentEdges, startOffset, len)
      }
    }

    val queryParam = queryRequest.queryParam
    val cacheTTL = queryParam.cacheTTLInMillis
    /* with version 4, request's type is (Scanner, (Int, Int)). otherwise GetRequest. */

    val edge = graph.toRequestEdge(queryRequest, parentEdges)
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

  /**
    * Private Methods which is specific to Asynchbase implementation.
    */
  private def fetchKeyValuesInner(rpc: AsyncRPC)(implicit ec: ExecutionContext): Deferred[util.ArrayList[KeyValue]] = {
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

  private def toCacheKeyBytes(hbaseRpc: AsyncRPC): Array[Byte] = {
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

}
