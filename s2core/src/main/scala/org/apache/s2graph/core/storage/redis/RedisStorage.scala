package com.kakao.s2graph.core.storage.redis

import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.GraphExceptions.UnsupportedVersionException
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.storage.redis._
import org.apache.s2graph.core.storage.redis.jedis.JedisClient
import org.apache.s2graph.core.storage.{CanSKeyValue, SKeyValue, Storage}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.logger

import scala.collection.JavaConversions._
import scala.collection.Seq
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success}

/**
 * @author Junki Kim (wishoping@gmail.com), Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Feb/19.
 */
class RedisStorage(override val config: Config)(implicit ec: ExecutionContext)
  extends Storage[Future[QueryRequestWithResult]](config) {

  import GraphType._

  val futureCache = CacheBuilder.newBuilder()
    .initialCapacity(maxSize)
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
    .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
    .maximumSize(maxSize).build[java.lang.Long, (Long, Future[QueryRequestWithResult])]()

  /** Simple Vertex Cache */
  private val vertexCache = CacheBuilder.newBuilder()
    .initialCapacity(maxSize)
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
    .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
    .maximumSize(maxSize).build[java.lang.Integer, Option[Vertex]]()

  override def indexEdgeDeserializer(schemaVer: String) =
    schemaVer match {
      case VERSION4 => new RedisIndexEdgeDeserializable
      case _ => throw new UnsupportedVersionException(s"Redis storage does not support version ${schemaVer}")
    }

  override def snapshotEdgeDeserializer(schemaVer: String) =
    schemaVer match {
      case VERSION4 => new RedisSnapshotEdgeDeserializable
      case _ => throw new UnsupportedOperationException
    }

  override val vertexDeserializer = new RedisVertexDeserializable

  override def indexEdgeSerializer(indexedEdge: IndexEdge) = new RedisIndexEdgeSerializable(indexedEdge)

  override def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge) = new RedisSnapshotEdgeSerializable(snapshotEdge)

  override def vertexSerializer(vertex: Vertex) = new RedisVertexSerializable(vertex)

  private val RedisZsetScore = 1

  private val client = new JedisClient(config)

  /**
   * decide how to store given SKeyValue into storage using storage's client.
   * we assumes that each storage implementation has client as member variable.
   *
   * ex) Asynchbase client provide PutRequest/DeleteRequest/AtomicIncrement/CompareAndSet operations
   * to actually apply byte array into storage. in this case, AsynchbaseStorage use HBaseClient
   * and build + fire rpc and return future that will return if this rpc has been succeed.
   *
  //   * @param kv       : SKeyValue that need to be stored in storage.
//   * @param withWait : flag to control wait ack from storage.
   * note that in AsynchbaseStorage(which support asynchronous operations), even with true,
   * it never block thread, but rather submit work and notified by event loop when storage send ack back.
   * @return ack message from storage.
   */
  override def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
    if (kvs.isEmpty) {
      Future.successful(true)
    } else {
      val futures = kvs.map { kv => writeToRedis(kv, withWait) }
      Future.sequence(futures).map(_.forall(identity))
    }
  }

  def writeToRedis(kv: SKeyValue, withWait: Boolean): Future[Boolean] = {
    val future = Future.successful {
      client.doBlockWithKey[Boolean](GraphUtil.bytesToHexString(kv.row)) { jedis =>
        kv.operation match {
          case SKeyValue.Put if kv.qualifier.length > 0 =>
            jedis.zadd(kv.row, RedisZsetScore, kv.qualifier ++ kv.value) == 1
          case SKeyValue.Put if kv.qualifier.length == 0 =>
            if (kv.operation == SKeyValue.SnapshotPut) {
              jedis.set(kv.row, kv.value) == 1
            } else {
              jedis.zadd(kv.row, RedisZsetScore, kv.value) == 1
            }
          case SKeyValue.Delete if kv.qualifier.length > 0 =>
            jedis.zrem(kv.row, kv.qualifier ++ kv.value) == 1
          case SKeyValue.Delete if kv.qualifier.length == 0 =>
            val r = jedis.zrem(kv.row, kv.value) == 1
            r
          case SKeyValue.Increment => true // no need for degree increment since Redis storage uses ZCARD for degree
        }
      } match {
        case Success(b) => b
        case Failure(e) =>
          logger.error(s"mutation failed. $kv", e)
          false
      }
    }

    if (withWait) future else Future.successful(true)
  }

  /**
   * create table on storage.
   * if storage implementation does not support namespace or table, then there is nothing to be done
   *
   * @param zkAddr
   * @param tableName
   * @param cfs
   * @param regionMultiplier
   * @param ttl
   * @param compressionAlgorithm
   */
  override def createTable(zkAddr: String, tableName: String, cfs: List[String], regionMultiplier: Int, ttl: Option[Int], compressionAlgorithm: String): Unit = {
    logger.info(s"create table is not supported")
  }

  /**
   * build proper request which is specific into storage to call fetchIndexEdgeKeyValues or fetchSnapshotEdgeKeyValues.
   * for example, Asynchbase use GetRequest, Scanner so this method is responsible to build
   * client request(GetRequest, Scanner) based on user provided query.
   *
   * @param queryRequest
   * @return
   */

  override def buildRequest(queryRequest: QueryRequest): RedisRPC = {
    val srcVertex = queryRequest.vertex

    val queryParam = queryRequest.queryParam
    val tgtVertexIdOpt = queryParam.tgtVertexInnerIdOpt
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val (srcInnerId, tgtInnerId) = tgtVertexIdOpt match {
      case Some(tgtVertexId) => // _to is given.
        /** we use toSnapshotEdge so dont need to swap src, tgt */
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        val tgt = InnerVal.convertVersion(tgtVertexId, tgtColumn.columnType, label.schemaVersion)
        (src, tgt)
      case None =>
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        (src, src)
    }

    val (srcVId, tgtVId) = (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val currentTs = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs))
    val edge = Edge(srcV, tgtV, labelWithDir, propsWithTs = propsWithTs)

    val (kv, isSnapshot) = if (tgtVertexIdOpt.isDefined) {
      val snapshotEdge = edge.toSnapshotEdge
      (snapshotEdgeSerializer(snapshotEdge).toKeyValues.head, true)
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == queryParam.labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)

      val indexedEdge = indexedEdgeOpt.get
      (indexEdgeSerializer(indexedEdge).toKeyValues.head, false)
    }

    // Redis supports client-side sharding and does not require hash key so remove heading hash key(2 bytes)
    val rowkey = kv.row

    // 1. RedisGet instance initialize
    if (isSnapshot) new RedisSnapshotGetRequest(rowkey)
    else {
      val _get = new RedisGetRequest(rowkey)
      _get.isIncludeDegree = !tgtVertexIdOpt.isDefined

      // 2. set filter and min/max value's key build
      val (minTs, maxTs) = queryParam.duration.getOrElse(-1L -> -1L)
      val (min, max) =
        if (queryParam.columnRangeFilterMinBytes.length != 0 && queryParam.columnRangeFilterMaxBytes.length != 0)
          (Bytes.add(Bytes.toBytes("["), queryParam.columnRangeFilterMinBytes),
            Bytes.add(Bytes.toBytes("["), queryParam.columnRangeFilterMaxBytes))
        else
          ("-".getBytes, "+".getBytes)

      val (newLimit, newOffset) = revertLimit(queryParam.offset, queryParam.limit)

      _get.setCount(newLimit)
        .setOffset(newOffset)
        .setTimeout(queryParam.rpcTimeoutInMillis)
        .setFilter(min, true, max, true, minTs, maxTs)
    }
  }

  // Degrees are handled with separate keys in Redis.
  // No need for offset/ limit adjustments in QueryParam limit().
  // Hence, the function name revertLimit.
  private def revertLimit(offset: Int, limit: Int): (Int, Int) = {
    /** since degree info is located on first always */
    val (newLimit, newOffset) = if (offset == 0) {
      (limit - 1, offset)
    } else {
      (limit, offset - 1)
    }
    (newLimit, newOffset)
  }

  private def fetchKeyValuesInner(request: RedisRPC) = {
    Future[Seq[SKeyValue]] {
      // send rpc call to Redis instance
      client.doBlockWithKey[Seq[SKeyValue]](GraphUtil.bytesToHexString(request.key)) { jedis =>
        val paddedBytes = Array.fill[Byte](2)(0)
        request match {
          case req@RedisGetRequest(_) =>
            val result = jedis.zrangeByLex(req.key, req.min, req.max, req.offset, req.count).toSeq.map(v =>
              SKeyValue(Array.empty[Byte], paddedBytes ++ req.key, Array.empty[Byte], Array.empty[Byte], v, 0L)
            )
            if (req.isIncludeDegree) {
              val degree = jedis.zcard(req.key)
              val degreeBytes = Bytes.toBytes(degree)
              SKeyValue(Array.empty[Byte], paddedBytes ++ req.key, Array.empty[Byte], Array.empty[Byte], degreeBytes, 0L, operation = SKeyValue.Increment) +: result
            } else result
          case req@RedisSnapshotGetRequest(_) =>
            val _result = jedis.get(req.key)
            if (_result == null) {
              Seq.empty[SKeyValue]
            }
            else {
              val (tsInnerVal, numOfBytesUsed) = InnerVal.fromBytes(_result, 0, 0, GraphType.VERSION4, false)

              val ts = tsInnerVal.value match {
                case n: BigDecimal => n.bigDecimal.longValue()
                case _ => tsInnerVal.toString().toLong
              }

              val snapshot = SKeyValue(Array.empty[Byte], req.key, Array.empty[Byte], Array.empty[Byte], _result, ts, operation = SKeyValue.SnapshotPut)
              Seq[SKeyValue](snapshot)
            }
        }
      } match {
        case Success(v) => v
        case Failure(e) =>
          //          logger.error(s">> get fail!! $e")
          e.printStackTrace()
          Seq[SKeyValue]()
      }
    }
  }

  /**
   * fetch IndexEdges for given queryParam in queryRequest.
   * this expect previous step starting score to propagate score into next step.
   * also parentEdges is necessary to return full bfs tree when query require it.
   *
   * note that return type is general type.
   * for example, currently we wanted to use Asynchbase
   * so single I/O return type should be Deferred[T].
   *
   * if we use native hbase client, then this return type can be Future[T] or just T.
   *
   * @param queryRequest
   * @param prevStepScore
   * @param isInnerCall
   * @param parentEdges
   * @return
   */
  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = {
    def fetchInner(request: RedisRPC) = {
      fetchKeyValuesInner(request).map { values =>
        val edgeWithScores = toEdges(values, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
        val resultEdgesWithScores = if (queryRequest.queryParam.sample >= 0) {
          sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
        } else edgeWithScores
        QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores))
      }.recover { case ex: Exception =>
        logger.error(s"fetchInner failed. fallback return. $request}", ex)
        QueryRequestWithResult(queryRequest, QueryResult(isFailure = true))
      }
    }

    def checkAndExpire(request: RedisRPC,
                       cacheKey: Long,
                       cacheTTL: Long,
                       cachedAt: Long,
                       defer: Future[QueryRequestWithResult]): Future[QueryRequestWithResult] = {

      if (System.currentTimeMillis() >= cachedAt + cacheTTL) {
        // future is too old. so need to expire and fetch new data from storage.
        futureCache.asMap().remove(cacheKey)
        val newPromise = Promise[QueryRequestWithResult]()
        val newFuture = newPromise.future
        futureCache.asMap().putIfAbsent(cacheKey, (System.currentTimeMillis(), newFuture)) match {
          case null =>
            // only one thread succeed to come here concurrently
            // initiate fetch to storage then add callback on complete to finish promise.
            fetchInner(request) map { queryRequestWithResult =>
              newPromise.trySuccess(queryRequestWithResult)
              queryRequestWithResult
            }
            newFuture
          case (cachedAt, oldDefer) => oldDefer
        }
      } else {
        // future is not too old so reuse it.
        defer
      }
    }

    val queryParam = queryRequest.queryParam
    val cacheTTL = queryParam.cacheTTLInMillis
    val request = buildRequest(queryRequest)
    if (cacheTTL <= 0) fetchInner(request)
    else {
      val cacheKeyBytes = Bytes.add(queryRequest.query.cacheKeyBytes, toCacheKeyBytes(request))
      val cacheKey = queryParam.toCacheKey(cacheKeyBytes)

      val cacheVal = futureCache.getIfPresent(cacheKey)
      cacheVal match {
        case null =>
          // here there is no promise set up for this cacheKey so we need to set promise on future cache.
          val promise = Promise[QueryRequestWithResult]()
          val future = promise.future
          val now = System.currentTimeMillis()
          val (cachedAt, defer) = futureCache.asMap().putIfAbsent(cacheKey, (now, future)) match {
            case null =>
              fetchInner(request) map { queryRequestWithResult =>
                promise.trySuccess(queryRequestWithResult)
                queryRequestWithResult
              }
              (now, future)
            case oldVal => oldVal
          }
          checkAndExpire(request, cacheKey, cacheTTL, cachedAt, defer)
        case (cachedAt, defer) =>
          checkAndExpire(request, cacheKey, cacheTTL, cachedAt, defer)
      }
    }

  }

  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = {
    def fromResult(queryParam: QueryParam,
                   kvs: Seq[SKeyValue],
                   version: String): Option[Vertex] = {

      if (kvs.isEmpty) None
      else {
        vertexDeserializer.fromKeyValues(queryParam, kvs, version, None)
      }
    }

    val futures = vertices.map { vertex =>
      val kvs = vertexSerializer(vertex).toKeyValues
      val get = new RedisGetRequest(kvs.head.row)
      get.isIncludeDegree = false

      val cacheKey = MurmurHash3.stringHash(get.toString)
      val cacheVal = vertexCache.getIfPresent(cacheKey)
      if (cacheVal == null) {
        val result = client.doBlockWithKey[Set[SKeyValue]](GraphUtil.bytesToHexString(get.key)) { jedis =>
          get.setFilter("-".getBytes, true, "+".getBytes, true)
          jedis.zrangeByLex(get.key, get.min, get.max).toSet[Array[Byte]].map(v =>
            SKeyValue(Array.empty[Byte], get.key, Array.empty[Byte], Array.empty[Byte], v, 0L)
          )
        } match {
          case Success(v) =>
            v
          case Failure(e) =>
            logger.error(s"Redis vertex get fail: ", e)
            Set[SKeyValue]()
        }
        val fetchVal = fromResult(QueryParam.Empty, result.toSeq, vertex.serviceColumn.schemaVersion)
        Future.successful(fetchVal)
      }

      else Future.successful(cacheVal)
    }

    Future.sequence(futures).map { result => result.toList.flatten }
  }

  /**
   * write requestKeyValue into storage if the current value in storage that is stored matches.
   * note that we only use SnapshotEdge as place for lock, so this method only change SnapshotEdge.
   *
   * Most important thing is this have to be 'atomic' operation.
   * When this operation is mutating requestKeyValue's snapshotEdge, then other thread need to be
   * either blocked or failed on write-write conflict case.
   *
   * Also while this method is still running, then fetchSnapshotEdgeKeyValues should be synchronized to
   * prevent wrong data for read.
   *
   * Best is use storage's concurrency control(either pessimistic or optimistic) such as transaction,
   * compareAndSet to synchronize.
   *
   * for example, AsynchbaseStorage use HBase's CheckAndSet atomic operation to guarantee 'atomicity'.
   * for storage that does not support concurrency control, then storage implementation
   * itself can maintain manual locks that synchronize read(fetchSnapshotEdgeKeyValues)
   * and write(writeLock).
   *
   * @param requestKeyValue
   * @param expectedOpt
   * @return
   */
  override def writeLock(requestKeyValue: SKeyValue, expectedOpt: Option[SKeyValue]): Future[Boolean] = {
    Future.successful {
      client.doBlockWithKey[Boolean](GraphUtil.bytesToHexString(requestKeyValue.row)) { jedis =>
        try {
          expectedOpt match {
            case Some(expected) =>

              jedis.watch(requestKeyValue.row)
              val curVal = jedis.get(requestKeyValue.row)
              val result =
                if (Bytes.compareTo(expected.value, curVal) == 0) {
                  val transaction = jedis.multi()
                  try {
                    transaction.set(requestKeyValue.row, requestKeyValue.value)
                    transaction.exec()
                  } catch {
                    case e: Throwable =>
                      logger.error(s">> error thrown", e)
                      transaction.discard()
                      false
                  }
                } else "[FAIL]"

              result != null && result.toString.equals("[OK]")

            case None =>

              jedis.watch(requestKeyValue.row)
              val curVal = jedis.get(requestKeyValue.row)

              val result =
                if (curVal == null) {
                  val transaction = jedis.multi()
                  try {
                    transaction.set(requestKeyValue.row, requestKeyValue.value)
                    transaction.exec()
                  } catch {
                    case e: Throwable =>
                      logger.error(s">> error thrown", e)
                      transaction.discard()
                      false
                  }

                } else {
                  logger.error(s"\n[[ writeLock failed")
                  "[FAIL]"
                }

              result != null && result.toString.equals("[OK]")
          }
        } catch {
          case ex: Throwable =>
            logger.error(s"writeLock transaction failed old : $requestKeyValue, expected : $expectedOpt", ex)
            throw ex
        }
      } match {
        case Success(b) => b
        case Failure(e) =>
          logger.error(s"writeLock failed old : $requestKeyValue, expected : $expectedOpt", e)
          false
      }
    }
  }

  def simpleWrite(k: String, v: String): Boolean = {
    client.doBlockWithKey(k) { jedis =>
      val result = jedis.set(k, v)
      result != null && result.toString.equals("[OK]")
    } match {
      case Success(b) => b
      case Failure(e) =>
        logger.error("write failed")
        false
    }
  }

  def writeWithTx(k: String, v: String, exp: String): Future[Boolean] = {
    Future.successful {
      client.doBlockWithKey(k) { jedis =>
        jedis.watch(k)
        val fetched = jedis.get(k)
        //        logger.info(s"fetched: $fetched")
        val result = if (fetched.contentEquals(exp)) {
          val tx = jedis.multi()
          try {
            tx.set(k, v)
            val r = tx.exec()
            r
          } catch {
            case e: Throwable =>
              tx.discard()
              false
          }
        } else "[FAIL]"
        result != null && result.toString.equals("[OK]")

      } match {
        case Success(b) => b
        case Failure(e) =>
          logger.error(s"write failed: key - $k, val - $v, exp - $exp")
          false
      }
    }
  }


  /**
   * this method need to be called when client shutdown. this is responsible to cleanUp the resources
   * such as client into storage.
   */
  override def flush(): Unit = {}

  /**
   * fetch SnapshotEdge for given request from storage.
   * also storage datatype should be converted into SKeyValue.
   * note that return type is Sequence rather than single SKeyValue for simplicity,
   * even though there is assertions sequence.length == 1.
   *
   * @param request
   * @return
   */
  override def fetchSnapshotEdgeKeyValues(request: AnyRef): Future[Seq[SKeyValue]] = {
    val defer = fetchKeyValuesInner(request.asInstanceOf[RedisRPC])
    defer.map { kvsArr =>
      kvsArr.map { kv =>
        implicitly[CanSKeyValue[SKeyValue]].toSKeyValue(kv)
      }
    }
  }

  /**
   * decide how to apply given edges(indexProps values + Map(_count -> countVal)) into storage.
   *
   * @param edges
   * @param withWait
   * @return
   */
  override def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = {
    logger.error(s"'incrementCount' operation is not yet supported")
    Future[Seq[(Boolean, Long)]] {
      Seq[(Boolean, Long)]()
    }
  }

  /**
   * responsible to fire parallel fetch call into storage and create future that will return merged result.
   *
   * @param queryRequestWithScoreLs
   * @param prevStepEdges
   * @return
   */
  override def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = {

    val reads: Seq[Future[QueryRequestWithResult]] = for {
      (queryRequest, prevStepScore) <- queryRequestWithScoreLs
    } yield {
        val prevStepEdgesOpt = prevStepEdges.get(queryRequest.vertex.id)
        if (prevStepEdgesOpt.isEmpty) throw new RuntimeException("miss match on prevStepEdge and current GetRequest")

        val parentEdges = for {
          parentEdge <- prevStepEdgesOpt.get
        } yield parentEdge

        fetch(queryRequest, prevStepScore, isInnerCall = true, parentEdges)
      }

    Future.sequence(reads)
  }

  private def toCacheKeyBytes(redisRpc: RedisRPC): Array[Byte] = {
    redisRpc match {
      case getRequest: RedisGetRequest => getRequest.key
      case snapshotRequest: RedisSnapshotGetRequest => snapshotRequest.key
      case _ =>
        logger.error(s"toCacheKeyBytes failed. not supported class type. $redisRpc")
        Array.empty[Byte]
    }
  }

  /**
   * fetch Vertex for given request from storage.
   * @param request
   * @return
   */
  override def fetchVertexKeyValues(request: AnyRef): Future[scala.Seq[SKeyValue]] = ???
}
