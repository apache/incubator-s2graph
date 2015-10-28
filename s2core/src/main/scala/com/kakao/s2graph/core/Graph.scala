package com.kakao.s2graph.core

import java.util
import java.util.{Properties, ArrayList}
import java.util.concurrent.{ConcurrentHashMap, Executors}

import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.parsers.WhereParser
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.types2.{AsyncHBaseStorageWritable}
import com.kakao.s2graph.logger
import com.google.common.cache.CacheBuilder
import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.hbase.async
import org.hbase.async._

import scala.collection.JavaConversions._
import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.hashing.MurmurHash3
import scala.util.{Random, Failure, Success, Try}


object Graph {
  val vertexCf = "v".getBytes()
  val edgeCf = "e".getBytes()

  val maxValidEdgeListSize = 10000
  val DefaultScore = 1.0

  private val connections = new java.util.concurrent.ConcurrentHashMap[String, Connection]()
  private val clients = new java.util.concurrent.ConcurrentHashMap[String, HBaseClient]()

  val storageFactory = AsyncHBaseStorageWritable
//    StorageFactory("asynchbase")

  var emptyKVs = new ArrayList[KeyValue]()

  val DefaultConfigs: Map[String, AnyRef] = Map(
    "hbase.zookeeper.quorum" -> "localhost",
    "hbase.table.name" -> "s2graph",
    "hbase.table.compression.algorithm" -> "gz",
    "phase" -> "dev",
    "db.default.driver" -> "com.mysql.jdbc.Driver",
    "db.default.url" -> "jdbc:mysql://localhost:3306/graph_dev",
    "db.default.password" -> "graph",
    "db.default.user" -> "graph",
    "cache.max.size" -> java.lang.Integer.valueOf(10000),
    "cache.ttl.seconds" -> java.lang.Integer.valueOf(60),
    "hbase.client.retries.number" -> java.lang.Integer.valueOf(20),
    "hbase.rpcs.buffered_flush_interval" -> java.lang.Short.valueOf(100.toShort),
    "hbase.rpc.timeout" -> java.lang.Integer.valueOf(1000),
    "max.retry.number" -> java.lang.Integer.valueOf(100)
  )

  var config: Config = ConfigFactory.parseMap(DefaultConfigs)
  var executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  val asyncConfig: org.hbase.async.Config = new org.hbase.async.Config()
  lazy val MaxBackOff = 5
  lazy val clientFlushInterval = this.config.getInt("hbase.rpcs.buffered_flush_interval").toString().toShort
  lazy val MaxRetryNum = this.config.getInt("max.retry.number")


  lazy val cache = CacheBuilder.newBuilder()
    .maximumSize(this.config.getInt("cache.max.size"))
    .build[java.lang.Integer, Seq[QueryResult]]()

  //TODO: Merge this into cache.
  lazy val vertexCache = CacheBuilder.newBuilder()
    .maximumSize(this.config.getInt("cache.max.size"))
    .build[java.lang.Integer, Option[Vertex]]()

  def loadAsyncConfig() = {
    for (entry <- config.entrySet() if entry.getKey.contains("hbase")) {
      this.asyncConfig.overrideConfig(entry.getKey, entry.getValue.unwrapped().toString)
    }
  }

  def apply(config: com.typesafe.config.Config)(implicit ex: ExecutionContext) = {
    this.config = config.withFallback(this.config)
    loadAsyncConfig()

    Model(this.config)

    this.executionContext = ex

    // make hbase client for cache
    getClient(this.config.getString("hbase.zookeeper.quorum"))

    ExceptionHandler.apply(config)

    for {
      entry <- this.config.entrySet() if DefaultConfigs.contains(entry.getKey)
      (k, v) = (entry.getKey, entry.getValue)
    } {
      logger.info(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")
      println(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")
    }
  }

  def getClient(zkQuorum: String, flushInterval: Short = clientFlushInterval) = {
    val key = zkQuorum + ":" + flushInterval

    clients.getOrElseUpdate(key, {
      val client = new HBaseClient(this.asyncConfig)
      client.setFlushInterval(flushInterval)
      client
    })
  }

  /** This method only used by Bulk loader */
  def getConn(zkQuorum: String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    connections.getOrElseUpdate(zkQuorum, ConnectionFactory.createConnection(conf))
  }


  def flush: Unit = {
    for ((zkQuorum, client) <- Graph.clients) {
      Await.result(deferredToFutureWithoutFallback(client.flush), Duration((clientFlushInterval + 10) * 20, duration.MILLISECONDS))
    }
  }

  //  def getConn(zkQuorum: String) = connections.getOrElseUpdate(zkQuorum, ConnectionFactory.createConnection(this.hbaseConfig))

  def deferredToFuture[A](d: Deferred[A])(fallback: A): Future[A] = {
    val promise = Promise[A]

    d.addBoth(new Callback[Unit, A] {
      def call(arg: A) = arg match {
        case e: Exception =>
          logger.error(s"deferred failed with return fallback: $e", e)
          promise.success(fallback)
        case _ => promise.success(arg)
      }
    })

    promise.future
  }

  def deferredToFutureWithoutFallback[T](d: Deferred[T]) = {
    val promise = Promise[T]

    d.addBoth(new Callback[Unit, T] {
      def call(arg: T) = arg match {
        case e: Exception =>
          logger.error(s"deferred return Exception: $e", e)
          promise.failure(e)
        case _ => promise.success(arg)
      }
    })

    promise.future
  }

  def deferredCallbackWithFallback[T, R](d: Deferred[T])(f: T => R, fallback: => R) = {
    d.addCallback(new Callback[R, T] {
      def call(args: T): R = {
        f(args)
      }
    }).addErrback(new Callback[R, Exception] {
      def call(e: Exception): R = {
        logger.error(s"Exception on deferred: $e", e)
        fallback
      }
    })
  }

  private def errorBack(block: => Exception => Unit) = new Callback[Unit, Exception] {
    def call(ex: Exception): Unit = block(ex)
  }

  def writeAsyncWithWaitRetry(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]], retryNum: Int): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext

    if (retryNum > MaxRetryNum) {
      logger.error(s"writeAsyncWithWaitRetry failed: $elementRpcs")
      Future.successful(elementRpcs.map(_ => false))
    } else {
      writeAsyncWithWait(zkQuorum, elementRpcs).flatMap { rets =>
        val allSuccess = rets.forall(identity)
        if (allSuccess) Future.successful(elementRpcs.map(_ => true))
        else {
          Thread.sleep(Random.nextInt(MaxBackOff) + 1)
          writeAsyncWithWaitRetry(zkQuorum, elementRpcs, retryNum + 1)
        }
      }
    }
  }

  def writeAsyncWithWaitRetrySimple(zkQuorum: String, elementRpcs: Seq[HBaseRpc], retryNum: Int): Future[Boolean] = {
    implicit val ex = this.executionContext

    if (retryNum > MaxRetryNum) {
      logger.error(s"writeAsyncWithWaitRetry failed: $elementRpcs")
      Future.successful(false)
    } else {
      writeAsyncWithWaitSimple(zkQuorum, elementRpcs).flatMap { ret =>
        if (ret) Future.successful(ret)
        else {
          Thread.sleep(Random.nextInt(MaxBackOff) + 1)
          writeAsyncWithWaitRetrySimple(zkQuorum, elementRpcs, retryNum + 1)
        }
      }
    }
  }

  def writeAsyncWithWait(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]]): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext

    if (elementRpcs.isEmpty) {
      Future.successful(Seq.empty[Boolean])
    } else {
      val client = getClient(zkQuorum, flushInterval = 0.toShort)
      val defers = elementRpcs.map { rpcs =>

        val defer = rpcs.map { rpc =>
          val deferred = rpc match {
            case d: DeleteRequest => client.delete(d).addErrback(errorBack(ex => logger.error(s"delete request failed. $d, $ex", ex)))
            case p: PutRequest => client.put(p).addErrback(errorBack(ex => logger.error(s"put request failed. $p, $ex", ex)))
            case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).addErrback(errorBack(ex => logger.error(s"increment request failed. $i, $ex", ex)))
          }

          deferredCallbackWithFallback(deferred)({
            (anyRef: Any) => anyRef match {
              case e: Exception =>
                logger.error(s"mutation failed. $e", e)
                false
              case _ => true
            }
          }, false)
        }

        deferredToFutureWithoutFallback(Deferred.group(defer)).map { arr => arr.forall(identity) }
      }

      Future.sequence(defers)
    }
  }

  def writeAsyncWithWaitSimple(zkQuorum: String, elementRpcs: Seq[HBaseRpc]): Future[Boolean] = {
    implicit val ex = this.executionContext

    if (elementRpcs.isEmpty) {
      Future.successful(true)
    } else {
      val client = getClient(zkQuorum)

      val defers = elementRpcs.map { rpc =>
        val deferred = rpc match {
          case d: DeleteRequest => client.delete(d).addErrback(errorBack(ex => logger.error(s"delete request failed. $d, $ex", ex)))
          case p: PutRequest => client.put(p).addErrback(errorBack(ex => logger.error(s"put request failed. $p, $ex", ex)))
          case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).addErrback(errorBack(ex => logger.error(s"increment request failed. $i, $ex", ex)))
        }

        deferredCallbackWithFallback(deferred)({
          (anyRef: Any) => anyRef match {
            case e: Exception =>
              logger.error(s"mutation failed. $e", e)
              false
            case _ => true
          }
        }, false)
      }
      deferredToFutureWithoutFallback(Deferred.group(defers)).map { arr => arr.forall(identity) }
    }
  }

  def writeAsyncSimple(zkQuorum: String, elementRpcs: Seq[HBaseRpc]): Future[Boolean] = {
    implicit val ex = this.executionContext

    if (elementRpcs.isEmpty) {
      Future.successful(true)
    } else {
      val client = getClient(zkQuorum)

      elementRpcs.foreach { rpc =>
        val deferred = rpc match {
          case d: DeleteRequest => client.delete(d).addErrback(errorBack(ex => logger.error(s"delete request failed. $d, $ex", ex)))
          case p: PutRequest => client.put(p).addErrback(errorBack(ex => logger.error(s"put request failed. $p, $ex", ex)))
          case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).addErrback(errorBack(ex => logger.error(s"increment request failed. $i, $ex", ex)))
        }

        deferredCallbackWithFallback(deferred)({
          (anyRef: Any) => anyRef match {
            case e: Exception =>
              logger.error(s"mutation failed. $e", e)
              false
            case _ => true
          }
        }, false)
      }
      Future.successful(true)
    }
  }

  def writeAsync(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]]): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext

    if (elementRpcs.isEmpty) {
      Future.successful(Seq.empty[Boolean])
    } else {
      val client = getClient(zkQuorum)
      elementRpcs.foreach { rpcs =>

        rpcs.foreach { rpc =>
          val deferred = rpc match {
            case d: DeleteRequest => client.delete(d).addErrback(errorBack(ex => logger.error(s"delete request failed. $d, $ex", ex)))
            case p: PutRequest => client.put(p).addErrback(errorBack(ex => logger.error(s"put request failed. $p, $ex", ex)))
            case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).addErrback(errorBack(ex => logger.error(s"increment request failed. $i, $ex", ex)))
          }

          deferredCallbackWithFallback(deferred)({
            (anyRef: Any) => anyRef match {
              case e: Exception =>
                logger.error(s"mutation failed. $e", e)
                false
              case _ => true
            }
          }, false)
        }
      }

      Future.successful(elementRpcs.map(x => true))
    }
  }


  def getEdgesAsync(q: Query): Future[Seq[QueryResult]] = {
    implicit val ex = this.executionContext

    Try {
      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        Future.successful(q.vertices.map(v => QueryResult(query = q, stepIdx = 0, queryParam = QueryParam.Empty)))
      } else {
        val startQueryResultLs = QueryResult.fromVertices(q)
        q.steps.zipWithIndex.foldLeft(Future.successful(startQueryResultLs)) { case (acc, (_, idx)) =>
          getEdgesAsyncWithRank(acc, q, idx)
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        Future.successful(q.vertices.map(v => QueryResult(query = q, stepIdx = 0, queryParam = QueryParam.Empty)))
    } get
  }

  private def fetchEdgesLs(prevStepTgtVertexIdEdges: Map[VertexId, Seq[EdgeWithScore]],
                           currentStepRequestLss: Seq[(Iterable[(VertexId, GetRequest, QueryParam)], Double)],
                           q: Query, stepIdx: Int): Seq[Deferred[QueryResult]] = {
    for {
      (prevStepTgtVertexResultLs, prevScore) <- currentStepRequestLss
      (startVertexId, getRequest, queryParam) <- prevStepTgtVertexResultLs
    } yield {
      val prevStepEdgesOpt = prevStepTgtVertexIdEdges.get(startVertexId)
      if (prevStepEdgesOpt.isEmpty) throw new RuntimeException("miss match on prevStepEdge and current GetRequest")

      val parentEdges = for {
        parentEdge <- prevStepEdgesOpt.get
      } yield parentEdge

      fetchEdgesWithCache(parentEdges, getRequest, q, stepIdx, queryParam, prevScore)
    }
  }

  private def fetchEdgesWithCache(parentEdges: Seq[EdgeWithScore],
                                  getRequest: GetRequest,
                                  q: Query,
                                  stepIdx: Int,
                                  queryParam: QueryParam,
                                  prevScore: Double): Deferred[QueryResult] = {
    val cacheKey = queryParam.toCacheKey(getRequest)
    def queryResultCallback(cacheKey: Int) = new Callback[QueryResult, QueryResult] {
      def call(arg: QueryResult): QueryResult = {
        //        logger.debug(s"queryResultCachePut, $arg")
        cache.put(cacheKey, Seq(arg))
        arg
      }
    }
    if (queryParam.cacheTTLInMillis > 0) {
      val cacheTTL = queryParam.cacheTTLInMillis
      if (cache.asMap().containsKey(cacheKey)) {
        val cachedVal = cache.asMap().get(cacheKey)
        if (cachedVal != null && cachedVal.nonEmpty && queryParam.timestamp - cachedVal.head.timestamp < cacheTTL) {
          // val elapsedTime = queryParam.timestamp - cachedVal.timestamp
          //          logger.debug(s"cacheHitAndValid: $cacheKey, $cacheTTL, $elapsedTime")
          Deferred.fromResult(cachedVal.head)
        }
        else {
          // cache.asMap().remove(cacheKey)
          //          logger.debug(s"cacheHitInvalid(invalidated): $cacheKey, $cacheTTL")
          fetchEdges(parentEdges, getRequest, q, stepIdx, queryParam, prevScore).addBoth(queryResultCallback(cacheKey))
        }
      } else {
        //        logger.debug(s"cacheMiss: $cacheKey")
        fetchEdges(parentEdges, getRequest, q, stepIdx, queryParam, prevScore).addBoth(queryResultCallback(cacheKey))
      }
    } else {
      //      logger.debug(s"cacheMiss(no cacheTTL in QueryParam): $cacheKey")
      fetchEdges(parentEdges, getRequest, q, stepIdx, queryParam, prevScore)
    }
  }

  /** actual request to HBase */
  private def fetchEdges(parentEdges: Seq[EdgeWithScore],
                         getRequest: GetRequest,
                         q: Query,
                         stepIdx: Int,
                         queryParam: QueryParam,
                         prevScore: Double): Deferred[QueryResult] =
    Try {
      val client = getClient(queryParam.label.hbaseZkAddr)

      val successCallback = (kvs: util.ArrayList[KeyValue]) => {
        val edgeWithScores = Edge.toEdges(kvs, queryParam, prevScore, isInnerCall = false, parentEdges)
        QueryResult(q, stepIdx, queryParam, edgeWithScores)
      }

      val fallback = QueryResult(q, stepIdx, queryParam)

      deferredCallbackWithFallback(client.get(getRequest))(successCallback, fallback)

    } recover {
      case e: Exception =>
        logger.error(s"Exception: $e", e)
        Deferred.fromResult(QueryResult(q, stepIdx, queryParam))
    } get

  private def alreadyVisitedVertices(queryResultLs: Seq[QueryResult]) = {
    val vertices = for {
      queryResult <- queryResultLs
      (edge, score) <- queryResult.edgeWithScoreLs
    } yield {
        (edge.labelWithDir, if (edge.labelWithDir.dir == GraphUtil.directions("out")) edge.tgtVertex else edge.srcVertex) -> true
      }
    vertices.toMap
  }

  private def getEdgesAsyncWithRank(queryResultsLs: Seq[QueryResult],
                                    q: Query,
                                    stepIdx: Int): Future[Seq[QueryResult]] = {
    implicit val ex = executionContext

    val prevStepOpt = if (stepIdx > 0) Option(q.steps(stepIdx - 1)) else None
    val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.DefaultThreshold)
    val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
    val step = q.steps(stepIdx)
    val alreadyVisited =
      if (stepIdx == 0) Map.empty[(LabelWithDirection, Vertex), Boolean]
      else alreadyVisitedVertices(queryResultsLs)

    //TODO:
    val groupedBy = queryResultsLs.flatMap { queryResult =>
      queryResult.edgeWithScoreLs.map { case (edge, score) =>
        edge.tgtVertex ->(edge, score)
      }
    }.groupBy { case (vertex, (edge, score)) =>
      vertex
    }

    //    logger.debug(s"groupedBy: $groupedBy")
    val groupedByFiltered = for {
      (vertex, edgesWithScore) <- groupedBy
      aggregatedScore = edgesWithScore.map(_._2._2).sum if aggregatedScore >= prevStepThreshold
    } yield vertex -> aggregatedScore

    val prevStepTgtVertexIdEdges = for {
      (vertex, edgesWithScore) <- groupedBy
    } yield vertex.id -> edgesWithScore.map { case (vertex, (edge, score)) => EdgeWithScore(edge, score) }
    //    logger.debug(s"groupedByFiltered: $groupedByFiltered")

    val nextStepSrcVertices = if (prevStepLimit >= 0) {
      groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
    } else {
      groupedByFiltered.toSeq
    }
    //    logger.debug(s"nextStepSrcVertices: $nextStepSrcVertices")
    val currentStepRequestLss = buildGetRequests(nextStepSrcVertices, step.queryParams)

    val queryParams = currentStepRequestLss.flatMap { case (getsWithQueryParams, prevScore) =>
      getsWithQueryParams.map { case (vertexId, get, queryParam) => queryParam }
    }
    /** support step wise cache */
    val getWithQueryParams = currentStepRequestLss.flatMap { case (getsWithQueryParams, prevScore) =>
      getsWithQueryParams.map { case (vertexId, get, queryParam) => (get, queryParam) }
    }
    val cacheKey = step.toCacheKey(getWithQueryParams)

    val fallback = new util.ArrayList(queryParams.map(param => QueryResult(q, stepIdx, param)))

    if (step.cacheTTL > 0) {
      val cacheVal = cache.getIfPresent(cacheKey)
      if (cacheVal == null) {
        val deferred = fetchEdgesLs(prevStepTgtVertexIdEdges, currentStepRequestLss, q, stepIdx)
        val grouped: Deferred[util.ArrayList[QueryResult]] = Deferred.group(deferred)

        filterEdges(deferredToFuture(grouped)(fallback), q, stepIdx, alreadyVisited).map { queryResultLs =>
          cache.put(cacheKey, queryResultLs)
          queryResultLs
        }
      } else {
        Future.successful(cacheVal)
      }
    } else {
      val deferred = fetchEdgesLs(prevStepTgtVertexIdEdges, currentStepRequestLss, q, stepIdx)
      val grouped: Deferred[util.ArrayList[QueryResult]] = Deferred.group(deferred)

      filterEdges(deferredToFuture(grouped)(fallback), q, stepIdx, alreadyVisited)
    }

  }

  def getEdgesAsyncWithRank(queryResultLsFuture: Future[Seq[QueryResult]], q: Query, stepIdx: Int): Future[Seq[QueryResult]] = {
    implicit val ex = executionContext
    for {
      queryResultLs <- queryResultLsFuture
      ret <- getEdgesAsyncWithRank(queryResultLs, q, stepIdx)
    } yield {
      ret
    }
  }

  def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): Future[QueryResult] = {
    implicit val ex = this.executionContext


    val invertedEdge = Edge(srcVertex, tgtVertex, queryParam.labelWithDir).toInvertedEdgeHashLike
    val getRequest = queryParam.tgtVertexInnerIdOpt(Option(invertedEdge.tgtVertex.innerId))
      .buildGetRequest(invertedEdge.srcVertex)
    val q = Query.toQuery(Seq(srcVertex), queryParam)

    deferredToFuture(getClient(queryParam.label.hbaseZkAddr).get(getRequest))(emptyKVs).map { kvs =>
      val edgeWithScoreLs = Edge.toEdges(kvs, queryParam, prevScore = 1.0, isInnerCall = isInnerCall, Nil)
      QueryResult(query = q, stepIdx = 0, queryParam = queryParam, edgeWithScoreLs = edgeWithScoreLs)
    }
  }

  def checkEdges(quads: Seq[(Vertex, Vertex, Label, Int)], isInnerCall: Boolean): Future[Seq[QueryResult]] = {
    implicit val ex = this.executionContext
    val futures = for {
      (srcVertex, tgtVertex, label, dir) <- quads
      queryParam = QueryParam(LabelWithDirection(label.id.get, dir))
    } yield getEdge(srcVertex, tgtVertex, queryParam, isInnerCall)

    Future.sequence(futures)
  }


  def buildGetRequests(startVertices: Seq[(Vertex, Double)], params: List[QueryParam]): Seq[(Iterable[(VertexId, GetRequest, QueryParam)], Double)] = {
    for {
      (vertex, score) <- startVertices
    } yield {
      val requests = for {
        param <- params
      } yield {
          (vertex.id, param.buildGetRequest(vertex), param)
        }
      (requests, score)
    }
  }


  def convertEdges(queryParam: QueryParam, edge: Edge, nextStepOpt: Option[Step]): Seq[Edge] = {
    for {
      convertedEdge <- queryParam.transformer.transform(edge, nextStepOpt)
    } yield convertedEdge
  }

  /** helpers for filterEdges */
  private type HashKey = (Int, Int, Int, Int, Boolean)
  private type FilterHashKey = (Int, Int)
  private type Result = (ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]],
    ConcurrentHashMap[HashKey, (FilterHashKey, Edge, Double)],
    ListBuffer[(HashKey, FilterHashKey, Edge, Double)])

  /**
   * create edge hashKey, filterHashKey for aggregate edges for queryParams in current step.
   * @param queryParam
   * @param edge
   * @param isDegree
   * @return
   */
  private def toHashKey(queryParam: QueryParam, edge: Edge, isDegree: Boolean): (HashKey, FilterHashKey) = {
    val src = edge.srcVertex.innerId.hashCode()
    val tgt = edge.tgtVertex.innerId.hashCode()
    val hashKey = (src, edge.labelWithDir.labelId, edge.labelWithDir.dir, tgt, isDegree)
    val filterHashKey = (src, tgt)

    (hashKey, filterHashKey)
  }

  /**
   * create timeDecayed newScore
   * @param queryParam
   * @param edge
   * @return
   */
  private def processTimeDecay(queryParam: QueryParam, edge: Edge) = {
    /** process time decay */
    val tsVal = queryParam.timeDecay match {
      case None => 1.0
      case Some(timeDecay) =>
        val timeDiff = queryParam.timestamp - edge.ts
        timeDecay.decay(timeDiff)
    }
    tsVal
  }

  /**
   * aggregate score into result. note that this is only aggregate in queryParam scope
   * @param newScore
   * @param resultEdges
   * @param duplicateEdges
   * @param edgeWithScoreSorted
   * @param hashKey
   * @param filterHashKey
   * @param queryParam
   * @param convertedEdge
   * @return
   */
  private def aggregateScore(newScore: Double,
                             resultEdges: ConcurrentHashMap[HashKey, (FilterHashKey, Edge, Double)],
                             duplicateEdges: ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]],
                             edgeWithScoreSorted: ListBuffer[(HashKey, FilterHashKey, Edge, Double)],
                             hashKey: HashKey,
                             filterHashKey: FilterHashKey,
                             queryParam: QueryParam,
                             convertedEdge: Edge) = {
    /** skip duplicate policy check if consistencyLevel is strong */
    if (queryParam.label.consistencyLevel != "strong" && resultEdges.containsKey(hashKey)) {
      val (oldFilterHashKey, oldEdge, oldScore) = resultEdges.get(hashKey)
      //TODO:
      queryParam.duplicatePolicy match {
        case Query.DuplicatePolicy.First => // do nothing
        case Query.DuplicatePolicy.Raw =>
          if (duplicateEdges.containsKey(hashKey)) {
            duplicateEdges.get(hashKey).append(convertedEdge -> newScore)
          } else {
            val newBuffer = new ListBuffer[(Edge, Double)]
            newBuffer.append(convertedEdge -> newScore)
            duplicateEdges.put(hashKey, newBuffer)
          }
        case Query.DuplicatePolicy.CountSum =>
          resultEdges.put(hashKey, (filterHashKey, oldEdge, oldScore + 1))
        case _ =>
          resultEdges.put(hashKey, (filterHashKey, oldEdge, oldScore + newScore))
      }
    } else {
      resultEdges.put(hashKey, (filterHashKey, convertedEdge, newScore))
      edgeWithScoreSorted.append((hashKey, filterHashKey, convertedEdge, newScore))
    }
  }

  /**
   * apply where parser filter.
   * @param queryResult
   * @return
   */
  private def queryResultWithFilter(queryResult: QueryResult) = {
    val whereFilter = queryResult.queryParam.where.get
    if (whereFilter == WhereParser.success) queryResult.edgeWithScoreLs
    else queryResult.edgeWithScoreLs.withFilter(edgeWithScore => whereFilter.filter(edgeWithScore._1))
  }

  /**
   *
   * @param queryParam
   * @param edge
   * @param nextStepOpt
   * @return
   */
  private def buildConvertedEdges(queryParam: QueryParam,
                                  edge: Edge,
                                  nextStepOpt: Option[Step]) = {
    if (queryParam.transformer.isDefault) Seq(edge) else convertEdges(queryParam, edge, nextStepOpt)
  }

  /**
   *
   * @param edge
   * @param score
   * @param hashKey
   * @param duplicateEdges
   * @return
   */
  private def fetchDuplicatedEdges(edge: Edge,
                                   score: Double,
                                   hashKey: HashKey,
                                   duplicateEdges: ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]]) = {
    (edge -> score) +: (if (duplicateEdges.containsKey(hashKey)) duplicateEdges.get(hashKey) else Seq.empty)
  }

  /**
   *
   * @param queryResult
   * @param queryParamResult
   * @param edgesToInclude
   * @param edgesToExclude
   * @return
   */
  private def aggregateResults(queryResult: QueryResult,
                               queryParamResult: Result,
                               edgesToInclude: util.HashSet[FilterHashKey],
                               edgesToExclude: util.HashSet[FilterHashKey]) = {
    val (duplicateEdges, resultEdges, edgeWithScoreSorted) = queryParamResult
    val edgesWithScores = for {
      (hashKey, filterHashKey, edge, _) <- edgeWithScoreSorted if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
      score = resultEdges.get(hashKey)._3
      (duplicateEdge, aggregatedScore) <- fetchDuplicatedEdges(edge, score, hashKey, duplicateEdges) if aggregatedScore >= queryResult.queryParam.threshold
    } yield (duplicateEdge, aggregatedScore)

    QueryResult(queryResult.query, queryResult.stepIdx, queryResult.queryParam, edgesWithScores)
  }


  /**
   *
   * @param queryResultLsFuture
   * @param q
   * @param stepIdx
   * @param alreadyVisited
   * @return
   */
  def filterEdges(queryResultLsFuture: Future[ArrayList[QueryResult]],
                  q: Query,
                  stepIdx: Int,
                  alreadyVisited: Map[(LabelWithDirection, Vertex), Boolean] = Map.empty[(LabelWithDirection, Vertex), Boolean]): Future[Seq[QueryResult]] = {
    implicit val ex = Graph.executionContext

    queryResultLsFuture.map { queryResultLs =>
      val step = q.steps(stepIdx)

      val nextStepOpt = if (stepIdx < q.steps.size - 1) Option(q.steps(stepIdx + 1)) else None

      val excludeLabelWithDirSet = new util.HashSet[(Int, Int)]
      val includeLabelWithDirSet = new util.HashSet[(Int, Int)]
      step.queryParams.filter(_.exclude).foreach(l => excludeLabelWithDirSet.add(l.labelWithDir.labelId -> l.labelWithDir.dir))
      step.queryParams.filter(_.include).foreach(l => includeLabelWithDirSet.add(l.labelWithDir.labelId -> l.labelWithDir.dir))

      val edgesToExclude = new util.HashSet[FilterHashKey]()
      val edgesToInclude = new util.HashSet[FilterHashKey]()

      val queryParamResultLs = new ListBuffer[Result]
      queryResultLs.foreach { queryResult =>

        val duplicateEdges = new util.concurrent.ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]]()
        val resultEdges = new util.concurrent.ConcurrentHashMap[HashKey, (FilterHashKey, Edge, Double)]()
        val edgeWithScoreSorted = new ListBuffer[(HashKey, FilterHashKey, Edge, Double)]
        val labelWeight = step.labelWeights.getOrElse(queryResult.queryParam.labelWithDir.labelId, 1.0)

        // store degree value with Array.empty so if degree edge exist, it comes at very first.
        def checkDegree() = queryResult.edgeWithScoreLs.headOption.map { edgeWithScore =>
          edgeWithScore._1.propsWithTs.containsKey(LabelMeta.degreeSeq)
        }.getOrElse(false)
        var isDegree = checkDegree()

        val includeExcludeKey = queryResult.queryParam.labelWithDir.labelId -> queryResult.queryParam.labelWithDir.dir
        val shouldBeExcluded = excludeLabelWithDirSet.contains(includeExcludeKey)
        val shouldBeIncluded = includeLabelWithDirSet.contains(includeExcludeKey)


        queryResultWithFilter(queryResult).foreach { case (edge, score) =>
          if (queryResult.queryParam.transformer.isDefault) {
            val convertedEdge = edge

            val (hashKey, filterHashKey) = toHashKey(queryResult.queryParam, convertedEdge, isDegree)

            /** check if this edge should be exlcuded. */
            if (shouldBeExcluded && !isDegree) {
              edgesToExclude.add(filterHashKey)
            } else {
              if (shouldBeIncluded && !isDegree) {
                edgesToInclude.add(filterHashKey)
              }
              val tsVal = processTimeDecay(queryResult.queryParam, convertedEdge)
              val newScore = labelWeight * score * tsVal
              aggregateScore(newScore, resultEdges, duplicateEdges, edgeWithScoreSorted, hashKey, filterHashKey, queryResult.queryParam, convertedEdge)
            }
          } else {
            convertEdges(queryResult.queryParam, edge, nextStepOpt).foreach { convertedEdge =>
              val (hashKey, filterHashKey) = toHashKey(queryResult.queryParam, convertedEdge, isDegree)

              /** check if this edge should be exlcuded. */
              if (shouldBeExcluded) {
                edgesToExclude.add(filterHashKey)
              } else {
                if (shouldBeIncluded) {
                  edgesToInclude.add(filterHashKey)
                }
                val tsVal = processTimeDecay(queryResult.queryParam, convertedEdge)
                val newScore = labelWeight * score * tsVal
                aggregateScore(newScore, resultEdges, duplicateEdges, edgeWithScoreSorted, hashKey, filterHashKey, queryResult.queryParam, convertedEdge)
              }
            }
          }
          isDegree = false
        }
        val ret = (duplicateEdges, resultEdges, edgeWithScoreSorted)
        queryParamResultLs.append(ret)
      }

      val aggregatedResults = for {
        (queryResult, queryParamResult) <- queryResultLs.zip(queryParamResultLs)
      } yield {
          aggregateResults(queryResult, queryParamResult, edgesToInclude, edgesToExclude)
        }

      aggregatedResults
    }
  }


  private def logMap[K, V](h: ConcurrentHashMap[K, V]) = {
    for {
      e <- h.entrySet()
    } {
      logger.error(s"${e.getKey} -> ${e.getValue}")
    }
  }

  def getVerticesAsync(vertices: Seq[Vertex]): Future[Seq[Vertex]] = {
    implicit val ex = executionContext

    val futures = vertices.map { vertex =>
      val client = getClient(vertex.hbaseZkAddr)
      val get = vertex.buildGet
      //      get.setTimeout(this.singleGetTimeout.toShort)
      get.setFailfast(true)
      get.maxVersions(1)

      val cacheKey = MurmurHash3.stringHash(get.toString)
      val cacheVal = vertexCache.getIfPresent(cacheKey)
      if (cacheVal == null)
        deferredToFuture(client.get(get))(emptyKVs).map { kvs =>
          Vertex(QueryParam.Empty, kvs, vertex.serviceColumn.schemaVersion)
        }
      else Future.successful(cacheVal)
    }
    Future.sequence(futures).map { result => result.toList.flatten }
  }

  /**
   *
   * @param edgeWriter
   * @return
   */
  def fetchInvertedAsync(edgeWriter: EdgeWriter): Future[(QueryParam, Option[Edge])] = {
    implicit val ex = this.executionContext
    val edge = edgeWriter.edge
    val labelWithDir = edgeWriter.labelWithDir
    val queryParam = QueryParam(labelWithDir)

    getEdge(edge.srcVertex, edge.tgtVertex, queryParam, isInnerCall = true).map { queryResult =>
      (queryParam, queryResult.edgeWithScoreLs.headOption.map { case (e, _) => e })
    }
  }

  /**
   *
   * @param edgeWriter
   * @param snapshotEdgeOpt
   * @return
   */
  def commitPending(edgeWriter: EdgeWriter)(snapshotEdgeOpt: Option[Edge]): Future[Boolean] = {
    implicit val ex = this.executionContext
    val edge = edgeWriter.edge
    val label = edgeWriter.label
    val labelWithDir = edgeWriter.labelWithDir
    val pendingEdges =
      if (snapshotEdgeOpt.isEmpty || snapshotEdgeOpt.get.pendingEdgeOpt.isEmpty) Nil
      else Seq(snapshotEdgeOpt.get.pendingEdgeOpt.get)

    if (pendingEdges == Nil) Future.successful(true)
    else {
      val snapshotEdge = snapshotEdgeOpt.get
      // 1. commitPendingEdges
      // after: state without pending edges
      // before: state with pending edges

      val after = snapshotEdge.toInvertedEdgeHashLike.withNoPendingEdge().buildPutAsync().head.asInstanceOf[PutRequest]
      val before = snapshotEdge.toInvertedEdgeHashLike.valueBytes
      val client = Graph.getClient(label.hbaseZkAddr)

      for {
        pendingEdgesLock <- mutateEdges(pendingEdges, withWait = true)
        ret <- if (pendingEdgesLock.forall(identity)) Graph.deferredToFutureWithoutFallback(client.compareAndSet(after, before)).map(_.booleanValue())
        else Future.successful(false)
      } yield ret
    }
  }

  /**
   *
   * @param edgeWriter
   * @param snapshotEdgeOpt
   * @param edgeUpdate
   * @param retryNum
   * @return
   */
  def commitUpdate(edgeWriter: EdgeWriter)(snapshotEdgeOpt: Option[Edge], edgeUpdate: EdgeUpdate, retryNum: Int): Future[Boolean] = {
    implicit val ex = this.executionContext
    val edge = edgeWriter.edge
    val label = edgeWriter.label
    val client = Graph.getClient(label.hbaseZkAddr)
    if (edgeUpdate.newInvertedEdge.isEmpty) Future.successful(true)
    else {
      val lock = edgeUpdate.newInvertedEdge.get.withPendingEdge(Option(edge)).buildPutAsync().head.asInstanceOf[PutRequest]
      val before = snapshotEdgeOpt.map(old => old.toInvertedEdgeHashLike.valueBytes).getOrElse(Array.empty[Byte])
      val after = edgeUpdate.newInvertedEdge.get.withNoPendingEdge().buildPutAsync().head.asInstanceOf[PutRequest]

      def indexedEdgeMutationFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else Graph.writeAsyncWithWait(label.hbaseZkAddr, Seq(edgeUpdate.indexedEdgeMutations)).map { indexedEdgesUpdated =>
          indexedEdgesUpdated.forall(identity)
        }
      }
      def indexedEdgeIncrementFuture(predicate: Boolean): Future[Boolean] = {
        if (!predicate) Future.successful(false)
        else Graph.writeAsyncWithWaitRetry(label.hbaseZkAddr, Seq(edgeUpdate.increments), 0).map { rets =>
          val allSuccess = rets.forall(identity)
          if (!allSuccess) logger.error(s"indexedEdgeIncrement failed: $edgeUpdate")
          else logger.debug(s"indexedEdgeIncrement success: $edgeUpdate")
          allSuccess
        }
      }
      val fallback = Future.successful(false)
      val javaFallback = Future.successful[java.lang.Boolean](false)

      /**
       * step 1. acquire lock on snapshot edge.
       * step 2. try mutate indexed Edge mutation. note that increment is seperated for retry cases.
       * step 3. once all mutate on indexed edge success, then try release lock.
       * step 4. once lock is releaseed successfully, then mutate increment on this edgeUpdate.
       * note thta step 4 never fail to avoid multiple increments.
       */
      for {
        locked <- Graph.deferredToFutureWithoutFallback(client.compareAndSet(lock, before))
        indexEdgesUpdated <- indexedEdgeMutationFuture(locked)
        releaseLock <- if (indexEdgesUpdated) Graph.deferredToFutureWithoutFallback(client.compareAndSet(after, lock.value())) else javaFallback
        indexEdgesIncremented <- if (releaseLock) indexedEdgeIncrementFuture(releaseLock) else fallback
      } yield indexEdgesIncremented
    }
  }

  /**
   *
   * @param edgeWriter
   * @param f
   * @param tryNum
   * @return
   */
  def mutateEdgeInner(edgeWriter: EdgeWriter,
                      checkConsistency: Boolean,
                      withWait: Boolean)(f: (Option[Edge], Edge) => EdgeUpdate, tryNum: Int = 0): Future[Boolean] = {
    implicit val ex = this.executionContext
    val edge = edgeWriter.edge
    val zkQuorum = edge.label.hbaseZkAddr
    if (!checkConsistency) {
      val update = f(None, edge)
      val mutations = update.indexedEdgeMutations ++ update.invertedEdgeMutations ++ update.increments
      if (withWait) writeAsyncWithWaitSimple(zkQuorum, mutations)
      else writeAsyncSimple(zkQuorum, mutations)
    } else {
      if (tryNum >= MaxRetryNum) {
        logger.error(s"mutate failed after $tryNum retry, $this")
        ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = edge))
        Future.successful(false)
      } else {
        val waitTime = Random.nextInt(Graph.MaxBackOff) + 1

        fetchInvertedAsync(edgeWriter).flatMap { case (queryParam, edges) =>
          val snapshotEdgeOpt = edges.headOption
          val edgeUpdate = f(snapshotEdgeOpt, edge)

          /** if there is no changes to be mutate, then just return true */
          if (edgeUpdate.newInvertedEdge.isEmpty) Future.successful(true)
          else
            commitPending(edgeWriter)(snapshotEdgeOpt).flatMap { case pendingAllCommitted =>
              if (pendingAllCommitted) {
                commitUpdate(edgeWriter)(snapshotEdgeOpt, edgeUpdate, tryNum).flatMap { case updateCommitted =>
                  if (!updateCommitted) {
                    Thread.sleep(waitTime)
                    logger.info(s"mutate failed. retry $edge")
                    mutateEdgeInner(edgeWriter, checkConsistency, withWait = true)(f, tryNum + 1)
                  } else {
                    logger.debug(s"mutate success: ${edgeUpdate.toLogString}\n$edge")
                    Future.successful(true)
                  }
                }
              } else {
                Thread.sleep(waitTime)
                mutateEdgeInner(edgeWriter, checkConsistency, withWait = true)(f, tryNum + 1)
              }
            }

        }
      }
    }
  }

  /** TODO: deleteAll is seperated from mutateEdge. */
  /**
   *
   * @param edge
   * @param withWait
   * @return
   */
  def mutateEdgeWithOp(edge: Edge, withWait: Boolean = false): Future[Boolean] = {
    val edgeWriter = EdgeWriter(edge)
    val zkQuorum = edge.label.hbaseZkAddr
    val rpcLs = new ListBuffer[HBaseRpc]()
    // all cases, it is necessary to insert vertex.
    rpcLs.appendAll(edgeWriter.buildVertexPutsAsync())

    val vertexMutateFuture =
      if (withWait) writeAsyncWithWaitSimple(zkQuorum, rpcLs)
      else writeAsyncSimple(zkQuorum, rpcLs)


    val edgeMutateFuture = edge.op match {
      case op if op == GraphUtil.operations("insert") =>
        edge.label.consistencyLevel match {
          case "strong" => // upsert
            mutateEdgeInner(edgeWriter, checkConsistency = true, withWait = withWait)(Edge.buildUpsert)
          case _ => // insert
            mutateEdgeInner(edgeWriter, checkConsistency = false, withWait = withWait)(Edge.buildInsertBulk)
        }

      case op if op == GraphUtil.operations("delete") =>
        edge.label.consistencyLevel match {
          case "strong" => // delete
            mutateEdgeInner(edgeWriter, checkConsistency = true, withWait = withWait)(Edge.buildDelete)
          case _ => // deleteBulk
            mutateEdgeInner(edgeWriter, checkConsistency = false, withWait = withWait)(Edge.buildDeleteBulk)

        }

      case op if op == GraphUtil.operations("update") =>
        mutateEdgeInner(edgeWriter, checkConsistency = true, withWait = withWait)(Edge.buildUpdate)

      case op if op == GraphUtil.operations("increment") =>
        mutateEdgeInner(edgeWriter, checkConsistency = true, withWait = withWait)(Edge.buildIncrement)

      case op if op == GraphUtil.operations("insertBulk") =>
        mutateEdgeInner(edgeWriter, checkConsistency =  false, withWait = withWait)(Edge.buildInsertBulk)
//        rpcLs.appendAll(edgeWriter.insert(createRelEdges = true))

      case _ =>
        logger.error(s"not supported operation on edge: ${edge.op}, $edge")
        throw new RuntimeException(s"operation ${edge.op} is not supported on edge.")
    }

    edgeMutateFuture
  }

  /**
   *
   * @param edge
   * @param withWait
   * @return
   */
  def mutateEdge(edge: Edge, withWait: Boolean = false): Future[Boolean] = {
    implicit val ex = this.executionContext
    mutateEdgeWithOp(edge, withWait)
  }

  /**
   *
   * @param edges
   * @param withWait
   * @return
   */
  def mutateEdges(edges: Seq[Edge], withWait: Boolean = false): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    val futures = edges.map { edge => mutateEdge(edge, withWait) }

    Future.sequence(futures)
  }

  /**
   *
   * @param vertex
   * @param withWait
   * @return
   */
  def mutateVertex(vertex: Vertex, withWait: Boolean = false): Future[Boolean] = {
    implicit val ex = this.executionContext
    if (vertex.op == GraphUtil.operations("delete")) {
      deleteVertex(vertex, withWait)
    } else if (vertex.op == GraphUtil.operations("deleteAll")) {
      logger.info(s"deleteAll for vertex is truncated. $vertex")
      Future.successful(true) // Ignore withWait parameter, because deleteAll operation may takes long time
    } else {
      if (withWait)
        writeAsyncWithWaitSimple(vertex.hbaseZkAddr, vertex.buildPutsAll())
      else
        writeAsyncSimple(vertex.hbaseZkAddr, vertex.buildPutsAll())
    }
  }

  /**
   *
   * @param vertices
   * @param withWait
   * @return
   */
  def mutateVertices(vertices: Seq[Vertex], withWait: Boolean = false): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    val futures = vertices.map { vertex => mutateVertex(vertex, withWait) }
    Future.sequence(futures)
  }

  /**
   *
   * @param vertex
   * @param withWait
   * @return
   */
  private def deleteVertex(vertex: Vertex, withWait: Boolean = false): Future[Boolean] = {
    implicit val ex = this.executionContext

    if (withWait)
      writeAsyncWithWait(vertex.hbaseZkAddr, Seq(vertex).map(_.buildDeleteAsync())).map(_.forall(identity))
    else
      writeAsync(vertex.hbaseZkAddr, Seq(vertex).map(_.buildDeleteAsync())).map(_.forall(identity))
  }

  /**
   *
   * @param vertices
   * @return
   */
  private def deleteVertices(vertices: Seq[Vertex]): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    val futures = vertices.map { vertex => deleteVertex(vertex) }
    Future.sequence(futures)
  }

  /**
   * by iterating on fetched edges and build future list first.
   * each future in future list will be responsible for delete in/out direction edge for single edge.
   * decrement degrees only if all of futures in future list success, otherwise retry.
   * @param queryResult
   * @param requestTs
   * @param retryNum
   * @param walTopic
   * @return
   */
  def deleteAllFetchedEdgesAsync(queryResult: QueryResult,
                                 requestTs: Long,
                                 retryNum: Int = 0,
                                 walTopic: String): Future[Boolean] = {
    implicit val ex = Graph.executionContext
    val queryParam = queryResult.queryParam
//    val size = queryResult.edgeWithScoreLs.size
    val size = queryResult.sizeWithoutDegreeEdge()
    if (retryNum > MaxRetryNum) {
      queryResult.edgeWithScoreLs.foreach { case (edge, score) =>
        val copiedEdge = edge.copy(op = GraphUtil.operations("delete"), ts = requestTs, version = requestTs)
        logger.error(s"deleteAll failed: $copiedEdge")
        ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = copiedEdge))
      }
      Future.successful(false)
    } else {
      val futures: Seq[Future[Boolean]] =
        for {
          (edge, score) <- queryResult.edgeWithScoreLs
          duplicateEdge = edge.duplicateEdge.copy(op = GraphUtil.operations("delete"))
          //        version = edge.version + Edge.incrementVersion // this lead to forcing delete on fetched edges
          version = requestTs
          copiedEdge = edge.copy(op = GraphUtil.operations("delete"), ts = requestTs, version = version)
          hbaseZkAddr = queryResult.queryParam.label.hbaseZkAddr
        } yield {
          if (retryNum == 0)
            ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(topic = walTopic, element = copiedEdge))

          logger.debug(s"FetchedEdge: $edge")
          logger.debug(s"DeleteEdge: $duplicateEdge")

          val indexedEdgesDeletes = if (edge.ts < requestTs) duplicateEdge.edgesWithIndex.flatMap { indexedEdge =>
            val delete = indexedEdge.buildDeletesAsync()
            logger.debug(s"indexedEdgeDelete: $delete")
            delete
          } else Nil

          val snapshotEdgeDelete =
            if (edge.ts < requestTs) duplicateEdge.toInvertedEdgeHashLike.buildDeleteAsync()
            else Nil

          val copyEdgeIndexedEdgesDeletes =
            if (edge.ts < requestTs) copiedEdge.edgesWithIndex.flatMap { e => e.buildDeletesAsync() }
            else Nil

          val indexedEdgesIncrements = if (edge.ts < requestTs) duplicateEdge.edgesWithIndex.flatMap { indexedEdge =>
            val incr = indexedEdge.buildIncrementsAsync(-1L)
            logger.debug(s"indexedEdgeIncr: $incr")
            incr
          } else Nil

          val deletesForThisEdge = snapshotEdgeDelete ++ indexedEdgesDeletes ++ copyEdgeIndexedEdgesDeletes
          Graph.writeAsyncWithWait(queryParam.label.hbaseZkAddr, Seq(deletesForThisEdge)).flatMap { rets =>
            if (rets.forall(identity)) {
              Graph.writeAsyncWithWait(queryParam.label.hbaseZkAddr, Seq(indexedEdgesIncrements)).map { rets =>
                rets.forall(identity)
              }
            } else {
              Future.successful(false)
            }
          }
        }

      Future.sequence(futures).flatMap { duplicateEdgeDeletedLs =>
        val edgesToRetry = for {
          ((edge, score), duplicatedEdgeDeleted) <- queryResult.edgeWithScoreLs.zip(duplicateEdgeDeletedLs)
          if !duplicatedEdgeDeleted
        } yield (edge, score)
        val deletedEdgesNum = size - edgesToRetry.size
        val queryResultToRetry = queryResult.copy(edgeWithScoreLs = edgesToRetry)
        // not sure if increment rpc itset fail, then should we retry increment also?
        if (deletedEdgesNum > 0) {
          // decrement on current queryResult`s start vertex`s degree
          val incrs = queryResult.edgeWithScoreLs.headOption.map { case (edge, score) =>
            edge.edgesWithIndex.flatMap { indexedEdge => indexedEdge.buildIncrementsAsync(-1 * deletedEdgesNum) }
          }.getOrElse(Nil)
          Graph.writeAsyncWithWaitRetry(queryParam.label.hbaseZkAddr, Seq(incrs), 0).map { rets =>
            if (!rets.forall(identity)) logger.error(s"decrement for deleteAll failed. $incrs")
            else logger.debug(s"decrement for deleteAll successs. $incrs")
            rets
          }
        }
        if (edgesToRetry.isEmpty) {
          Future.successful(true)
        } else {
          deleteAllFetchedEdgesAsync(queryResultToRetry, requestTs, retryNum + 1, walTopic)
        }
      }
    }
  }

  /**
   *
   * @param queryResultLs
   * @param requestTs
   * @param retryNum
   * @param walTopic
   * @return
   */
  def deleteAllFetchedEdgesLs(queryResultLs: Seq[QueryResult], requestTs: Long,
                              retryNum: Int = 0, walTopic: String): Future[Boolean] = {
    implicit val ex = Graph.executionContext
    if (retryNum > MaxRetryNum) {
      logger.error(s"deleteDuplicateEdgesLs failed. ${queryResultLs}")
      Future.successful(false)
    } else {
      val futures = for {
        queryResult <- queryResultLs
      } yield {
          deleteAllFetchedEdgesAsync(queryResult, requestTs, 0, walTopic)
        }
      Future.sequence(futures).flatMap { rets =>
        val allSuccess = rets.forall(identity)
        if (!allSuccess) deleteAllFetchedEdgesLs(queryResultLs, requestTs, retryNum + 1, walTopic)
        else Future.successful(allSuccess)
      }
    }
  }

  /**
   *
   * @param srcVertices
   * @param labels
   * @param dir
   * @param ts
   * @param walTopic
   * @return
   */
  def deleteAllAdjacentEdgesAsync(srcVertices: List[Vertex],
                                  labels: Seq[Label],
                                  dir: Int,
                                  ts: Option[Long] = None,
                                  walTopic: String): Future[Boolean] = {
    implicit val ex = Graph.executionContext
    val requestTs = ts.getOrElse(System.currentTimeMillis())
    val queryParams = for {
      label <- labels
    } yield {
        val labelWithDir = LabelWithDirection(label.id.get, dir)
        QueryParam(labelWithDir).limit(0, maxValidEdgeListSize * 5).duplicatePolicy(Option(Query.DuplicatePolicy.Raw))
      }

    val step = Step(queryParams.toList)
    val q = Query(srcVertices, Vector(step), false)

    for {
      queryResultLs <- getEdgesAsync(q)
      ret <- deleteAllFetchedEdgesLs(queryResultLs, requestTs, 0, walTopic)
    } yield ret
  }

  /**
   *
   * @param elements
   * @return
   */
  def mutateElements(elements: Seq[GraphElement], withWait: Boolean = false): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    val futures = elements.map { element =>
      element match {
        case edge: Edge => mutateEdge(edge, withWait)
        case vertex: Vertex => mutateVertex(vertex, withWait)
        case _ => throw new RuntimeException(s"$element is not edge/vertex")
      }
    }
    Future.sequence(futures)
  }


  // select
  def getVertex(vertex: Vertex): Future[Option[Vertex]] = {
    implicit val ex = executionContext
    val client = getClient(vertex.hbaseZkAddr)
    deferredToFuture(client.get(vertex.buildGet))(emptyKVs).map { kvs =>
      Vertex(QueryParam.Empty, kvs, vertex.serviceColumn.schemaVersion)
    }
  }

  def toGraphElement(s: String, labelMapping: Map[String, String] = Map.empty): Option[GraphElement] = Try {
    val parts = GraphUtil.split(s)
    val logType = parts(2)
    val element = if (logType == "edge" | logType == "e") {
      /** current only edge is considered to be bulk loaded */
      labelMapping.get(parts(5)) match {
        case None =>
        case Some(toReplace) =>
          parts(5) = toReplace
      }
      toEdge(parts)
    } else if (logType == "vertex" | logType == "v") {
      toVertex(parts)
    } else {
      throw new GraphExceptions.JsonParseException("log type is not exist in log.")
    }

    element
  } recover {
    case e: Exception =>
      logger.error(s"$e", e)
      None
  } get


  def toVertex(s: String): Option[Vertex] = {
    toVertex(GraphUtil.split(s))
  }

  def toEdge(s: String): Option[Edge] = {
    toEdge(GraphUtil.split(s))
  }

  //"1418342849000\tu\te\t3286249\t71770\ttalk_friend\t{\"is_hidden\":false}"
  //{"from":1,"to":101,"label":"graph_test","props":{"time":-1, "weight":10},"timestamp":1417616431},
  def toEdge(parts: Array[String]): Option[Edge] = Try {
    val (ts, operation, logType, srcId, tgtId, label) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) parts(6) else "{}"
    val tempDirection = if (parts.length >= 8) parts(7) else "out"
    val direction = if (tempDirection != "out" && tempDirection != "in") "out" else tempDirection

    val edge = Management.toEdge(ts.toLong, operation, srcId, tgtId, label, direction, props)
    //            logger.debug(s"toEdge: $edge")
    Some(edge)
  } recover {
    case e: Exception =>
      logger.error(s"toEdge: $e", e)
      throw e
  } get

  //"1418342850000\ti\tv\t168756793\ttalk_user_id\t{\"country_iso\":\"KR\"}"
  def toVertex(parts: Array[String]): Option[Vertex] = Try {
    val (ts, operation, logType, srcId, serviceName, colName) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) parts(6) else "{}"
    Some(Management.toVertex(ts.toLong, operation, srcId, serviceName, colName, props))
  } recover {
    case e: Throwable =>
      logger.error(s"toVertex: $e", e)
      throw e
  } get
}
