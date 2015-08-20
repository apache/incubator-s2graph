package com.daumkakao.s2graph.core

import java.util
import com.daumkakao.s2graph.core.mysqls._
import com.google.common.cache.CacheBuilder
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success}
import java.util.ArrayList
import java.util.concurrent.{ConcurrentHashMap, Executors}
import com.daumkakao.s2graph.core.types2._
import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.hbase.async._
import play.api.Logger
import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.concurrent._
import scala.concurrent.duration._


object Graph {

  val vertexCf = "v".getBytes()
  val edgeCf = "e".getBytes()
  val updateCf = "u".getBytes()
  val ttsForActivity = 60 * 60 * 24 * 30
  val delimiter = "|"
  val seperator = ":"

  val writeBufferSize = 1024 * 1024 * 2

  val maxValidEdgeListSize = 10000
  //  val Logger = Edge.Logger
  val conns = scala.collection.mutable.Map[String, Connection]()
  val clients = scala.collection.mutable.Map[String, HBaseClient]()
  val emptyKVs = new ArrayList[KeyValue]()
  val emptyKVlist = new ArrayList[ArrayList[KeyValue]]()

  //  var shouldRunFromBytes = true
  //  var shouldReturnResults = true
  //  var shouldRunFetch = true
  //  var shouldRunFilter = true
  val defaultConfigs: Map[String, AnyRef] = Map(
    "hbase.zookeeper.quorum" -> "localhost",
    "hbase.table.name" -> "s2graph",
    "hbase.table.compression.algorithm" -> "gz",
    "phase" -> "dev",
    "async.hbase.client.flush.interval" -> java.lang.Short.valueOf(100.toShort),
    "hbase.client.operation.timeout" -> java.lang.Integer.valueOf(1000),
    "db.default.driver" -> "com.mysql.jdbc.Driver",
    "db.default.url" -> "jdbc:mysql://localhost:3306/graph_dev",
    "db.default.password" -> "graph",
    "db.default.user" -> "graph",
    "cache.max.size" -> java.lang.Integer.valueOf(100000),
    "cache.ttl.seconds" -> java.lang.Integer.valueOf(60))

  var config: Config = ConfigFactory.parseMap(defaultConfigs)
  var executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  var hbaseConfig: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
  var storageExceptionCount = 0L
  var singleGetTimeout = 1000
  var clientFlushInterval = 100.toShort
  val defaultScore = 1.0

  lazy val cache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build[java.lang.Integer, QueryResult]()

  lazy val vertexCache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build[java.lang.Integer, Option[Vertex]]()

  /**
   * requred: hbase.zookeeper.quorum
   * optional: all hbase. prefix configurations.
   */
  private def toHBaseConfig(config: com.typesafe.config.Config) = {
    val conf = HBaseConfiguration.create()

    for  {
      (k, v) <- defaultConfigs if !config.hasPath(k)
    } {
      conf.set(k, v.toString())
    }

    for (entry <- config.entrySet() if entry.getKey().contains("hbase")) {
      conf.set(entry.getKey(), entry.getValue().unwrapped().toString)
    }

    conf
  }

  //  implicit val ex = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))
  def apply(config: com.typesafe.config.Config)(implicit ex: ExecutionContext) = {

    this.config = config.withFallback(this.config)
    this.hbaseConfig = toHBaseConfig(this.config)

    Model(this.config)

    this.executionContext = ex
    this.singleGetTimeout = this.config.getInt("hbase.client.operation.timeout")
    this.clientFlushInterval = this.config.getInt("async.hbase.client.flush.interval").toShort
    val zkQuorum = hbaseConfig.get("hbase.zookeeper.quorum")
    clients += (zkQuorum -> getClient(zkQuorum, this.clientFlushInterval))
    ExceptionHandler.apply(config)
    for {
      (k, v) <- defaultConfigs
    } {
      Logger.info(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")
    }
  }


  def getClient(zkQuorum: String, flushInterval: Short = clientFlushInterval) = {
    val client = clients.get(zkQuorum) match {
      case None =>
        val client = new HBaseClient(zkQuorum)
        client.setFlushInterval(clientFlushInterval)
        clients += (zkQuorum -> client)
        client
      //        throw new RuntimeException(s"connection to $zkQuorum is not established.")
      case Some(c) => c
    }
    client.setFlushInterval(flushInterval)
    client
  }

  def getConn(zkQuorum: String) = {
    conns.get(zkQuorum) match {
      case None =>
        Logger.debug(s"${this.hbaseConfig}")
        val conn = ConnectionFactory.createConnection(this.hbaseConfig)
        conns += (zkQuorum -> conn)
        conn
      //        throw new RuntimeException(s"connection to $zkQuorum is not established.")
      case Some(c) => c
    }
  }

  def defferedToFuture[A](d: Deferred[A])(fallback: A): Future[A] = {
    val promise = Promise[A]

    d.addBoth(new Callback[Unit, A] {
      def call(arg: A) = arg match {
        case e: Throwable =>
          Logger.error(s"deferred return throwable: $e", e)
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
        case e: Throwable =>
          Logger.error(s"deferred return throwable: $e", e)
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
        Logger.error(s"Exception on deferred: $e", e)
        fallback
      }
    })
  }


  def writeAsyncWithWait(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]]): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    if (elementRpcs.isEmpty) {
      Future.successful(Seq.empty[Boolean])
    } else {
      val client = getClient(zkQuorum)
      val defers = elementRpcs.map { rpcs =>
        //TODO: register errorBacks on this operations to log error
        //          Logger.debug(s"$rpc")
        val defer = rpcs.map { rpc =>
          //          Logger.debug(s"$rpc")
          val deferred = rpc match {
            case d: DeleteRequest => client.delete(d)
            case p: PutRequest => client.put(p)
            case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i)
          }
          deferredCallbackWithFallback(deferred)({
            (anyRef: Any) => anyRef match {
              case e: Exception =>
                Logger.error(s"mutation failed. $e", e)
                false
              case _ => true
            }
          }, {
            false
          })
        }
        val ret = deferredToFutureWithoutFallback(Deferred.group(defer)).map { arr => arr.forall(identity) }
        ret
      }
      Future.sequence(defers)
    }
  }


  def writeAsync(zkQuorum: String, elementRpcs: Seq[Seq[HBaseRpc]]): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    val errorLogger = Logger("error")
    if (elementRpcs.isEmpty) {
      Future.successful(Seq.empty[Boolean])
    } else {
      val client = getClient(zkQuorum)
      val defers = elementRpcs.map { rpcs =>
        //TODO: register errorBacks on this operations to log error
        //          Logger.debug(s"$rpc")
        val defer = rpcs.map { rpc =>
                    Logger.debug(s"$rpc")
          val deferred = rpc match {
            case d: DeleteRequest => client.delete(d).addErrback(new Callback[Unit, Exception] {
              def call(arg: Exception): Unit = {
                errorLogger.error(s"delete request failed. $d, $arg", arg)
              }
            })
            case p: PutRequest => client.put(p).addErrback(new Callback[Unit, Exception] {
              def call(arg: Exception): Unit = {
                errorLogger.error(s"put request failed. $p, $arg", arg)
              }
            })
            case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).addErrback(new Callback[Unit, Exception] {
              def call(arg: Exception): Unit = {
                errorLogger.error(s"increment request failed. $i, $arg", arg)
              }
            })
          }
          //          deferredCallbackWithFallback(deferred)({
          //            (anyRef: Any) => anyRef match {
          //              case e: Exception => false
          //              case _ => true
          //            }
          //          }, {
          //            false
          //          })
        }
        //        val ret = deferredToFutureWithoutFallback(Deferred.group(defer)).map { arr => arr.forall(identity) }
        //        ret
      }
      //      Future.sequence(defers)
      Future.successful(elementRpcs.map(x => true))
    }
  }

  /**
   * Edge
   */
  //  def mutateEdge(edge: Edge): Unit = {
  //    save(edge.label.hbaseZkAddr, edge.label.hbaseTableName, edge.buildPutsAll())
  //  }

  //only for testcase.
  def getEdgesSync(q: Query): Seq[QueryResult] = {
    Await.result(getEdgesAsync(q), 10 seconds)
  }

  //select

  /**
   *
   */
  def getEdgesAsync(q: Query): Future[Seq[QueryResult]] = {
    implicit val ex = this.executionContext
    // not sure this is right. make sure refactor this after.
    try {
      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        Future.successful(q.vertices.map(v => QueryResult(query = q, stepIdx = 0, queryParam = QueryParam.empty)))
      } else {
        val startQueryResultLs = QueryResult.fromVertices(q, stepIdx = 0, q.steps.head.queryParams, q.vertices)
        var seedEdgesFuture: Future[Seq[QueryResult]] = Future.successful(startQueryResultLs)
        for {
          (step, idx) <- q.steps.zipWithIndex
        } {
          seedEdgesFuture = getEdgesAsyncWithRank(seedEdgesFuture, q, idx)
        }
        seedEdgesFuture
      }
    } catch {
      case e: Throwable =>
        Logger.error(s"getEdgesAsync: $e", e)
        Future.successful(q.vertices.map(v => QueryResult(query = q, stepIdx = 0, queryParam = QueryParam.empty)))
    }
  }


  private def fetchEdgesLs(currentStepRequestLss: Seq[(Iterable[(GetRequest, QueryParam)], Double)], q: Query, stepIdx: Int): Seq[Deferred[QueryResult]] = {
    for {
      (prevStepTgtVertexResultLs, prevScore) <- currentStepRequestLss
      (getRequest, queryParam) <- prevStepTgtVertexResultLs
    } yield {
      //      fetchEdges(getRequest, queryParam, prevScore)
      fetchEdgesWithCache(getRequest, q, stepIdx, queryParam, prevScore)
    }
  }


  private def fetchEdgesWithCache(getRequest: GetRequest, q: Query, stepIdx: Int, queryParam: QueryParam, prevScore: Double): Deferred[QueryResult] = {
    val cacheKey = MurmurHash3.stringHash(getRequest.toString)
    def queryResultCallback(cacheKey: Int) = new Callback[QueryResult, QueryResult] {
      def call(arg: QueryResult): QueryResult = {
        Logger.debug(s"queryResultCachePut, $arg")
        cache.put(cacheKey, arg)
        arg
      }
    }
    if (queryParam.cacheTTLInMillis > 0) {
      val cacheTTL = queryParam.cacheTTLInMillis
      if (cache.asMap().containsKey(cacheKey)) {
        val cachedVal = cache.asMap().get(cacheKey)
        if (cachedVal != null && queryParam.timestamp - cachedVal.timestamp < cacheTTL) {
          val elapsedTime = queryParam.timestamp - cachedVal.timestamp
          Logger.debug(s"cacheHitAndValid: $cacheKey, $cacheTTL, $elapsedTime")
          Deferred.fromResult(cachedVal)
        }
        else {
          // cache.asMap().remove(cacheKey)
          Logger.debug(s"cacheHitInvalid(invalidated): $cacheKey, $cacheTTL")
          fetchEdges(getRequest, q, stepIdx, queryParam, prevScore).addBoth(queryResultCallback(cacheKey))
        }
      } else {
        Logger.debug(s"cacheMiss: $cacheKey")
        fetchEdges(getRequest, q, stepIdx, queryParam, prevScore).addBoth(queryResultCallback(cacheKey))
      }
    } else {
      Logger.debug(s"cacheMiss(no cacheTTL in QueryParam): $cacheKey")
      fetchEdges(getRequest, q, stepIdx, queryParam, prevScore)
    }
  }

  /** actual request to HBase */
  private def fetchEdges(getRequest: GetRequest, q: Query, stepIdx: Int, queryParam: QueryParam, prevScore: Double): Deferred[QueryResult] = {
    //    if (!this.shouldRunFetch) Deferred.fromResult(QueryResult(q, stepIdx, queryParam))
    //    else {
    try {
      val client = getClient(queryParam.label.hbaseZkAddr)
      deferredCallbackWithFallback(client.get(getRequest))({ kvs =>
        val edgeWithScores = Edge.toEdges(kvs, queryParam, prevScore)
        QueryResult(q, stepIdx, queryParam, new ArrayList(edgeWithScores))
      }, QueryResult(q, stepIdx, queryParam))
    } catch {
      case e@(_: Throwable | _: Exception) =>
        Logger.error(s"Exception: $e", e)
        Deferred.fromResult(QueryResult(q, stepIdx, queryParam))
    }
    //    }
  }

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
    val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.defaultThreshold)
    val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
    val step = q.steps(stepIdx)
    val alreadyVisited =
      if (stepIdx == 0) Map.empty[(LabelWithDirection, Vertex), Boolean]
      else alreadyVisitedVertices(queryResultsLs)

    //TODO:
    val groupedBy = queryResultsLs.flatMap { queryResult =>
      queryResult.edgeWithScoreLs.map { case (edge, score) =>
        (edge.tgtVertex -> score)
      }
    }.groupBy { case (vertex, score) =>
      vertex
    }

//    Logger.debug(s"groupedBy: $groupedBy")
    val groupedByFiltered = for {
      (vertex, edgesWithScore) <- groupedBy
      aggregatedScore = edgesWithScore.map(_._2).sum if aggregatedScore >= prevStepThreshold
    } yield (vertex -> aggregatedScore)
//    Logger.debug(s"groupedByFiltered: $groupedByFiltered")

    val nextStepSrcVertices = if (prevStepLimit >= 0) {
      groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
    } else {
      groupedByFiltered.toSeq
    }
//    Logger.debug(s"nextStepSrcVertices: $nextStepSrcVertices")
    val currentStepRequestLss = buildGetRequests(nextStepSrcVertices, step.queryParams)

    val queryParams = currentStepRequestLss.flatMap { case (getsWithQueryParams, prevScore) =>
      getsWithQueryParams.map { case (get, queryParam) => queryParam }
    }
    val fallback = new util.ArrayList(queryParams.map(param => QueryResult(q, stepIdx, param)))
    val deffered = fetchEdgesLs(currentStepRequestLss, q, stepIdx)
    val grouped: Deferred[util.ArrayList[QueryResult]] = Deferred.group(deffered)

    filterEdges(defferedToFuture(grouped)(fallback), q, stepIdx, alreadyVisited)
  }

  def getEdgesAsyncWithRank(queryResultLsFuture: Future[Seq[QueryResult]], q: Query, stepIdx: Int): Future[Seq[QueryResult]] = {
    implicit val ex = executionContext
    for {
      queryResultLs <- queryResultLsFuture
      //      (queryParam, edgeWithScoreLs) <- srcEdges
      // prevStep: (QueryParam, Seq[(Edge, Double)]), q: Query, stepIdx: Int): Future[Seq[(QueryParam, Iterable[(Edge, Double)])]] = {
      ret <- getEdgesAsyncWithRank(queryResultLs, q, stepIdx)
    } yield {
      ret
    }
  }

  def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam): Future[QueryResult] = {
    implicit val ex = this.executionContext


    val invertedEdge = Edge(srcVertex, tgtVertex, queryParam.labelWithDir).toInvertedEdgeHashLike()
    val getRequest = queryParam.tgtVertexInnerIdOpt(Option(invertedEdge.tgtVertex.innerId))
      .buildGetRequest(invertedEdge.srcVertex)
    val q = Query.toQuery(Seq(srcVertex), queryParam)

    defferedToFuture(getClient(queryParam.label.hbaseZkAddr).get(getRequest))(emptyKVs).map { kvs =>
      val edgeWithScoreLs = Edge.toEdges(kvs, queryParam, prevScore = 1.0)
      QueryResult(query = q, stepIdx = 0, queryParam = queryParam, edgeWithScoreLs = edgeWithScoreLs)
    }
  }

  def checkEdges(quads: Seq[(Vertex, Vertex, Label, Int)]): Future[Seq[QueryResult]] = {
    implicit val ex = this.executionContext
    val futures = for {
      (srcVertex, tgtVertex, label, dir) <- quads
      queryParam = QueryParam(LabelWithDirection(label.id.get, dir))
    } yield getEdge(srcVertex, tgtVertex, queryParam)

    Future.sequence(futures)
  }


  def buildGetRequests(startVertices: Seq[(Vertex, Double)], params: List[QueryParam]): Seq[(Iterable[(GetRequest, QueryParam)], Double)] = {
    for {
      (vertex, score) <- startVertices
    } yield {
      val requests = for {
        param <- params
      } yield {
          (param.buildGetRequest(vertex), param)
        }
      (requests, score)
    }
  }


  def convertEdges(queryParam: QueryParam, edge: Edge, nextStepOpt: Option[Step]): Seq[Edge] = {
    for {
      convertedEdge <- queryParam.transformer.transform(edge, nextStepOpt)
    } yield convertedEdge
  }


  type HashKey = (Int, Int, Int, Int)
  type FilterHashKey = (Int, Int)

  def toHashKey(queryParam: QueryParam, edge: Edge): (HashKey, FilterHashKey) = {
    val src = edge.srcVertex.innerId.hashKey(queryParam.srcColumnWithDir.columnType)
    val tgt = edge.tgtVertex.innerId.hashKey(queryParam.tgtColumnWithDir.columnType)
    val hashKey = (src, edge.labelWithDir.labelId, edge.labelWithDir.dir, tgt)
    val filterHashKey = (src, tgt)
    (hashKey, filterHashKey)
  }

  def filterEdges(queryResultLsFuture: Future[ArrayList[QueryResult]],
                  q: Query,
                  stepIdx: Int,
                  alreadyVisited: Map[(LabelWithDirection, Vertex), Boolean] =
                  Map.empty[(LabelWithDirection, Vertex), Boolean]): Future[Seq[QueryResult]] = {
    implicit val ex = Graph.executionContext
    queryResultLsFuture.map { queryResultLs =>
      val step = q.steps(stepIdx)

      val nextStepOpt = if (stepIdx < q.steps.size - 1) Option(q.steps(stepIdx + 1)) else None

//      val labelOutputFields = step.queryParams.map { qParam =>
//        qParam.labelWithDir.labelId -> qParam.outputFields
//      }.toMap

      val excludeLabelWithDirSet = step.queryParams.filter(_.exclude).map(l => l.labelWithDir.labelId -> l.labelWithDir.dir).toSet
      val includeLabelWithDirSet = step.queryParams.filter(_.include).map(l => l.labelWithDir.labelId -> l.labelWithDir.dir).toSet



      val edgesToExclude = new util.concurrent.ConcurrentHashMap[FilterHashKey, Boolean]()
      val edgesToInclude = new util.concurrent.ConcurrentHashMap[FilterHashKey, Boolean]()

      val queryParamResultLs = for {
        queryResult <- queryResultLs
      } yield {
          val duplicateEdges = new util.concurrent.ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]]()
          val resultEdgeWithScores = new util.concurrent.ConcurrentHashMap[HashKey, (HashKey, FilterHashKey, Edge, Double)]()
          val labelWeight = step.labelWeights.get(queryResult.queryParam.labelWithDir.labelId).getOrElse(1.0)
          for {
            (edge, score) <- queryResult.edgeWithScoreLs
//            outputFields <- labelOutputFields.get(edge.labelWithDir.labelId)
            convertedEdge <- convertEdges(queryResult.queryParam, edge, nextStepOpt)
//            convertedEdge <- convertEdges(edge, labelOutputFields(edge.labelWithDir.labelId))
            (hashKey, filterHashKey) = toHashKey(queryResult.queryParam, convertedEdge)
          } {
//            Logger.error(s"filterEdge: $edge")
            /** check if this edge should be exlcuded. */
            val filterKey = edge.labelWithDir.labelId -> edge.labelWithDir.dir
            if (excludeLabelWithDirSet.contains(filterKey) && !edge.propsWithTs.containsKey(LabelMeta.degreeSeq)) {
              edgesToExclude.put(filterHashKey, true)
            } else {
              /** include should be aggregated into score */
              if (includeLabelWithDirSet.contains(filterKey) && !edge.propsWithTs.containsKey(LabelMeta.degreeSeq)) {
                edgesToInclude.put(filterHashKey, true)
              }
              /** process time decay */
              val tsVal = queryResult.queryParam.timeDecay match {
                case None => 1.0
                case Some(timeDecay) =>
                  val timeDiff = queryResult.queryParam.timestamp - edge.ts
                  timeDecay.decay(timeDiff)
              }

              val newScore = labelWeight * score * tsVal

              /** aggregate score into result. note that this is only aggregate in queryParam scope */
              if (resultEdgeWithScores.containsKey(hashKey)) {
                val (oldHashKey, oldFilterHashKey, oldEdge, oldScore) = resultEdgeWithScores.get(hashKey)
                //TODO:
                queryResult.queryParam.duplicatePolicy match {
                  case Query.DuplicatePolicy.First => // do nothing
                  case Query.DuplicatePolicy.Raw =>
                    if (duplicateEdges.containsKey(hashKey)) {
                      duplicateEdges.get(hashKey) += (convertedEdge -> newScore)
                    } else {
                      val newBuffer = new ListBuffer[(Edge, Double)]
                      newBuffer += (convertedEdge -> newScore)
                      duplicateEdges.put(hashKey, newBuffer)
                    }
                  case Query.DuplicatePolicy.CountSum =>
                    resultEdgeWithScores.put(hashKey, (hashKey, filterHashKey, oldEdge, oldScore + 1))
                  case _ =>
                    resultEdgeWithScores.put(hashKey, (hashKey, filterHashKey, oldEdge, oldScore + newScore))
                }
              } else {
                resultEdgeWithScores.put(hashKey, (hashKey, filterHashKey, convertedEdge, newScore))
              }
            }
          }
//          logMap(duplicateEdges)
//          logMap(resultEdgeWithScores)
          (duplicateEdges, resultEdgeWithScores)
        }

      val aggregatedResults = for {
        (queryResult, queryParamResult) <- queryResultLs.zip(queryParamResultLs)
        (duplicateEdges, resultEdgeWithScores) = queryParamResult
      } yield {
        val edgesWithScores = for {
          (hashKey, filterHashKey, edge, score) <- resultEdgeWithScores.values if edgesToInclude.containsKey(filterHashKey) || !edgesToExclude.containsKey(filterHashKey)
          (duplicateEdge, aggregatedScore) <- (edge -> score) +: (if (duplicateEdges.containsKey(hashKey)) duplicateEdges.get(hashKey) else Seq.empty)
          if aggregatedScore >= queryResult.queryParam.threshold
        } yield {
//            Logger.error(s"remainEdge: $duplicateEdge")
            (duplicateEdge, aggregatedScore)
          }

        QueryResult(queryResult.query, queryResult.stepIdx, queryResult.queryParam, edgesWithScores)
      }
      aggregatedResults
    }
  }

  private def logMap[K, V](h: ConcurrentHashMap[K, V]) = {
    for {
      e <- h.entrySet()
    } {
      Logger.error(s"${e.getKey} -> ${e.getValue}")
    }
  }
  private def filterDuplicates(seen: HashMap[(String, Int, Int, String), Double], queryParam: QueryParam,
                               edge: Edge, score: Double) = {
    val key = (edge.srcVertex.innerId.toString, edge.labelWithDir.labelId, edge.labelWithDir.dir, edge.tgtVertex.innerId.toString)
    val newScore = queryParam.duplicatePolicy match {
      case Query.DuplicatePolicy.CountSum => 1.0
      case _ => score
    }
    seen.get(key) match {
      case None =>
        seen += (key -> newScore)
        true
      case Some(oldScore) =>

        queryParam.duplicatePolicy match {
          case Query.DuplicatePolicy.First =>
            // use first occurrence`s score
            false
          case Query.DuplicatePolicy.Raw =>
            // TODO: assumes all duplicate vertices will have same score
            seen += (key -> newScore)
            true
          case _ =>
            // aggregate score
            seen += (key -> (oldScore + newScore))
            false
        }
    }
  }


  /**
   * Vertex
   */

  def getVerticesAsync(vertices: Seq[Vertex]): Future[Seq[Vertex]] = {
    implicit val ex = executionContext

    val futures = vertices.map { vertex =>
      val client = getClient(vertex.hbaseZkAddr)
      val get = vertex.buildGet
      get.setRpcTimeout(this.singleGetTimeout.toShort)
      get.setFailfast(true)

      val cacheKey = MurmurHash3.stringHash(get.toString)
      //FIXME
      val cacheTTL = 10000
      if (vertexCache.asMap().containsKey(cacheKey)) {
        val cachedVal = vertexCache.asMap().get(cacheKey)
        if (cachedVal == null) {
          defferedToFuture(client.get(get))(emptyKVs).map { kvs =>
            Vertex(kvs, vertex.serviceColumn.schemaVersion)
          }
        } else {
          Future.successful(cachedVal)
        }
      } else {
        defferedToFuture(client.get(get))(emptyKVs).map { kvs =>
          Vertex(kvs, vertex.serviceColumn.schemaVersion)
        }
      }
    }
    Future.sequence(futures).map { result => result.toList.flatten }
  }

  def mutateEdge(edge: Edge): Future[Boolean] = {
    implicit val ex = this.executionContext
    writeAsync(edge.label.hbaseZkAddr, Seq(edge).map(e => e.buildPutsAll())).map { rets =>
      rets.forall(identity)
    }
  }

  def mutateEdges(edges: Seq[Edge]): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    val futures = edges.map { edge => mutateEdge(edge) }
    Future.sequence(futures)
  }

  def mutateVertex(vertex: Vertex): Future[Boolean] = {
    implicit val ex = this.executionContext
    if (vertex.op == GraphUtil.operations("delete")) {
      deleteVertex(vertex)
    } else if (vertex.op == GraphUtil.operations("deleteAll")) {
      //      throw new RuntimeException("Not yet supported")
      deleteVerticesAll(List(vertex)).onComplete {
        case Success(s) => Logger.info(s"mutateVertex($vertex) for deleteAll successed.")
        case Failure(ex) => Logger.error(s"mutateVertex($vertex) for deleteAll failed. $ex", ex)
      }
      Future.successful(true)
    } else {
      writeAsync(vertex.hbaseZkAddr, Seq(vertex).map(v => v.buildPutsAll())).map { rets =>
        rets.forall(identity)
      }
    }
  }

  def mutateVertices(vertices: Seq[Vertex]): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    val futures = vertices.map { vertex => mutateVertex(vertex) }
    Future.sequence(futures)
  }

  private def deleteVertex(vertex: Vertex): Future[Boolean] = {
    implicit val ex = this.executionContext
    writeAsync(vertex.hbaseZkAddr, Seq(vertex).map(_.buildDeleteAsync())).map { rets =>
      rets.forall(identity)
    }
  }

  private def deleteVertices(vertices: Seq[Vertex]): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    val futures = vertices.map { vertex => deleteVertex(vertex) }
    Future.sequence(futures)
  }

  /**
   * O(E), maynot feasable
   */

  def deleteVerticesAll(vertices: List[Vertex]): Future[Boolean] = {
    implicit val ex = this.executionContext

    val labelsMap = for {
      vertex <- vertices
      label <- (Label.findBySrcColumnId(vertex.id.colId) ++ Label.findByTgtColumnId(vertex.id.colId))
    } yield {
        label.id.get -> label
      }
    val labels = labelsMap.groupBy { case (labelId, label) => labelId }.map {
      _._2.head
    } values

    /** delete vertex only */
    for {
      relEdgesOutDeleted <- deleteVerticesAllAsync(vertices, labels.toSeq, GraphUtil.directions("out"))
      relEdgesInDeleted <- deleteVerticesAllAsync(vertices, labels.toSeq, GraphUtil.directions("in"))
      vertexDeleted <- deleteVertices(vertices)
    } yield {
      relEdgesOutDeleted && relEdgesInDeleted && vertexDeleted.forall(identity)
    }
  }

  def deleteVerticesAllAsync(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Option[Long]=None): Future[Boolean] = {
    implicit val ex = Graph.executionContext

    val queryParams = for {
      label <- labels
    } yield {
        val labelWithDir = LabelWithDirection(label.id.get, dir)
        QueryParam(labelWithDir).limit(0, maxValidEdgeListSize * 5)
      }

    val step = Step(queryParams.toList)
    val q = Query(srcVertices, List(step), true)

    for {
      queryResultLs <- getEdgesAsync(q)
      edges = for {
        queryResult <- queryResultLs
        (edge, score) <- queryResult.edgeWithScoreLs
      } yield {
        val timestamp = ts.getOrElse(System.currentTimeMillis())
        Edge(edge.srcVertex, edge.tgtVertex, edge.labelWithDir, GraphUtil.operations("delete"), timestamp, timestamp, edge.propsWithTs)
      }
      ret <- mutateEdges(edges)
    } yield {
      ret.foldLeft(true){(a,b) => a && b}
    }
  }


  def mutateElements(elemnents: Seq[GraphElement]): Future[Seq[Boolean]] = {
    implicit val ex = this.executionContext
    val futures = elemnents.map { element =>
      element match {
        case edge: Edge => mutateEdge(edge)
        case vertex: Vertex => mutateVertex(vertex)
        case _ => throw new RuntimeException(s"$element is not edge/vertex")
      }
    }
    Future.sequence(futures)
  }


  // select
  def getVertex(vertex: Vertex): Future[Option[Vertex]] = {
    implicit val ex = executionContext
    val client = getClient(vertex.hbaseZkAddr)
    defferedToFuture(client.get(vertex.buildGet))(emptyKVs).map { kvs =>
      Vertex(kvs, vertex.serviceColumn.schemaVersion)
    }
  }

  /**
   * Bulk
   */

  /**
   * when how from to what direction (meta info key:value)
   * ex) timestamp insert shon sol talk_friend directed/undirected propperties
   * ex) timestamp insert shon talk_user_id propperties
   *
   */
  def toGraphElement(s: String, labelMapping: Map[String, String] = Map.empty): Option[GraphElement] = {
    val parts = GraphUtil.split(s)
    try {
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
        throw new KGraphExceptions.JsonParseException("log type is not exist in log.")
      }
      element
    } catch {
      case e: Throwable =>
        Logger.error(s"$e", e)
        None
    }
  }

  def bulkMutates(elements: Iterable[GraphElement], mutateInPlace: Boolean = false) = {
    val vertices = new ListBuffer[Vertex]
    val edges = new ListBuffer[Edge]
    for (e <- elements) {
      e match {
        case edge: Edge => edges += edge
        case vertex: Vertex => vertices += vertex
        case _ => throw new Exception("GraphElement should be either vertex or edge.")
      }
    }
    mutateVertices(vertices)
    mutateEdges(edges)
  }

  def toVertex(s: String): Option[Vertex] = {
    toVertex(GraphUtil.split(s))
  }

  def toEdge(s: String): Option[Edge] = {
    toEdge(GraphUtil.split(s))
  }

  //"1418342849000\tu\te\t3286249\t71770\ttalk_friend\t{\"is_hidden\":false}"
  //{"from":1,"to":101,"label":"graph_test","props":{"time":-1, "weight":10},"timestamp":1417616431},
  def toEdge(parts: Array[String]): Option[Edge] = {
    try {
      val (ts, operation, logType, srcId, tgtId, label) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
      val props = if (parts.length >= 7) parts(6) else "{}"
      val tempDirection = if (parts.length >= 8) parts(7) else "out"
      val direction = if (tempDirection != "out" && tempDirection != "in") "out" else tempDirection

      val edge = Management.toEdge(ts.toLong, operation, srcId, tgtId, label, direction, props)
      //            Logger.debug(s"toEdge: $edge")
      Some(edge)
    } catch {
      case e: Throwable =>
        Logger.error(s"toEdge: $e", e)
        throw e
    }
  }

  //"1418342850000\ti\tv\t168756793\ttalk_user_id\t{\"country_iso\":\"KR\"}"
  def toVertex(parts: Array[String]): Option[Vertex] = {
    try {
      val (ts, operation, logType, srcId, serviceName, colName) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
      val props = if (parts.length >= 7) parts(6) else "{}"
      Some(Management.toVertex(ts.toLong, operation, srcId, serviceName, colName, props))
    } catch {
      case e: Throwable =>
        Logger.error(s"toVertex: $e", e)
        throw e
    }
  }

}
