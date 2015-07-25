package com.daumkakao.s2graph.core

import java.util

import com.daumkakao.s2graph.core.mysqls._
import com.google.common.cache.CacheBuilder

import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success}

//import com.daumkakao.s2graph.core.models._

import java.util.ArrayList
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import com.daumkakao.s2graph.core.types2._
import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.hbase.async._
import play.api.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.ClassTag

object GraphConstant {

  val vertexCf = "v".getBytes()
  val edgeCf = "e".getBytes()
  val updateCf = "u".getBytes()
  val ttsForActivity = 60 * 60 * 24 * 30
  val delimiter = "|"
  val seperator = ":"

  val writeBufferSize = 1024 * 1024 * 2

  val maxValidEdgeListSize = 10000

  //  implicit val ex = play.api.libs.concurrent.Execution.Implicits.defaultContext
}

object GraphConnection {
  lazy val tablePool = Executors.newFixedThreadPool(1)
  lazy val connectionPool = Executors.newFixedThreadPool(1)
  val defaultConfigs = Map(
    "hbase.zookeeper.quorum" -> "localhost",
    "hbase.table.name" -> "s2graph",
    "phase" -> "dev",
    "async.hbase.client.flush.interval" -> 100.toShort)
  var config: Config = null

  def getOrElse[T: ClassTag](conf: com.typesafe.config.Config)(key: String, default: T): T = {
    if (conf.hasPath(key)) (default match {
      case _: String => conf.getString(key)
      case _: Int | _: Integer => conf.getInt(key)
      case _: Float | _: Double => conf.getDouble(key)
      case _: Boolean => conf.getBoolean(key)
      case _ => default
    }).asInstanceOf[T]
    else default
  }

  /**
   * requred: hbase.zookeeper.quorum
   * optional: all hbase. prefix configurations.
   */
  def toHBaseConfig(config: com.typesafe.config.Config) = {
    val configVals = for ((k, v) <- defaultConfigs) yield {
      val currentVal = getOrElse(config)(k, v)
      Logger.debug(s"$k -> $currentVal")
      k -> currentVal
    }
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", configVals("hbase.zookeeper.quorum").toString)
    for (entry <- config.entrySet() if entry.getKey().startsWith("hbase.")) {
      val value = entry.getValue().unwrapped().toString
      conf.set(entry.getKey(), value)
    }
    conf
  }

  def apply(config: Config) = {
    this.config = config
    val hbaseConfig = toHBaseConfig(config)
    (hbaseConfig -> ConnectionFactory.createConnection(hbaseConfig))
  }
}

object Graph {

  import GraphConnection._
  import GraphConstant._

  //  val Logger = Edge.Logger
  val conns = scala.collection.mutable.Map[String, Connection]()
  val clients = scala.collection.mutable.Map[String, HBaseClient]()
  val emptyKVs = new ArrayList[KeyValue]()
  val emptyKVlist = new ArrayList[ArrayList[KeyValue]]()

  //  var shouldRunFromBytes = true
  //  var shouldReturnResults = true
  //  var shouldRunFetch = true
  //  var shouldRunFilter = true

  var executionContext: ExecutionContext = null
  var config: com.typesafe.config.Config = null
  var hbaseConfig: org.apache.hadoop.conf.Configuration = null
  var storageExceptionCount = 0L
  var singleGetTimeout = 10000 millis
  var clientFlushInterval = 100.toShort
  val defaultScore = 1.0

  lazy val cache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build[java.lang.Integer, QueryResult]()

  lazy val vertexCache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build[java.lang.Integer, Option[Vertex]]()


  //  implicit val ex = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))
  def apply(config: com.typesafe.config.Config)(implicit ex: ExecutionContext) = {
    this.config = config
    val (hbaseConfig, conn) = GraphConnection.apply(config)
    this.hbaseConfig = hbaseConfig
    Model.apply(config)
    this.executionContext = ex
    this.singleGetTimeout = getOrElse(config)("hbase.client.operation.timeout", 1000 millis)
    this.clientFlushInterval = getOrElse(config)("async.hbase.client.flush.interval", 100.toShort)
    val zkQuorum = hbaseConfig.get("hbase.zookeeper.quorum")
    //    conns += (zkQuorum -> conn)
    //    clients += (zkQuorum -> new HBaseClient(zkQuorum, "/hbase", Executors.newCachedThreadPool(), 8))
    clients += (zkQuorum -> getClient(zkQuorum, this.clientFlushInterval))
    ExceptionHandler.apply(config)
    //
    //    this.shouldRunFromBytes = getOrElse(config)("should.run.from.bytes", true)
    //    this.shouldReturnResults = getOrElse(config)("should.return.results", true)
    //    this.shouldRunFetch = getOrElse(config)("should.run.fetch", true)
    //    this.shouldRunFilter = getOrElse(config)("should.run.filter", true)
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

  //  def deferredCallback[R, T](d: Deferred[T])(f: T => R, fallback: => R) = {
  //    d.addCallback(new Callback[R, T]{
  //      def call(args: T): R = {
  //        args match {
  //          case ex: Throwable =>
  //            Logger.error(s"$ex", ex)
  //
  //        }
  //      }
  //    })
  //  }

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
          //          Logger.debug(s"$rpc")
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
        var seedEdgesFuture: Future[Seq[QueryResult]] = Future.successful(QueryResult.fromVertices(q, stepIdx = 0, q.steps.head.queryParams, q.vertices))
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
        //          val edgeWithScores = for {
        //            kv <- kvs
        //            edge <- Edge.toEdge(kv, queryParam)
        //          } yield {
        //              (edge, edge.rank(queryParam.rank) * prevScore)
        //            }
        val edgeWithScores = Edge.toEdges(kvs, queryParam, prevScore, isSnapshotEdge = false)
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
    val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(0.0)
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
      vertex.id.toString
    }
    val groupedByFiltered = for {
      (vertexId, edgesWithScore) <- groupedBy
      aggregatedScore = edgesWithScore.map(_._2).sum if aggregatedScore >= prevStepThreshold
    } yield {
        (edgesWithScore.head._1 -> aggregatedScore)
    }
    val nextStepSrcVertices = if (prevStepLimit >= 0) {
      groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
    } else {
      groupedByFiltered.toSeq
    }
//    val nextStepSrcVertices = (for {
//      queryResult <- queryResultsLs
//      (edge, score) <- queryResult.edgeWithScoreLs
//    } yield {
//        (edge.tgtVertex -> score)
//      }).groupBy { case (vertex, score) =>
//      vertex.id.toString
//    }.map { case (vertexId, edgesWithScore) =>
//      edgesWithScore.head._1 -> edgesWithScore.map(_._2).sum
//    }.filter(kv => kv._2 >= step.scoreThreshold)
//
//    val srcVertices = if (step.stepLimit >= 0) {
//      nextStepSrcVertices.toList.sortBy(-1 * _._2).take(step.stepLimit)
//    } else {
//      nextStepSrcVertices
//    }
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
      //      val edgeWithScoreLs = for {
      //        kv <- kvs
      //        edge <- Edge.toEdge(kv, queryParam)
      //      } yield {
      //          (edge, edge.rank(queryParam.rank))
      //        }
      val edgeWithScoreLs = Edge.toEdges(kvs, queryParam, prevScore = 1.0, isSnapshotEdge = true)
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


  def convertEdge(edge: Edge, labelOutputFields: Map[Int, Byte]): Option[Edge] = {
    //    val outputField = labelOutputFields.get(edge.labelWithDir.labelId)
    //    if (outputField == null) Option(edge)
    //    else {
    //      if (outputField == LabelMeta.toSeq) Option(edge)
    //      else if (outputField == LabelMeta.fromSeq) Option(edge.updateTgtVertex(edge.srcVertex.innerId))
    //      else {
    //        edge.propsWithTs.get(outputField) match {
    //          case None => None
    //          case Some(propVal) => Option(edge.updateTgtVertex(propVal.innerVal))
    //        }
    //      }
    //    }
    labelOutputFields.get(edge.labelWithDir.labelId) match {
      case None => Option(edge)
      case Some(outputField) if outputField == LabelMeta.toSeq => Option(edge)
      case Some(outputField) if outputField == LabelMeta.fromSeq => Option(edge.updateTgtVertex(edge.srcVertex.innerId))
      case Some(outputField) =>
        edge.propsWithTs.get(outputField) match {
          case None => None
          case Some(outputFieldVal) =>
            Some(edge.updateTgtVertex(outputFieldVal.innerVal))
        }
    }
  }

  //  def elapsed[T](prefix: String)(op: => T): T = {
  //    val started = System.nanoTime()
  //    val ret = op
  //    val duration = System.nanoTime() - started
  //    Logger.info(s"[ELAPSED]\t$prefix\t$duration")
  //    ret
  //  }
  type HashKey = (Int, Int, Int, Int)
  type FilterHashKey = (Int, Int)

  def toHashKey(queryParam: QueryParam, edge: Edge): (HashKey, FilterHashKey) = {
    val src = edge.srcVertex.innerId.hashKey(queryParam.srcColumnWithDir.columnType)
    val tgt = edge.tgtVertex.innerId.hashKey(queryParam.tgtColumnWithDir.columnType)
    val hashKey = (src, edge.labelWithDir.labelId, edge.labelWithDir.dir, tgt)
    val filterHashKey = (src, tgt)
    (hashKey, filterHashKey)
  }

  def edgeWithScoreWithDuplicatePolicy(duplicateEdges: ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]],
                                       hashKey: HashKey, edge: Edge, score: Double) = {
    if (duplicateEdges.containsKey(hashKey)) {
      duplicateEdges.get(hashKey)
    } else {
      Seq((edge -> score))
    }
  }

  def filterEdges(queryResultLsFuture: Future[ArrayList[QueryResult]],
                  q: Query,
                  stepIdx: Int,
                  alreadyVisited: Map[(LabelWithDirection, Vertex), Boolean] =
                  Map.empty[(LabelWithDirection, Vertex), Boolean]): Future[Seq[QueryResult]] = {
    implicit val ex = Graph.executionContext
    queryResultLsFuture.map { queryResultLs =>
      val step = q.steps(stepIdx)
      val labelOutputFields = step.queryParams.map { qParam =>
        qParam.outputField.map(outputField => qParam.labelWithDir.labelId -> outputField)
          .getOrElse(qParam.labelWithDir.labelId -> LabelMeta.toSeq)
      }.toMap

      val excludeLabelWithDirSet = step.queryParams.filter(_.exclude).map(l => l.labelWithDir.labelId -> l.labelWithDir.dir).toSet
      val includeLabelWithDirSet = step.queryParams.filter(_.include).map(l => l.labelWithDir.labelId -> l.labelWithDir.dir).toSet



      val edgesToExclude = new util.concurrent.ConcurrentHashMap[FilterHashKey, Boolean]()
      val edgesToInclude = new util.concurrent.ConcurrentHashMap[FilterHashKey, Boolean]()

      val queryParamResultLs = for {
        queryResult <- queryResultLs
      } yield {
          val duplicateEdges = new util.concurrent.ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]]()
          val resultEdgeWithScores = new util.concurrent.ConcurrentHashMap[HashKey, (HashKey, FilterHashKey, Edge, Double)]()

          for {
            (edge, score) <- queryResult.edgeWithScoreLs
            convertedEdge <- convertEdge(edge, labelOutputFields)
            (hashKey, filterHashKey) = toHashKey(queryResult.queryParam, convertedEdge)
          } {
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

              val newScore = score * tsVal

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
          (duplicateEdges, resultEdgeWithScores)
        }

      val aggregatedResults = for {
        (queryResult, queryParamResult) <- queryResultLs.zip(queryParamResultLs)
        (duplicateEdges, resultEdgeWithScores) = queryParamResult
      } yield {
        val edgesWithScores = for {
          (hashKey, filterHashKey, edge, score) <- resultEdgeWithScores.values
          if edgesToInclude.containsKey(filterHashKey) || !edgesToExclude.containsKey(filterHashKey)
          (duplicateEdge, aggregatedScore) <- edgeWithScoreWithDuplicatePolicy(duplicateEdges, hashKey, edge, score)
          if aggregatedScore >= queryResult.queryParam.threshold
        } yield (duplicateEdge, aggregatedScore)

        QueryResult(queryResult.query, queryResult.stepIdx, queryResult.queryParam, edgesWithScores)
      }
      aggregatedResults
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
      get.setRpcTimeout(this.singleGetTimeout.toMillis.toShort)
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
    if (vertex.op == GraphUtil.operations("delete") || vertex.op == GraphUtil.operations("deleteAll")) {
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

  def deleteVerticesAllAsync(srcVertices: List[Vertex], labels: Seq[Label], dir: Int): Future[Boolean] = {
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
      invertedFutures = for {
        queryResult <- queryResultLs
        (edge, score) <- queryResult.edgeWithScoreLs
      } yield {
          val now = System.currentTimeMillis()
          val convertedEdge = if (dir == GraphUtil.directions("out")) {
            Edge(edge.srcVertex, edge.tgtVertex, edge.labelWithDir, edge.op, edge.ts, now, edge.propsWithTs)
          } else {
            Edge(edge.tgtVertex, edge.srcVertex, edge.labelWithDir, edge.op, edge.ts, now, edge.propsWithTs)
          }
          Logger.debug(s"ConvertedEdge: $convertedEdge")
          for {
            rets <- writeAsync(edge.label.hbaseZkAddr, Seq(convertedEdge).map { e =>
              e.buildDeleteBulk()
            })
          } yield {
            rets.forall(identity)
          }
        }
      ret <- Future.sequence(invertedFutures).map { rets => rets.forall(identity) }
    } yield ret
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
