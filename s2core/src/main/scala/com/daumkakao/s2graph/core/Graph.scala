package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.models.{HBaseModel, HLabel}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import java.util.concurrent.Executors
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory

import scala.collection.mutable.SynchronizedQueue
import org.apache.hadoop.hbase.filter.FilterList
import HBaseElement._
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.HashSet
import GraphUtil._
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import scala.annotation.tailrec
import scala.collection.mutable.HashMap
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter
import org.apache.hadoop.hbase.Cell
import Management._
import play.api.libs.json.Json
import KGraphExceptions._
import play.libs.Akka
import com.typesafe.config.Config
import scala.reflect.ClassTag
import org.hbase.async._
import com.stumbleupon.async.Deferred
import com.stumbleupon.async.Callback
import java.util.ArrayList

import scala.util.{Try, Success, Failure}

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
  val logger = Graph.logger
  lazy val tablePool = Executors.newFixedThreadPool(1)
  lazy val connectionPool = Executors.newFixedThreadPool(1)
  val defaultConfigs = Map(
    "hbase.zookeeper.quorum" -> "localhost",
    "hbase.table.name" -> "s2graph",
    "phase" -> "dev")
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
      logger.debug(s"$k -> $currentVal")
      k -> currentVal
    }
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", configVals("hbase.zookeeper.quorum"))
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
  import GraphConstant._
  import GraphConnection._

  val logger = Edge.logger
  val conns = scala.collection.mutable.Map[String, Connection]()
  val clients = scala.collection.mutable.Map[String, HBaseClient]()
  val emptyKVs = new ArrayList[KeyValue]()
  val emptyKVlist = new ArrayList[ArrayList[KeyValue]]();
  val emptyEdgeList = new ArrayList[ArrayList[(Edge, Double, QueryParam)]]
  val emptyEdges = new ArrayList[(Edge, Double, QueryParam)]

  var executionContext: ExecutionContext = null
  var config: com.typesafe.config.Config = null
  var hbaseConfig: org.apache.hadoop.conf.Configuration = null
  var storageExceptionCount = 0L
  var singleGetTimeout = 1000 millis
  //  implicit val ex = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))
  def apply(config: com.typesafe.config.Config)(implicit ex: ExecutionContext) = {
    this.config = config
    val (hbaseConfig, conn) = GraphConnection.apply(config)
    this.hbaseConfig = hbaseConfig
    HBaseModel.apply(config)
    this.executionContext = ex
    this.singleGetTimeout = getOrElse(config)("hbase.client.operation.timeout", 1000 millis)
    val zkQuorum = hbaseConfig.get("hbase.zookeeper.quorum")
//    conns += (zkQuorum -> conn)
    //    clients += (zkQuorum -> new HBaseClient(zkQuorum, "/hbase", Executors.newCachedThreadPool(), 8))
    clients += (zkQuorum -> new HBaseClient(zkQuorum))

  }
  def getClient(zkQuorum: String) = {
    clients.get(zkQuorum) match {
      case None =>
        val client = new HBaseClient(zkQuorum)
        clients += (zkQuorum -> client)
        client
      //        throw new RuntimeException(s"connection to $zkQuorum is not established.")
      case Some(c) => c
    }
  }
  def getConn(zkQuorum: String) = {
    conns.get(zkQuorum) match {
      case None =>
        val conn = ConnectionFactory.createConnection(this.hbaseConfig)
        conns += (zkQuorum -> conn)
        conn
      //        throw new RuntimeException(s"connection to $zkQuorum is not established.")
      case Some(c) => c
    }
  }
//
//  def withWriteTable[T](connName: String, tName: String)(op: HTableInterface => T)(fallback: => T): T = {
//    //    Logger.debug(s"withWriteTable: $connName, $tName")
//    try {
//      val conn = getConn(connName)
//      if (!conn.isMasterRunning()) throw new RuntimeException(s"master is not running. $connName")
//      val table = conn.getTable(tName, tablePool)
//
//      table.setAutoFlush(false, false)
//      table.setWriteBufferSize(writeBufferSize)
//      try {
//        op(table)
//      } catch {
//        case e: Exception =>
//          Logger.error(s"Write Operation to table ($connName), ($tName) is failed: ${e.getMessage}", e)
//          fallback
//          throw e
//      } finally {
//        table.close()
//      }
//    } catch {
//      case e: Exception =>
//        Logger.error(s"withWriteTable ($connName) ($tName) is failed: ${e.getMessage}", e)
//        fallback
//        throw e
//    }
//  }
//
//  def withReadTable[T](connName: String, tName: String)(op: HTableInterface => T)(fallback: => T): T = {
//    //    queryLogger.debug(s"withReadTable: $connName, $tName")
//    try {
//      val conn = getConn(connName)
//      if (!conn.isMasterRunning()) throw new RuntimeException(s"master is not running. $connName")
//      val table = conn.getTable(tName, tablePool)
//
//      table.setAutoFlush(false, false)
//      table.setWriteBufferSize(writeBufferSize)
//      try {
//        op(table)
//      } catch {
//        case e: Exception =>
//          queryLogger.error(s"Read Operation to table ($connName) ($tName) is failed: ${e.getMessage}", e)
//          fallback
//      } finally {
//        table.close()
//      }
//    } catch {
//      case e: Exception =>
//        queryLogger.error(s"withReadTable ($connName) ($tName) is failed: ${e.getMessage}", e)
//        fallback
//    }
//  }
//
//  // to catch exception on htable.get
//  def htableGet(table: HTableInterface)(get: Get) = {
//    try {
//      table.get(get)
//    } catch {
//      case e: Throwable =>
//        // if hbase is throw any exception, simply return empty result
//        queryLogger.error(s"hbaseGet: $get, $e", e)
//        new Result()
//    }
//  }

  //  def withTimeout[T](op: => Future[T], fallback: => T)(implicit timeout: Duration): Future[T] = {
  //    //    val timeoutFuture = play.api.libs.concurrent.Promise.timeout(fallback, timeout)
  //    //    Future.firstCompletedOf(Seq(op, timeoutFuture))
  //    TimeoutFuture(op, fallback)(executionContext, timeout)
  //    //    val timeoutFuture = akka.pattern.after(timeout.toMillis millis, using = Akka.system.scheduler) { Future { fallback } }
  //    //    Future.firstCompletedOf(Seq(op, timeoutFuture))
  //  }
  def withTimeout[T](zkQuorum: String, op: => Future[T], fallback: => T)(implicit timeout: Duration): Future[T] = {
    try {
      val client = getClient(zkQuorum)
      //      TimeoutFuture(op, fallback)(this.executionContext, timeout)
      op
    } catch {
      case e: Throwable =>
        logger.error(s"withTimeout: $e", e)
        Future { fallback }(this.executionContext)
    }
  }

//  def save(zkQuorum: String, tableName: String, mutations: Seq[Mutation]): Unit = {
//    //    Logger.debug(s"save to $zkQuorum, $tableName, ${mutations.size} mutations.")
//    if (mutations.isEmpty) {}
//    else {
//      withWriteTable[Unit](zkQuorum, tableName) { table =>
//        val ret: Array[Object] = Array.fill(mutations.size)(null)
//        table.batch(mutations, ret)
//        // TODO: retry
//        for (r <- ret if r == null) {
//          Logger.error(s"save failed after batch.")
//        }
//      } {
//        Logger.error(s"save mutation failed.")
//      }
//    }
//  }
  def defferedToFuture[A](d: Deferred[A])(fallback: A): Future[A] = {
    val promise = Promise[A]

    d.addBoth(new Callback[Unit, A] {
      def call(arg: A) = arg match {
        case e: Throwable =>
          logger.error(s"deferred return throwable: $e", e)
          promise.success(fallback)
        case _ => promise.success(arg)
      }
    })

    promise.future
  }
  def deferredToFutureWithoutFallback[T](d: Deferred[T]) = {
    val promise = Promise[T]
    d.addBoth(new Callback[Unit, T]{
      def call(arg: T) = arg match {
        case e: Throwable =>
          logger.error(s"deferred return throwable: $e", e)
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
  def writeAsync(zkQuorum: String, rpcs: Seq[HBaseRpc]) = {
    if (rpcs.isEmpty) {}
    else {
      try {
        val client = getClient(zkQuorum)
        val futures = rpcs.map { rpc =>
          val deferred = rpc match {
            case d: DeleteRequest => client.delete(d)
            case p: PutRequest => client.put(p)
            case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i)
          }
          deferredToFutureWithoutFallback(deferred)
        }
//        Future.sequence(futures)
      } catch {
        case e: Throwable =>
          logger.error(s"writeAsync failed. $e", e)
      }
    }
  }
  /**
   * Edge
   */
//  def mutateEdge(edge: Edge): Unit = {
//    save(edge.label.hbaseZkAddr, edge.label.hbaseTableName, edge.buildPutsAll())
//  }
  def mutateEdge(edge: Edge) = {
    writeAsync(edge.label.hbaseZkAddr, edge.buildPutsAll())
  }
//  def mutateEdges(edges: Seq[Edge], mutateInPlace: Boolean = false): Unit = {
//
//    val edgesPerTable = edges.groupBy { edge => (edge.label.hbaseZkAddr, edge.label.hbaseTableName) }
//    for (((zkQuorum, tableName), edges) <- edgesPerTable) {
//      /**
//       * delete/update/increment can`t be batched.
//       */
//      val (batches, others) = edges.partition(e => e.canBeBatched)
//      save(zkQuorum, tableName, batches.flatMap(_.buildPutsAll))
//      others.foreach(other => save(zkQuorum, tableName, other.buildPutsAll))
//    }
//    val verticesPerTable = edges.flatMap { edge => List(edge.srcForVertex, edge.tgtForVertex) }.groupBy { v => (v.hbaseZkAddr, v.hbaseTableName) }
//    for (((zkQuorum, tableName), vertices) <- verticesPerTable) {
//      save(zkQuorum, tableName, vertices.flatMap(v => v.buildPuts))
//    }
//  }
  def mutateEdges(edges: Seq[Edge]) = {
    val edgesPerTable = edges.groupBy { edge => edge.label.hbaseZkAddr }
    for ((zkQuorum, edges) <- edgesPerTable) {
      val (batches, others) = edges.partition(e => e.canBeBatched)
      writeAsync(zkQuorum, batches.flatMap(_.buildPutsAll))
      others.foreach(other => writeAsync(zkQuorum, other.buildPutsAll))
    }
    val verticesPerTable = edges.flatMap { edge => List(edge.srcForVertex, edge.tgtForVertex) }.groupBy { v => v.hbaseZkAddr }
    for ((zkQuorum, vertices) <- verticesPerTable) {
      writeAsync(zkQuorum, vertices.flatMap(v => v.buildPutsAsync))
    }
  }
  //select
  /**
   *
   */
  def getEdgesAsync(q: Query): Future[Seq[Iterable[(Edge, Double)]]] = {
    implicit val ex = this.executionContext
    // not sure this is right. make sure refactor this after.
    try {
      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        Future { q.vertices.map(v => List.empty[(Edge, Double)]) }
      } else {
        val stepLen = q.steps.length
        var step = q.steps.head

        var seedEdgesFuture = getEdgesAsyncWithRankForFistStep(q.vertices, q, 0)

        for (i <- (1 until stepLen)) {
          seedEdgesFuture = getEdgesAsyncWithRank(seedEdgesFuture, q, i)
        }

        seedEdgesFuture
      }
    } catch {
      case e: Throwable =>
        logger.error(s"getEdgesAsync: $e", e)
        Future { q.vertices.map(v => List.empty[(Edge, Double)]) }
    }
  }

  //only for testcase.
  def getEdgesSync(q: Query): Seq[Iterable[(Edge, Double)]] = {
    Await.result(getEdgesAsync(q), 10 seconds)
  }
  //only for testcase.
//  def getEdgeSync(srcVertex: Vertex, tgtVertex: Vertex, label: Label, dir: Int) = {
//    val rowKey = EdgeRowKey(srcVertex.id, LabelWithDirection(label.id.get, dir), LabelIndex.defaultSeq, isInverted = true)
//    val qualifier = EdgeQualifierInverted(tgtVertex.id)
//    val get = new Get(rowKey.bytes)
//    get.addColumn(edgeCf, qualifier.bytes)
//    withReadTable(label.hbaseZkAddr, label.hbaseTableName) { table =>
//      //      table.get(get)
//      Edge.toEdges(htableGet(table)(get))
//    } {
//      List.empty[Edge]
//    }
//  }

  def getEdge(srcVertex: Vertex, tgtVertex: Vertex, label: HLabel, dir: Int): Future[Iterable[Edge]] = {
    implicit val ex = this.executionContext
    val rowKey = EdgeRowKey(srcVertex.id, LabelWithDirection(label.id.get, dir), label.defaultIndex.get.seq, isInverted = true)

    val qualifier = EdgeQualifierInverted(tgtVertex.id)
    val client = getClient(label.hbaseZkAddr)
    val getRequest = new GetRequest(label.hbaseTableName.getBytes(), rowKey.bytes, edgeCf, qualifier.bytes)
    defferedToFuture(client.get(getRequest))(emptyKVs).map { kvs =>
      for {
        kv <- kvs
        edge <- Edge.toEdge(kv, QueryParam(LabelWithDirection(label.id.get, dir.toByte)))
      } yield edge
    }
  }

  def buildGetRequests(srcVertices: Seq[Vertex], params: List[QueryParam]): Seq[Iterable[(GetRequest, QueryParam)]] = {
    srcVertices.map { vertex =>
      params.map { param =>
        (param.buildGetRequest(vertex), param)
      }
    }
  }
  //  def buildGets(srcVertices: Seq[Vertex], params: List[QueryParam]): Seq[Iterable[(Get, QueryParam)]] = {
  //    srcVertices.map { vertex =>
  //      params.map { param =>
  //        (param.buildGet(vertex), param)
  //      }
  //    }
  //  }

  def singleGet(table: Array[Byte], rowKey: Array[Byte], cf: Array[Byte], offset: Int, limit: Int,
    minTs: Long, maxTs: Long,
    maxAttempt: Int, rpcTimeoutInMillis: Int,
    columnRangeFilter: ColumnRangeFilter) = {
    val get = new GetRequest(table, rowKey, cf)
    get.maxVersions(1)
    get.setFailfast(true)
    get.setMaxResultsPerColumnFamily(limit)
    get.setRowOffsetPerColumnFamily(offset)
    get.setMinTimestamp(minTs)
    get.setMaxTimestamp(maxTs)
    get.setMaxAttempt(maxAttempt.toByte)
    get.setRpcTimeout(rpcTimeoutInMillis)
    if (columnRangeFilter != null) get.filter(columnRangeFilter)
    get
  }
  def convertEdge(edge: Edge, labelOutputFields: Map[Int, Byte]): Option[Edge] = {
    labelOutputFields.get(edge.labelWithDir.labelId) match {
      case None => Some(edge)
      case Some(outputField) =>
        edge.propsWithTs.get(outputField) match {
          case None => None
          case Some(outputFieldVal) =>
            Some(edge.updateTgtVertex(outputFieldVal.innerVal))
        }
    }
  }

  def filterEdges(edgesFuture: Future[ArrayList[ArrayList[(Edge, Double, QueryParam)]]],
    q: Query,
    stepIdx: Int,
    alreadyVisited: Map[(LabelWithDirection, Vertex), Boolean] = Map.empty[(LabelWithDirection, Vertex), Boolean]): Future[Seq[Iterable[(Edge, Double)]]] = {
    implicit val ex = Graph.executionContext
    edgesFuture.map { edgesByVertices =>
      val step = q.steps(stepIdx)
      val labelOutputFields = step.queryParams.map { qParam => qParam.outputField.map(outputField => qParam.labelWithDir.labelId -> outputField) }.flatten.toMap
      val excludeLabelWithDirs = (for (queryParam <- step.queryParams if queryParam.exclude) yield queryParam.labelWithDir).toSet
      val includeLabelWithDirOpt = (for (queryParam <- step.queryParams if queryParam.include) yield queryParam.labelWithDir).toSet.headOption
      val seen = new HashMap[(Vertex, LabelWithDirection, Vertex), Double]
      //      val seen = new HashSet[(LabelWithDirection, Vertex)]
      //      val excludeFromTos = new HashSet[(CompositeId, CompositeId)]
      //      val includeFromTos = new HashSet[(CompositeId, CompositeId)]
      val excludeFromTos = new HashSet[(Vertex, Vertex)]
      val includeFromTos = new HashSet[(Vertex, Vertex)]

      val hasIncludeLabel = includeLabelWithDirOpt.isDefined
      //      if (hasIncludeLabel) {
      for {
        edgesWithScore <- edgesByVertices
        (edge, score, queryParam) <- edgesWithScore
      } {
        //        Logger.debug(s"${edge.toStringRaw}")
        if (hasIncludeLabel && edge.labelWithDir == includeLabelWithDirOpt.get) {
          //          includeFromTos += (edge.srcVertex.id -> edge.tgtVertex.id)
          includeFromTos += (edge.srcVertex -> edge.tgtVertex)
        }
        if (excludeLabelWithDirs.contains(edge.labelWithDir)) {
          //          excludeFromTos += (edge.srcVertex.id -> edge.tgtVertex.id)
          excludeFromTos += (edge.srcVertex -> edge.tgtVertex)
        }
      }
      //      }

      //      for {
      //        edgesWithScore <- edgesByVertices
      //        (edge, score) <- edgesWithScore if excludeLabelWithDirs.contains(edge.labelWithDir)
      //      } {
      //        excludeFromTos += (edge.srcVertex -> edge.tgtVertex)
      //      }

      //      Logger.debug(s"${excludeFromTos.mkString("\n")}")
      //      Logger.debug(s"$includeFromTos")

      val convertedEdges = for {
        edgesWithScore <- edgesByVertices
      } yield {
        for {
          //          (edge, score) <- edgesWithScore if !excludeFromTos.contains((edge.srcVertex.id -> edge.tgtVertex.id))
          //          if (!hasIncludeLabel || includeFromTos.contains((edge.srcVertex.id -> edge.tgtVertex.id)))
          (edge, score, queryParam) <- edgesWithScore
          fromTo = (edge.srcVertex -> edge.tgtVertex)
          if !excludeFromTos.contains(fromTo)
          if (!hasIncludeLabel || includeFromTos.contains(fromTo))
          convertedEdge <- convertEdge(edge, labelOutputFields)
          key = (convertedEdge.labelWithDir, convertedEdge.tgtVertex)
          //          if !seen.contains(key)
          if filterDuplicates(seen, edge, score, queryParam)
          if !(q.removeCycle && alreadyVisited.contains(key))
        } yield {
          //          seen += key

          (convertedEdge, score)
        }
      }
      for {
        edgesWithScore <- convertedEdges
      } yield {
        for {
          (edge, score) <- edgesWithScore
          key = (edge.srcVertex, edge.labelWithDir, edge.tgtVertex)
          aggregatedScore = seen.getOrElse(key, score)
        } yield {
          (edge, aggregatedScore)
        }
      }
    }
  }
  private def filterDuplicates(seen: HashMap[(Vertex, LabelWithDirection, Vertex), Double], edge: Edge, score: Double, queryParam: QueryParam) = {
    val key = (edge.srcVertex, edge.labelWithDir, edge.tgtVertex)
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


  def getEdgesAsyncWithRankForFistStep(srcVertices: Seq[Vertex], q: Query, stepIdx: Int): Future[Seq[Iterable[(Edge, Double)]]] = {
    val step = q.steps(stepIdx)
    val uniqSrcVertices = srcVertices.groupBy(v => v.id).map { kv => (kv._2.head, 1.0) }
    getEdgesAsyncWithRank(uniqSrcVertices.toSeq, q, stepIdx)
  }


  private def getEdgesAsyncWithRank(prevEdges: Seq[(Edge, Double)], q: Query, stepIdx: Int): Future[Seq[Iterable[(Edge, Double)]]] = {
    val step = q.steps(stepIdx)
    val alreadyVisited = prevEdges.map {
      case (edge, score) =>
        (edge.labelWithDir, if (edge.labelWithDir.dir == GraphUtil.directions("out")) edge.tgtVertex else edge.srcVertex) -> true
    }.toMap

    val srcVertices = prevEdges.map { case (edge, score) => (edge.tgtVertex -> score) }
    //    val getsAll = buildGets(srcVertices.map(_._1), step.queryParams).zip(srcVertices.map(_._2))
    val getsAll = buildGetRequests(srcVertices.map(_._1), step.queryParams).zip(srcVertices.map(_._2))

    implicit val ex = executionContext
    val deffered = getsAll.flatMap { //by verticies
      case (getsWithQueryParams, prevScore) =>
        getsWithQueryParams.map { //by labels
          case (get, queryParam) =>
            try {
              val client = getClient(queryParam.label.hbaseZkAddr)
              deferredCallbackWithFallback(client.get(get))({ kvs =>
                val edges = for {
                  kv <- kvs
                  edge <- Edge.toEdge(kv, queryParam)
                } yield {
                  (edge, edge.rank(queryParam.rank) * prevScore, queryParam)
                }
                new ArrayList(edges)
              }, emptyEdges)
            } catch {
              case e @ (_: Throwable | _: Exception) =>
                logger.error(s"Exception: $e", e)
                Deferred.fromResult(emptyEdges)
            }
        }
    }
    val grouped = Deferred.group(deffered)
    filterEdges(defferedToFuture(grouped)(emptyEdgeList), q, stepIdx, alreadyVisited)
  }
  private def getEdgesAsyncWithRank(srcVertices: Seq[(Vertex, Double)], q: Query, stepIdx: Int): Future[Seq[Iterable[(Edge, Double)]]] = {
    val step = q.steps(stepIdx)
    //    val getsAll = buildGets(srcVertices.map(_._1), step.queryParams).zip(srcVertices.map(_._2))
    val getsAll = buildGetRequests(srcVertices.map(_._1), step.queryParams).zip(srcVertices.map(_._2))
    implicit val ex = executionContext
    val deffered = getsAll.flatMap { //by verticies
      case (getsWithQueryParams, prevScore) =>
        getsWithQueryParams.map { //by labels
          case (get, queryParam) =>
            try {
              val client = getClient(queryParam.label.hbaseZkAddr)
              deferredCallbackWithFallback(client.get(get))({ kvs =>
                val edges = for {
                  kv <- kvs
                  edge <- Edge.toEdge(kv, queryParam)
                } yield {
                  (edge, edge.rank(queryParam.rank) * prevScore, queryParam)
                }
                new ArrayList(edges)
              }, emptyEdges)
            } catch {
              case e @ (_: Throwable | _: Exception) =>
                logger.error(s"Exception: $e", e)
                Deferred.fromResult(emptyEdges)

            }
        }
    }
    val grouped = Deferred.group(deffered)

    filterEdges(defferedToFuture(grouped)(emptyEdgeList), q, stepIdx)
  }

  def getEdgesAsyncWithRank(srcEdgesFuture: Future[Seq[Iterable[(Edge, Double)]]], q: Query, stepIdx: Int): Future[Seq[Iterable[(Edge, Double)]]] = {
    implicit val ex = executionContext
    for {
      srcEdges <- srcEdgesFuture
      edgesWithScore = srcEdges.flatten
      ret <- getEdgesAsyncWithRank(edgesWithScore, q, stepIdx)
      //      verticeWithRanks = edgesWithScore.map(t => (t._1.tgtVertex, t._2)).toSeq
      //      ret <- getEdgesAsyncWithRank(verticeWithRanks, step)
    } yield {
      ret
    }
  }

  def getVerticesAsync(vertices: Seq[Vertex]): Future[Seq[Vertex]] = {
    // TODO: vertex needs meta for hbase table.
    //    play.api.Logger.error(s"$vertices")
    implicit val ex = executionContext
    val futures = vertices.map { vertex =>
      withTimeout[Option[Vertex]](vertex.hbaseZkAddr, {
        //          val get = vertex.buildGet
        val client = getClient(vertex.hbaseZkAddr)
        //        val get = vertex.buildGetRequest()
        val get = vertex.buildGet
        defferedToFuture(client.get(get))(emptyKVs).map { kvs =>
          Vertex(kvs)
        }
        //          Logger.error(s"$get")
      }, { None })(singleGetTimeout)

    }
    Future.sequence(futures).map { result => result.toList.flatten }
  }
  /**
   * Vertex
   */

//  def mutateVertex(vertex: Vertex) = {
//    mutateVertices(List(vertex))
//  }
  def mutateVertex(vertex: Vertex) = {
    mutateVertices(List(vertex))
  }
//  def mutateVertices(vertices: Seq[Vertex]) = {
//    for (((zkQuorum, tableName, op), vertices) <- vertices.groupBy(v => (v.hbaseZkAddr, v.hbaseTableName, v.op))) {
//      if (op == GraphUtil.operations("delete") || op == GraphUtil.operations("deleteAll")) deleteVertexAll(vertices)
//      else save(zkQuorum, tableName, vertices.flatMap(v => v.buildPutsAll()))
//    }
//  }
  def mutateVertices(vertices: Seq[Vertex]) = {
    for (((zkQuorum, op), vertices) <- vertices.groupBy(v => (v.hbaseZkAddr, v.op))) {
      if (op == GraphUtil.operations("delete") || op == GraphUtil.operations("deleteAll")) deleteVertexAll(vertices)
      else writeAsync(zkQuorum, vertices.flatMap(v => v.buildPutsAll()))
    }
  }
  // delete only vertices
  def deleteVertices(vertices: Seq[Vertex]) = {
    for ((zkQuorum, vs) <- vertices.groupBy(v => v.hbaseZkAddr)) {
      writeAsync(zkQuorum, vs.flatMap(v => v.buildDeleteAsync()))
    }
  }
  /**
   * O(E), maynot feasable
   */
  def deleteVertexAll(vertices: Seq[Vertex]): Unit = {
    for {
      vertex <- vertices
      label <- (HLabel.findBySrcColumnId(vertex.id.colId) ++ HLabel.findByTgtColumnId(vertex.id.colId)).groupBy(_.id.get).map { _._2.head }
    } {
      deleteVertexAllAsync(vertex.toEdgeVertex, label)
    }
    deleteVertices(vertices)
  }

  private def deleteVertexAllAsync(srcVertex: Vertex, label: HLabel): Future[Boolean] = {
    implicit val ex = Graph.executionContext
    val qParams = for (dir <- List(0, 1)) yield {
      val labelWithDir = LabelWithDirection(label.id.get, dir)
      QueryParam(labelWithDir).limit(0, maxValidEdgeListSize * 5)
    }
    val step = Step(qParams)
    val q = Query(List(srcVertex), List(step), true)
    val seen = new HashMap[(CompositeId, LabelWithDirection), Boolean]
    for {
      edgesByVertex <- getEdgesAsync(q)
    } yield {
      val fetchedEdges = for {
        edges <- edgesByVertex
        (edge, score) <- edges if edge.ts <= srcVertex.ts && !seen.containsKey((edge.tgtVertex.id, edge.labelWithDir))
      } yield {
        val labelWithDir = if (label.isDirected) edge.labelWithDir.updateDir(2) else edge.labelWithDir
        val edgeToDelete = Edge(edge.srcVertex, edge.tgtVertex, labelWithDir, GraphUtil.operations("delete"), srcVertex.ts, edge.version + Edge.incrementVersion, edge.propsWithTs)
        seen += ((edgeToDelete.tgtVertex.id, edgeToDelete.labelWithDir) -> true)

        // reverse or not? => reverse.
        // delete edge or real delete operation? => ?
        //          play.api.Logger.debug(s"EdgeToDelete: $edgeToDelete")
        //        (edge.label.hbaseTableName, edgeToDelete.relatedEdges.flatMap(e => e.buildPutsAll()))
        edge
      }
      for ((zkQuorum, edges) <- fetchedEdges.groupBy(e => e.label.hbaseZkAddr)) {
        writeAsync(zkQuorum, edges.flatMap(_.buildPutsAll))
      }
      true
    }
  }
  // select
  def getVertex(vertex: Vertex): Future[Option[Vertex]] = {
    implicit val ex = executionContext
    val client = getClient(vertex.hbaseZkAddr)
    defferedToFuture(client.get(vertex.buildGet))(emptyKVs).map { kvs =>
      Vertex(kvs)
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

  def toGraphElement(s: String, labelNameToReplace: Option[String] = None): Option[GraphElement] = {
    val parts = GraphUtil.split(s)
    try {
      val logType = parts(2)
      val element = if (logType == "edge" | logType == "e") {
        /** current only edge is considered to be bulk loaded */
        labelNameToReplace match {
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
      // use db field is_directed.
      val direction = ""
      val edge = Management.toEdge(ts.toLong, operation, srcId, tgtId, label, direction, props)
      //      Logger.debug(s"toEdge: $edge")
      Some(edge)
    } catch {
      case e: Throwable =>
        logger.error(s"toEdge: $e", e)
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
        logger.error(s"toVertex: $e", e)
        throw e
    }
  }

}