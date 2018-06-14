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

package org.apache.s2graph.core

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, Executors}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import org.apache.s2graph.core.index.IndexProvider
import org.apache.s2graph.core.io.tinkerpop.optimize.S2GraphStepStrategy
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorage
import org.apache.s2graph.core.storage.rocks.RocksStorage
import org.apache.s2graph.core.storage.{MutateResponse, OptimisticEdgeFetcher, Storage}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.{Extensions, Importer, logger}
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, Graph}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.util.Try


object S2Graph {

  type HashKey = (Int, Int, Int, Int, Boolean)
  type FilterHashKey = (Int, Int)

  val DefaultScore = 1.0
  val FetchAllLimit = 10000000
  val DefaultFetchLimit = 1000
  import S2GraphConfigs._

  private val DefaultConfigs: Map[String, AnyRef] = Map(
    S2GRAPH_STORE_BACKEND -> DEFAULT_S2GRAPH_STORE_BACKEND,
    PHASE -> DEFAULT_PHASE,
    HBaseConfigs.HBASE_ZOOKEEPER_QUORUM -> HBaseConfigs.DEFAULT_HBASE_ZOOKEEPER_QUORUM,
    HBaseConfigs.HBASE_ZOOKEEPER_ZNODE_PARENT -> HBaseConfigs.DEFAULT_HBASE_ZOOKEEPER_ZNODE_PARENT,
    HBaseConfigs.HBASE_TABLE_NAME -> HBaseConfigs.DEFAULT_HBASE_TABLE_NAME,
    HBaseConfigs.HBASE_TABLE_COMPRESSION_ALGORITHM -> HBaseConfigs.DEFAULT_HBASE_TABLE_COMPRESSION_ALGORITHM,
    HBaseConfigs.HBASE_CLIENT_RETRIES_NUMBER -> HBaseConfigs.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER,
    HBaseConfigs.HBASE_RPCS_BUFFERED_FLUSH_INTERVAL -> HBaseConfigs.DEFAULT_HBASE_RPCS_BUFFERED_FLUSH_INTERVAL,
    HBaseConfigs.HBASE_RPC_TIMEOUT -> HBaseConfigs.DEFAULT_HBASE_RPC_TIMEOUT,
    DBConfigs.DB_DEFAULT_DRIVER -> DBConfigs.DEFAULT_DB_DEFAULT_DRIVER,
    DBConfigs.DB_DEFAULT_URL -> DBConfigs.DEFAULT_DB_DEFAULT_URL,
    DBConfigs.DB_DEFAULT_PASSWORD -> DBConfigs.DEFAULT_DB_DEFAULT_PASSWORD,
    DBConfigs.DB_DEFAULT_USER -> DBConfigs.DEFAULT_DB_DEFAULT_USER,
    CacheConfigs.CACHE_MAX_SIZE -> CacheConfigs.DEFAULT_CACHE_MAX_SIZE,
    CacheConfigs.CACHE_TTL_SECONDS -> CacheConfigs.DEFAULT_CACHE_TTL_SECONDS,
    ResourceCacheConfigs.RESOURCE_CACHE_MAX_SIZE -> ResourceCacheConfigs.DEFAULT_RESOURCE_CACHE_MAX_SIZE,
    ResourceCacheConfigs.RESOURCE_CACHE_TTL_SECONDS -> ResourceCacheConfigs.DEFAULT_RESOURCE_CACHE_TTL_SECONDS,
    MutatorConfigs.MAX_RETRY_NUMBER -> MutatorConfigs.DEFAULT_MAX_RETRY_NUMBER,
    MutatorConfigs.LOCK_EXPIRE_TIME -> MutatorConfigs.DEFAULT_LOCK_EXPIRE_TIME,
    MutatorConfigs.MAX_BACK_OFF -> MutatorConfigs.DEFAULT_MAX_BACK_OFF,
    MutatorConfigs.BACK_OFF_TIMEOUT -> MutatorConfigs.DEFAULT_BACK_OFF_TIMEOUT,
    MutatorConfigs.HBASE_FAIL_PROB -> MutatorConfigs.DEFAULT_HBASE_FAIL_PROB,
    MutatorConfigs.DELETE_ALL_FETCH_SIZE -> MutatorConfigs.DEFAULT_DELETE_ALL_FETCH_SIZE,
    MutatorConfigs.DELETE_ALL_FETCH_COUNT -> MutatorConfigs.DEFAULT_DELETE_ALL_FETCH_COUNT,
    FutureCacheConfigs.FUTURE_CACHE_MAX_SIZE -> FutureCacheConfigs.DEFAULT_FUTURE_CACHE_MAX_SIZE,
    FutureCacheConfigs.FUTURE_CACHE_EXPIRE_AFTER_WRITE -> FutureCacheConfigs.DEFAULT_FUTURE_CACHE_EXPIRE_AFTER_WRITE,
    FutureCacheConfigs.FUTURE_CACHE_EXPIRE_AFTER_ACCESS -> FutureCacheConfigs.DEFAULT_FUTURE_CACHE_EXPIRE_AFTER_ACCESS,
    FutureCacheConfigs.FUTURE_CACHE_METRIC_INTERVAL -> FutureCacheConfigs.DEFAULT_FUTURE_CACHE_METRIC_INTERVAL,
    QueryConfigs.QUERY_HARDLIMIT -> QueryConfigs.DEFAULT_QUERY_HARDLIMIT,
    LogConfigs.QUERY_LOG_SAMPLE_RATE -> LogConfigs.DEFAULT_QUERY_LOG_SAMPLE_RATE
  )

  var DefaultConfig: Config = ConfigFactory.parseMap(DefaultConfigs)
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  val ec = ExecutionContext.fromExecutor(threadPool)
  val resourceManagerEc = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numOfThread))

  val DefaultServiceName = ""
  val DefaultColumnName = "vertex"
  val DefaultLabelName = "_s2graph"

  var hbaseExecutor: ExecutorService = _

  val graphStrategies: TraversalStrategies =
    TraversalStrategies.GlobalCache.getStrategies(classOf[Graph]).addStrategies(S2GraphStepStrategy.instance)

  def toTypeSafeConfig(configuration: Configuration): Config = {
    val m = new mutable.HashMap[String, AnyRef]()
    for {
      key <- configuration.getKeys
      value = configuration.getProperty(key)
    } {
      m.put(key, value)
    }
    val config = ConfigFactory.parseMap(m).withFallback(DefaultConfig)
    config
  }

  def fromTypeSafeConfig(config: Config): Configuration = {
    val configuration = new BaseConfiguration()
    for {
      e <- config.entrySet()
    } {
      configuration.setProperty(e.getKey, e.getValue.unwrapped())
    }
    configuration
  }

  def open(configuration: Configuration): S2GraphLike = {
    new S2Graph(configuration)(ec)
  }

  def initStorage(graph: S2GraphLike, config: Config)(ec: ExecutionContext): Storage = {
    val storageBackend = config.getString("s2graph.storage.backend")
    logger.info(s"[InitStorage]: $storageBackend")

    storageBackend match {
      case "hbase" =>
        hbaseExecutor =
          if (config.getString("hbase.zookeeper.quorum") == "localhost")
            AsynchbaseStorage.initLocalHBase(config)
          else
            null

        new AsynchbaseStorage(graph, config)
      case "rocks" => new RocksStorage(graph, config)
      case _ => throw new RuntimeException("not supported storage.")
    }
  }

  def parseCacheConfig(config: Config, prefix: String): Config = {
    import scala.collection.JavaConversions._

    val kvs = new java.util.HashMap[String, AnyRef]()
    for {
      entry <- config.entrySet()
      (k, v) = (entry.getKey, entry.getValue) if k.startsWith(prefix)
    } yield {
      val newKey = k.replace(prefix, "")
      kvs.put(newKey, v.unwrapped())
    }
    ConfigFactory.parseMap(kvs)
  }

  def initMutators(graph: S2GraphLike): Unit = {
    val management = graph.management

    ServiceColumn.findAll().foreach { column =>
      management.updateVertexMutator(column, column.options)
    }

    Label.findAll().foreach { label =>
      management.updateEdgeMutator(label, label.options)
    }
  }

  def initFetchers(graph: S2GraphLike): Unit = {
    val management = graph.management

    ServiceColumn.findAll().foreach { column =>
      management.updateVertexFetcher(column, column.options)
    }

    Label.findAll().foreach { label =>
      management.updateEdgeFetcher(label, label.options)
    }
  }

  def loadFetchersAndMutators(graph: S2GraphLike): Unit = {
    initFetchers(graph)
    initMutators(graph)
  }
}

class S2Graph(_config: Config)(implicit val ec: ExecutionContext) extends S2GraphLike {

  var apacheConfiguration: Configuration = _

  def dbSession() = scalikejdbc.AutoSession

  def this(apacheConfiguration: Configuration)(ec: ExecutionContext) = {
    this(S2Graph.toTypeSafeConfig(apacheConfiguration))(ec)
    this.apacheConfiguration = apacheConfiguration
  }

  private val running = new AtomicBoolean(true)

  override val config = _config.withFallback(S2Graph.DefaultConfig)

  val storageBackend = Try {
    config.getString("s2graph.storage.backend")
  }.getOrElse("hbase")

  Schema.apply(config)
  Schema.loadCache()

  override val management = new Management(this)

  override val resourceManager: ResourceManager = new ResourceManager(this, config)(S2Graph.resourceManagerEc)

  override val indexProvider = IndexProvider.apply(config)

  override val elementBuilder = new GraphElementBuilder(this)

  override val traversalHelper = new TraversalHelper(this)

  private def confWithFallback(conf: Config): Config = {
    conf.withFallback(config)
  }

  val defaultStorage: Storage = S2Graph.initStorage(this, config)(ec)

  for {
    entry <- config.entrySet() if S2Graph.DefaultConfigs.contains(entry.getKey)
    (k, v) = (entry.getKey, entry.getValue)
  } logger.info(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")

  /**
    * TODO: we need to some way to handle malformed configuration for storage.
    */
  val storagePool: scala.collection.mutable.Map[String, Storage] = {
    val labels = Label.findAll()
    val services = Service.findAll()

    val labelConfigs = labels.flatMap(_.toStorageConfig)
    val serviceConfigs = services.flatMap(_.toStorageConfig)

    val configs = (labelConfigs ++ serviceConfigs).map { conf =>
      confWithFallback(conf)
    }.toSet

    val pools = new java.util.HashMap[Config, Storage]()
    configs.foreach { config =>
      pools.put(config, S2Graph.initStorage(this, config)(ec))
    }

    val m = new java.util.concurrent.ConcurrentHashMap[String, Storage]()

    labels.foreach { label =>
      if (label.storageConfigOpt.isDefined) {
        m += (s"label:${label.label}" -> pools(label.storageConfigOpt.get))
      }
    }

    services.foreach { service =>
      if (service.storageConfigOpt.isDefined) {
        m += (s"service:${service.serviceName}" -> pools(service.storageConfigOpt.get))
      }
    }

    m
  }

  override def getStorage(service: Service): Storage = {
    storagePool.getOrElse(s"service:${service.serviceName}", defaultStorage)
  }

  override def getStorage(label: Label): Storage = {
    storagePool.getOrElse(s"label:${label.label}", defaultStorage)
  }

  /* Currently, each getter on Fetcher and Mutator missing proper implementation
  *  Please discuss what is proper way to maintain resources here and provide
  *  right implementation(S2GRAPH-213).
  * */
  override def getVertexFetcher(column: ServiceColumn): VertexFetcher = {
    resourceManager.getOrElseUpdateVertexFetcher(column)
      .getOrElse(defaultStorage.vertexFetcher)
  }

  override def getEdgeFetcher(label: Label): EdgeFetcher = {
    resourceManager.getOrElseUpdateEdgeFetcher(label)
      .getOrElse(defaultStorage.edgeFetcher)
  }

  override def getAllVertexFetchers(): Seq[VertexFetcher] = {
    resourceManager.getAllVertexFetchers()
  }

  override def getAllEdgeFetchers(): Seq[EdgeFetcher] = {
    resourceManager.getAllEdgeFetchers()
  }

  override def getVertexMutator(column: ServiceColumn): VertexMutator = {
    resourceManager.getOrElseUpdateVertexMutator(column)
      .getOrElse(defaultStorage.vertexMutator)
  }

  override def getEdgeMutator(label: Label): EdgeMutator = {
    resourceManager.getOrElseUpdateEdgeMutator(label)
      .getOrElse(defaultStorage.edgeMutator)
  }

  //TODO:
  override def getOptimisticEdgeFetcher(label: Label): OptimisticEdgeFetcher = {
//    getStorage(label).optimisticEdgeFetcher
    null
  }

  //TODO:
  override def flushStorage(): Unit = {
    storagePool.foreach { case (_, storage) =>

      /* flush is blocking */
      storage.flush()
    }
  }

  override def shutdown(modelDataDelete: Boolean = false): Unit =
    if (running.compareAndSet(true, false)) {
      flushStorage()
      Schema.shutdown(modelDataDelete)
      defaultStorage.shutdown()
      localLongId.set(0l)
    }

  def searchVertices(queryParam: VertexQueryParam): Future[Seq[S2VertexLike]] = {
    val matchedVertices = indexProvider.fetchVertexIdsAsyncRaw(queryParam).map { vids =>
      (queryParam.vertexIds ++ vids).distinct.map(vid => elementBuilder.newVertex(vid))
    }

    if (queryParam.fetchProp) matchedVertices.flatMap(vs => getVertices(vs))
    else matchedVertices
  }

  override def getVertices(vertices: Seq[S2VertexLike]): Future[Seq[S2VertexLike]] = {
    val verticesWithIdx = vertices.zipWithIndex
    val futures = verticesWithIdx.groupBy { case (v, idx) => v.serviceColumn }.map { case (serviceColumn, vertexGroup) =>
      getVertexFetcher(serviceColumn).fetchVertices(vertices).map(_.zip(vertexGroup.map(_._2)))
    }

    Future.sequence(futures).map { ls =>
      ls.flatten.toSeq.sortBy(_._2).map(_._1)
    }
  }

  override def checkEdges(edges: Seq[S2EdgeLike]): Future[StepResult] = {
    val futures = for {
      edge <- edges
    } yield {
      getOptimisticEdgeFetcher(edge.innerLabel).fetchSnapshotEdgeInner(edge).map { case (edgeOpt, _) =>
        edgeOpt.toSeq.map(e => EdgeWithScore(e, 1.0, edge.innerLabel))
      }
    }

    Future.sequence(futures).map { edgeWithScoreLs =>
      val edgeWithScores = edgeWithScoreLs.flatten
      StepResult(edgeWithScores = edgeWithScores, grouped = Nil, degreeEdges = Nil)
    }
  }

  override def mutateVertices(vertices: Seq[S2VertexLike], withWait: Boolean = false): Future[Seq[MutateResponse]] = {
    def mutateVertices(storage: Storage)(zkQuorum: String, vertices: Seq[S2VertexLike],
                                         withWait: Boolean = false): Future[Seq[MutateResponse]] = {
      val futures = vertices.map { vertex =>
        getVertexMutator(vertex.serviceColumn).mutateVertex(zkQuorum, vertex, withWait)
      }
      Future.sequence(futures)
    }

    val verticesWithIdx = vertices.zipWithIndex
    val futures = verticesWithIdx.groupBy { case (v, idx) => v.service }.map { case (service, vertexGroup) =>
      mutateVertices(getStorage(service))(service.cluster, vertexGroup.map(_._1), withWait).map(_.zip(vertexGroup.map(_._2)))
    }

    indexProvider.mutateVerticesAsync(vertices)
    Future.sequence(futures).map{ ls =>
      ls.flatten.toSeq.sortBy(_._2).map(_._1)
    }
  }

  override def mutateEdges(edges: Seq[S2EdgeLike], withWait: Boolean = false): Future[Seq[MutateResponse]] = {
    val edgeWithIdxs = edges.zipWithIndex

    val (strongEdges, weakEdges) =
      edgeWithIdxs.partition { case (edge, idx) =>
        val e = edge
        e.innerLabel.consistencyLevel == "strong" && e.getOp() != GraphUtil.operations("insertBulk")
      }

    val weakEdgesFutures = weakEdges.groupBy { case (edge, idx) => edge.innerLabel.hbaseZkAddr }.map { case (zkQuorum, edgeWithIdxs) =>
      val futures = edgeWithIdxs.groupBy(_._1.innerLabel).map { case (label, edgeGroup) =>
        val mutator = getEdgeMutator(label)
        val edges = edgeGroup.map(_._1)
        val idxs = edgeGroup.map(_._2)

        /* multiple edges with weak consistency level will be processed as batch */
        mutator.mutateWeakEdges(zkQuorum, edges, withWait)
      }
      Future.sequence(futures)
    }
    val (strongDeleteAll, strongEdgesAll) = strongEdges.partition { case (edge, idx) => edge.getOp() == GraphUtil.operations("deleteAll") }

    val deleteAllFutures = strongDeleteAll.map { case (edge, idx) =>
      deleteAllAdjacentEdges(Seq(edge.srcVertex), Seq(edge.innerLabel), edge.getDir(), edge.ts).map(idx -> _)
    }

    val strongEdgesFutures = strongEdgesAll.groupBy { case (edge, idx) => edge.innerLabel }.map { case (label, edgeGroup) =>
      val edges = edgeGroup.map(_._1)
      val idxs = edgeGroup.map(_._2)
      val mutator = getEdgeMutator(label)
      val zkQuorum = label.hbaseZkAddr

      mutator.mutateStrongEdges(zkQuorum, edges, withWait = true).map { rets =>
        idxs.zip(rets)
      }
    }

    for {
      weak <- Future.sequence(weakEdgesFutures)
      deleteAll <- Future.sequence(deleteAllFutures)
      strong <- Future.sequence(strongEdgesFutures)
    } yield {
      (deleteAll ++ weak.flatten.flatten ++ strong.flatten).sortBy(_._1).map(r => new MutateResponse(r._2))
    }
  }

  override def mutateElements(elements: Seq[GraphElement],
                              withWait: Boolean = false): Future[Seq[MutateResponse]] = {

    val edgeBuffer = ArrayBuffer[(S2EdgeLike, Int)]()
    val vertexBuffer = ArrayBuffer[(S2VertexLike, Int)]()

    elements.zipWithIndex.foreach {
      case (e: S2EdgeLike, idx: Int) => edgeBuffer.append((e, idx))
      case (v: S2VertexLike, idx: Int) => vertexBuffer.append((v, idx))
      case any@_ => logger.error(s"Unknown type: ${any}")
    }

    val edgeFutureWithIdx = mutateEdges(edgeBuffer.map(_._1), withWait).map { result =>
      edgeBuffer.map(_._2).zip(result)
    }

    val vertexFutureWithIdx = mutateVertices(vertexBuffer.map(_._1), withWait).map { result =>
      vertexBuffer.map(_._2).zip(result)
    }

    val graphFuture = for {
      edgesMutated <- edgeFutureWithIdx
      verticesMutated <- vertexFutureWithIdx
    } yield (edgesMutated ++ verticesMutated).sortBy(_._1).map(_._2)

    graphFuture

  }

  override def getEdges(q: Query): Future[StepResult] = {
    Try {
      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        fallback
      } else {
        val filterOutFuture = q.queryOption.filterOutQuery match {
          case None => fallback
          case Some(filterOutQuery) => traversalHelper.getEdgesStepInner(filterOutQuery)
        }
        for {
          stepResult <- traversalHelper.getEdgesStepInner(q)
          filterOutInnerResult <- filterOutFuture
        } yield {
          if (filterOutInnerResult.isEmpty) stepResult
          else StepResult.filterOut(this, q.queryOption, stepResult, filterOutInnerResult)
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        fallback
    } get
  }

  override def getEdgesMultiQuery(mq: MultiQuery): Future[StepResult] = {
    Try {
      if (mq.queries.isEmpty) fallback
      else {
        val filterOutFuture = mq.queryOption.filterOutQuery match {
          case None => fallback
          case Some(filterOutQuery) => traversalHelper.getEdgesStepInner(filterOutQuery)
        }

        val multiQueryFutures = Future.sequence(mq.queries.map { query => getEdges(query) })
        for {
          multiQueryResults <- multiQueryFutures
          filterOutInnerResult <- filterOutFuture
        } yield {
          StepResult.merges(mq.queryOption, multiQueryResults, mq.weights, filterOutInnerResult)
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        fallback
    } get
  }

  override def deleteAllAdjacentEdges(srcVertices: Seq[S2VertexLike],
                                      labels: Seq[Label],
                                      dir: Int,
                                      ts: Long): Future[Boolean] = {
    val requestTs = ts
    val vertices = srcVertices
    /* create query per label */
    val queries = for {
      label <- labels
    } yield {
      val queryParam = QueryParam(labelName = label.label, direction = GraphUtil.fromDirection(dir),
        offset = 0, limit = DeleteAllFetchSize, duplicatePolicy = DuplicatePolicy.Raw)
      val step = Step(List(queryParam))
      Query(vertices, Vector(step))
    }

    val retryFuture = Extensions.retryOnSuccess(DeleteAllFetchCount) {
      traversalHelper.fetchAndDeleteAll(queries, requestTs)
    } { case (allDeleted, deleteSuccess) =>
      allDeleted && deleteSuccess
    }.map { case (allDeleted, deleteSuccess) => allDeleted && deleteSuccess }

    retryFuture onFailure {
      case ex =>
        logger.error(s"[Error]: deleteAllAdjacentEdges failed.")
    }

    retryFuture
  }

  override def incrementCounts(edges: Seq[S2EdgeLike], withWait: Boolean): Future[Seq[MutateResponse]] = {
    val edgesWithIdx = edges.zipWithIndex
    val futures = edgesWithIdx.groupBy { case (e, idx) => e.innerLabel }.map { case (label, edgeGroup) =>
      getEdgeMutator(label).incrementCounts(label.hbaseZkAddr, edgeGroup.map(_._1), withWait).map(_.zip(edgeGroup.map(_._2)))
    }

    Future.sequence(futures).map { ls =>
      ls.flatten.toSeq.sortBy(_._2).map(_._1)
    }
  }

  override def updateDegree(edge: S2EdgeLike, degreeVal: Long = 0): Future[MutateResponse] = {
    val label = edge.innerLabel
    val mutator = getEdgeMutator(label)

    mutator.updateDegree(label.hbaseZkAddr, edge, degreeVal)
  }

  override def getVertex(vertexId: VertexId): Option[S2VertexLike] = {
    val v = elementBuilder.newVertex(vertexId)
    Await.result(getVertices(Seq(v)).map { vertices => vertices.headOption }, WaitTimeout)
  }

  override def fetchEdges(vertex: S2VertexLike, labelNameWithDirs: Seq[(String, String)]): util.Iterator[Edge] = {
    Await.result(traversalHelper.fetchEdgesAsync(vertex, labelNameWithDirs), WaitTimeout)
  }

  override def edgesAsync(vertex: S2VertexLike, direction: Direction, labelNames: String*): Future[util.Iterator[Edge]] = {
    val labelNameWithDirs =
      if (labelNames.isEmpty) {
        // TODO: Let's clarify direction
        if (direction == Direction.BOTH) {
          Label.findBySrcColumnId(vertex.id.colId).map(l => l.label -> Direction.OUT.name) ++
            Label.findByTgtColumnId(vertex.id.colId).map(l => l.label -> Direction.IN.name)
        } else if (direction == Direction.IN) {
          Label.findByTgtColumnId(vertex.id.colId).map(l => l.label -> direction.name)
        } else {
          Label.findBySrcColumnId(vertex.id.colId).map(l => l.label -> direction.name)
        }
      } else {
        direction match {
          case Direction.BOTH =>
            Seq(Direction.OUT, Direction.IN).flatMap { dir => labelNames.map(_ -> dir.name()) }
          case _ => labelNames.map(_ -> direction.name())
        }
      }

    traversalHelper.fetchEdgesAsync(vertex, labelNameWithDirs.distinct)
  }

  def isRunning(): Boolean = running.get()
}
