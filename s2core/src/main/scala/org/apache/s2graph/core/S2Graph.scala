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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import org.apache.s2graph.core.index.IndexProvider
import org.apache.s2graph.core.io.tinkerpop.optimize.S2GraphStepStrategy
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorage
import org.apache.s2graph.core.storage.rocks.RocksStorage
import org.apache.s2graph.core.storage.{MutateResponse, Storage}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.{DeferCache, Extensions, logger}
import org.apache.tinkerpop.gremlin.process.traversal.{P, TraversalStrategies}
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, Graph}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Random, Try}


object S2Graph {

  type HashKey = (Int, Int, Int, Int, Boolean)
  type FilterHashKey = (Int, Int)

  val DefaultScore = 1.0
  val FetchAllLimit = 10000000
  val DefaultFetchLimit = 1000

  private val DefaultConfigs: Map[String, AnyRef] = Map(
    "hbase.zookeeper.quorum" -> "localhost",
    "hbase.table.name" -> "s2graph",
    "hbase.table.compression.algorithm" -> "gz",
    "phase" -> "dev",
    "db.default.driver" -> "org.h2.Driver",
    "db.default.url" -> "jdbc:h2:file:./var/metastore;MODE=MYSQL",
    "db.default.password" -> "graph",
    "db.default.user" -> "graph",
    "cache.max.size" -> java.lang.Integer.valueOf(0),
    "cache.ttl.seconds" -> java.lang.Integer.valueOf(-1),
    "hbase.client.retries.number" -> java.lang.Integer.valueOf(20),
    "hbase.rpcs.buffered_flush_interval" -> java.lang.Short.valueOf(100.toShort),
    "hbase.rpc.timeout" -> java.lang.Integer.valueOf(600000),
    "max.retry.number" -> java.lang.Integer.valueOf(100),
    "lock.expire.time" -> java.lang.Integer.valueOf(1000 * 60 * 10),
    "max.back.off" -> java.lang.Integer.valueOf(100),
    "back.off.timeout" -> java.lang.Integer.valueOf(1000),
    "hbase.fail.prob" -> java.lang.Double.valueOf(-0.1),
    "delete.all.fetch.size" -> java.lang.Integer.valueOf(1000),
    "delete.all.fetch.count" -> java.lang.Integer.valueOf(200),
    "future.cache.max.size" -> java.lang.Integer.valueOf(100000),
    "future.cache.expire.after.write" -> java.lang.Integer.valueOf(10000),
    "future.cache.expire.after.access" -> java.lang.Integer.valueOf(5000),
    "future.cache.metric.interval" -> java.lang.Integer.valueOf(60000),
    "query.future.cache.max.size" -> java.lang.Integer.valueOf(1000),
    "query.future.cache.expire.after.write" -> java.lang.Integer.valueOf(10000),
    "query.future.cache.expire.after.access" -> java.lang.Integer.valueOf(5000),
    "query.future.cache.metric.interval" -> java.lang.Integer.valueOf(60000),
    "s2graph.storage.backend" -> "hbase",
    "query.hardlimit" -> java.lang.Integer.valueOf(100000),
    "hbase.zookeeper.znode.parent" -> "/hbase",
    "query.log.sample.rate" -> Double.box(0.05)
  )

  var DefaultConfig: Config = ConfigFactory.parseMap(DefaultConfigs)
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  val ec = ExecutionContext.fromExecutor(threadPool)

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

  Model.apply(config)
  Model.loadCache()

  override val management = new Management(this)

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

  override def flushStorage(): Unit = {
    storagePool.foreach { case (_, storage) =>

      /* flush is blocking */
      storage.flush()
    }
  }

  override def shutdown(modelDataDelete: Boolean = false): Unit =
    if (running.compareAndSet(true, false)) {
      flushStorage()
      Model.shutdown(modelDataDelete)
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
    val futures = verticesWithIdx.groupBy { case (v, idx) => v.service }.map { case (service, vertexGroup) =>
      getStorage(service).fetchVertices(vertices).map(_.zip(vertexGroup.map(_._2)))
    }

    Future.sequence(futures).map { ls =>
      ls.flatten.toSeq.sortBy(_._2).map(_._1)
    }
  }

  override def checkEdges(edges: Seq[S2EdgeLike]): Future[StepResult] = {
    val futures = for {
      edge <- edges
    } yield {
      getStorage(edge.innerLabel).fetchSnapshotEdgeInner(edge).map { case (edgeOpt, _) =>
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
        storage.mutateVertex(zkQuorum, vertex, withWait)
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
        val storage = getStorage(label)
        val edges = edgeGroup.map(_._1)
        val idxs = edgeGroup.map(_._2)

        /* multiple edges with weak consistency level will be processed as batch */
        storage.mutateWeakEdges(zkQuorum, edges, withWait)
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
      val storage = getStorage(label)
      val zkQuorum = label.hbaseZkAddr
      storage.mutateStrongEdges(zkQuorum, edges, withWait = true).map { rets =>
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
      getStorage(label).incrementCounts(label.hbaseZkAddr, edgeGroup.map(_._1), withWait).map(_.zip(edgeGroup.map(_._2)))
    }

    Future.sequence(futures).map { ls =>
      ls.flatten.toSeq.sortBy(_._2).map(_._1)
    }
  }

  override def updateDegree(edge: S2EdgeLike, degreeVal: Long = 0): Future[MutateResponse] = {
    val label = edge.innerLabel
    val storage = getStorage(label)

    storage.updateDegree(label.hbaseZkAddr, edge, degreeVal)
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
