package com.kakao.s2graph.core

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.parsers.WhereParser
import com.kakao.s2graph.core.storage.Storage
import com.kakao.s2graph.core.storage.hbase._
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.logger
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.util.Try

object Graph {
  val DefaultScore = 1.0

  private val DefaultConfigs: Map[String, AnyRef] = Map(
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
    "max.retry.number" -> java.lang.Integer.valueOf(100),
    "max.back.off" -> java.lang.Integer.valueOf(100),
    "hbase.fail.prob" -> java.lang.Double.valueOf(-0.1),
    "delete.all.fetch.size" -> java.lang.Integer.valueOf(1000),
    "future.cache.max.size" -> java.lang.Integer.valueOf(100000),
    "future.cache.expire.after.write" -> java.lang.Integer.valueOf(10000),
    "future.cache.expire.after.access" -> java.lang.Integer.valueOf(5000)
  )

  var DefaultConfig: Config = ConfigFactory.parseMap(DefaultConfigs)

  /** helpers for filterEdges */
  type HashKey = (Int, Int, Int, Int, Boolean)
  type FilterHashKey = (Int, Int)
  type Result = (ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]],
    ConcurrentHashMap[HashKey, (FilterHashKey, Edge, Double)],
    ListBuffer[(HashKey, FilterHashKey, Edge, Double)])

  def toHashKey(queryParam: QueryParam, edge: Edge, isDegree: Boolean): (HashKey, FilterHashKey) = {
    val src = edge.srcVertex.innerId.hashCode()
    val tgt = edge.tgtVertex.innerId.hashCode()
    val hashKey = (src, edge.labelWithDir.labelId, edge.labelWithDir.dir, tgt, isDegree)
    val filterHashKey = (src, tgt)

    (hashKey, filterHashKey)
  }

  def alreadyVisitedVertices(queryResultLs: Seq[QueryResult]): Map[(LabelWithDirection, Vertex), Boolean] = {
    val vertices = for {
      queryResult <- queryResultLs
      edgeWithScore <- queryResult.edgeWithScoreLs
      edge = edgeWithScore.edge
      vertex = if (edge.labelWithDir.dir == GraphUtil.directions("out")) edge.tgtVertex else edge.srcVertex
    } yield (edge.labelWithDir, vertex) -> true

    vertices.toMap
  }

  /** common methods for filter out, transform, aggregate queryResult */
  def convertEdges(queryParam: QueryParam, edge: Edge, nextStepOpt: Option[Step]): Seq[Edge] = {
    for {
      convertedEdge <- queryParam.transformer.transform(edge, nextStepOpt) if !edge.isDegree
    } yield convertedEdge
  }

  def processTimeDecay(queryParam: QueryParam, edge: Edge) = {
    /** process time decay */
    val tsVal = queryParam.timeDecay match {
      case None => 1.0
      case Some(timeDecay) =>
        val timeDiff = queryParam.timestamp - edge.ts
        timeDecay.decay(timeDiff)
    }

    tsVal
  }

  def aggregateScore(newScore: Double,
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

  def aggregateResults(queryRequestWithResult: QueryRequestWithResult,
                       queryParamResult: Result,
                       edgesToInclude: util.HashSet[FilterHashKey],
                       edgesToExclude: util.HashSet[FilterHashKey]): QueryRequestWithResult = {
    val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
    val (query, stepIdx, _, queryParam) = QueryRequest.unapply(queryRequest).get

    val (duplicateEdges, resultEdges, edgeWithScoreSorted) = queryParamResult
    val edgesWithScores = for {
      (hashKey, filterHashKey, edge, _) <- edgeWithScoreSorted if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
      score = resultEdges.get(hashKey)._3
      (duplicateEdge, aggregatedScore) <- fetchDuplicatedEdges(edge, score, hashKey, duplicateEdges) if aggregatedScore >= queryParam.threshold
    } yield EdgeWithScore(duplicateEdge, aggregatedScore)

    QueryRequestWithResult(queryRequest, QueryResult(edgesWithScores))
  }

  def fetchDuplicatedEdges(edge: Edge,
                           score: Double,
                           hashKey: HashKey,
                           duplicateEdges: ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]]) = {
    (edge -> score) +: (if (duplicateEdges.containsKey(hashKey)) duplicateEdges.get(hashKey) else Seq.empty)
  }

  def queryResultWithFilter(queryRequestWithResult: QueryRequestWithResult) = {
    val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
    val (_, _, _, queryParam) = QueryRequest.unapply(queryRequest).get
    val whereFilter = queryParam.where.get
    if (whereFilter == WhereParser.success) queryResult.edgeWithScoreLs
    else queryResult.edgeWithScoreLs.withFilter(edgeWithScore => whereFilter.filter(edgeWithScore.edge))
  }

  def filterEdges(queryResultLsFuture: Future[Seq[QueryRequestWithResult]],
                  alreadyVisited: Map[(LabelWithDirection, Vertex), Boolean] = Map.empty[(LabelWithDirection, Vertex), Boolean])
                 (implicit ec: scala.concurrent.ExecutionContext): Future[Seq[QueryRequestWithResult]] = {

    queryResultLsFuture.map { queryRequestWithResultLs =>
      if (queryRequestWithResultLs.isEmpty) Nil
      else {
        val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResultLs.head).get
        val (q, stepIdx, srcVertex, queryParam) = QueryRequest.unapply(queryRequest).get
        val step = q.steps(stepIdx)

        val nextStepOpt = if (stepIdx < q.steps.size - 1) Option(q.steps(stepIdx + 1)) else None

        val excludeLabelWithDirSet = new util.HashSet[(Int, Int)]
        val includeLabelWithDirSet = new util.HashSet[(Int, Int)]
        step.queryParams.filter(_.exclude).foreach(l => excludeLabelWithDirSet.add(l.labelWithDir.labelId -> l.labelWithDir.dir))
        step.queryParams.filter(_.include).foreach(l => includeLabelWithDirSet.add(l.labelWithDir.labelId -> l.labelWithDir.dir))

        val edgesToExclude = new util.HashSet[FilterHashKey]()
        val edgesToInclude = new util.HashSet[FilterHashKey]()

        val queryParamResultLs = new ListBuffer[Result]
        queryRequestWithResultLs.foreach { queryRequestWithResult =>
          val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
          val queryParam = queryRequest.queryParam
          val duplicateEdges = new util.concurrent.ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]]()
          val resultEdges = new util.concurrent.ConcurrentHashMap[HashKey, (FilterHashKey, Edge, Double)]()
          val edgeWithScoreSorted = new ListBuffer[(HashKey, FilterHashKey, Edge, Double)]
          val labelWeight = step.labelWeights.getOrElse(queryParam.labelWithDir.labelId, 1.0)

          // store degree value with Array.empty so if degree edge exist, it comes at very first.
          def checkDegree() = queryResult.edgeWithScoreLs.headOption.exists { edgeWithScore =>
            edgeWithScore.edge.isDegree
          }
          var isDegree = checkDegree()

          val includeExcludeKey = queryParam.labelWithDir.labelId -> queryParam.labelWithDir.dir
          val shouldBeExcluded = excludeLabelWithDirSet.contains(includeExcludeKey)
          val shouldBeIncluded = includeLabelWithDirSet.contains(includeExcludeKey)

          queryResultWithFilter(queryRequestWithResult).foreach { edgeWithScore =>
            val (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
            if (queryParam.transformer.isDefault) {
              val convertedEdge = edge

              val (hashKey, filterHashKey) = toHashKey(queryParam, convertedEdge, isDegree)

              /** check if this edge should be exlcuded. */
              if (shouldBeExcluded && !isDegree) {
                edgesToExclude.add(filterHashKey)
              } else {
                if (shouldBeIncluded && !isDegree) {
                  edgesToInclude.add(filterHashKey)
                }
                val tsVal = processTimeDecay(queryParam, convertedEdge)
                val newScore = labelWeight * score * tsVal
                aggregateScore(newScore, resultEdges, duplicateEdges, edgeWithScoreSorted, hashKey, filterHashKey, queryParam, convertedEdge)
              }
            } else {
              convertEdges(queryParam, edge, nextStepOpt).foreach { convertedEdge =>
                val (hashKey, filterHashKey) = toHashKey(queryParam, convertedEdge, isDegree)

                /** check if this edge should be exlcuded. */
                if (shouldBeExcluded && !isDegree) {
                  edgesToExclude.add(filterHashKey)
                } else {
                  if (shouldBeIncluded && !isDegree) {
                    edgesToInclude.add(filterHashKey)
                  }
                  val tsVal = processTimeDecay(queryParam, convertedEdge)
                  val newScore = labelWeight * score * tsVal
                  aggregateScore(newScore, resultEdges, duplicateEdges, edgeWithScoreSorted, hashKey, filterHashKey, queryParam, convertedEdge)
                }
              }
            }
            isDegree = false
          }
          val ret = (duplicateEdges, resultEdges, edgeWithScoreSorted)
          queryParamResultLs.append(ret)
        }

        val aggregatedResults = for {
          (queryRequestWithResult, queryParamResult) <- queryRequestWithResultLs.zip(queryParamResultLs)
        } yield {
            aggregateResults(queryRequestWithResult, queryParamResult, edgesToInclude, edgesToExclude)
          }

        aggregatedResults
      }
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

class Graph(_config: Config)(implicit val ec: ExecutionContext) {
  val config = _config.withFallback(Graph.DefaultConfig)
  val cacheSize = config.getInt("cache.max.size")
//  val cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build[java.lang.Integer, Seq[QueryResult]]()
  val vertexCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build[java.lang.Integer, Option[Vertex]]()

  Model.apply(config)
  Model.loadCache()

  // TODO: Make storage client by config param
  val storage: Storage = new AsynchbaseStorage(config, vertexCache)(ec)

  for {
    entry <- config.entrySet() if Graph.DefaultConfigs.contains(entry.getKey)
    (k, v) = (entry.getKey, entry.getValue)
  } logger.info(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")

  /** select */
  def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = storage.checkEdges(params)

  def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = storage.getEdges(q)

  def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = storage.getVertices(vertices)

  /** write */
  def deleteAllAdjacentEdges(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] =
    storage.deleteAllAdjacentEdges(srcVertices, labels, dir, ts)

  def mutateElements(elements: Seq[GraphElement], withWait: Boolean = false): Future[Seq[Boolean]] =
    storage.mutateElements(elements, withWait)

  def mutateEdges(edges: Seq[Edge], withWait: Boolean = false): Future[Seq[Boolean]] = storage.mutateEdges(edges, withWait)

  def mutateVertices(vertices: Seq[Vertex], withWait: Boolean = false): Future[Seq[Boolean]] = storage.mutateVertices(vertices, withWait)

  def incrementCounts(edges: Seq[Edge]): Future[Seq[(Boolean, Long)]] = storage.incrementCounts(edges)

  def shutdown(): Unit = storage.flush()
}
