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
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.Configuration
import org.apache.s2graph.core.GraphExceptions.{FetchTimeoutException, LabelNotExistException}
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorage
import org.apache.s2graph.core.storage.{SKeyValue, Storage}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.{DeferCache, Extensions, logger}
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure
import org.apache.tinkerpop.gremlin.structure.Graph.Variables
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.apache.tinkerpop.gremlin.structure.{Edge, Graph, T, Transaction}
import play.api.libs.json.{JsObject, Json}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Random, Try}


object S2Graph {

  type HashKey = (Int, Int, Int, Int, Boolean)
  type FilterHashKey = (Int, Int)


  val DefaultScore = 1.0

  private val DefaultConfigs: Map[String, AnyRef] = Map(
    "hbase.zookeeper.quorum" -> "localhost",
    "hbase.table.name" -> "s2graph",
    "hbase.table.compression.algorithm" -> "gz",
    "phase" -> "dev",
    "db.default.driver" ->  "org.h2.Driver",
    "db.default.url" -> "jdbc:h2:file:./var/metastore;MODE=MYSQL",
    "db.default.password" -> "graph",
    "db.default.user" -> "graph",
    "cache.max.size" -> java.lang.Integer.valueOf(10000),
    "cache.ttl.seconds" -> java.lang.Integer.valueOf(60),
    "hbase.client.retries.number" -> java.lang.Integer.valueOf(20),
    "hbase.rpcs.buffered_flush_interval" -> java.lang.Short.valueOf(100.toShort),
    "hbase.rpc.timeout" -> java.lang.Integer.valueOf(1000),
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



  def initStorage(graph: S2Graph, config: Config)(ec: ExecutionContext): Storage[_, _] = {
    val storageBackend = config.getString("s2graph.storage.backend")
    logger.info(s"[InitStorage]: $storageBackend")

    storageBackend match {
      case "hbase" => new AsynchbaseStorage(graph, config)(ec)
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

  /** Global helper functions */
  @tailrec
  final def randomInt(sampleNumber: Int, range: Int, set: Set[Int] = Set.empty[Int]): Set[Int] = {
    if (range < sampleNumber || set.size == sampleNumber) set
    else randomInt(sampleNumber, range, set + Random.nextInt(range))
  }

  def sample(queryRequest: QueryRequest, edges: Seq[EdgeWithScore], n: Int): Seq[EdgeWithScore] = {
    if (edges.size <= n) {
      edges
    } else {
      val plainEdges = if (queryRequest.queryParam.offset == 0) {
        edges.tail
      } else edges

      val randoms = randomInt(n, plainEdges.size)
      var samples = List.empty[EdgeWithScore]
      var idx = 0
      plainEdges.foreach { e =>
        if (randoms.contains(idx)) samples = e :: samples
        idx += 1
      }
      samples
    }
  }

  def normalize(edgeWithScores: Seq[EdgeWithScore]): Seq[EdgeWithScore] = {
    val sum = edgeWithScores.foldLeft(0.0) { case (acc, cur) => acc + cur.score }
    edgeWithScores.map { edgeWithScore =>
      edgeWithScore.copy(score = edgeWithScore.score / sum)
    }
  }

  def alreadyVisitedVertices(edgeWithScoreLs: Seq[EdgeWithScore]): Map[(LabelWithDirection, S2Vertex), Boolean] = {
    val vertices = for {
      edgeWithScore <- edgeWithScoreLs
      edge = edgeWithScore.edge
      vertex = if (edge.labelWithDir.dir == GraphUtil.directions("out")) edge.tgtVertex else edge.srcVertex
    } yield (edge.labelWithDir, vertex) -> true

    vertices.toMap
  }

  /** common methods for filter out, transform, aggregate queryResult */
  def convertEdges(queryParam: QueryParam, edge: S2Edge, nextStepOpt: Option[Step]): Seq[S2Edge] = {
    for {
      convertedEdge <- queryParam.edgeTransformer.transform(queryParam, edge, nextStepOpt) if !edge.isDegree
    } yield convertedEdge
  }

  def processTimeDecay(queryParam: QueryParam, edge: S2Edge) = {
    /* process time decay */
    val tsVal = queryParam.timeDecay match {
      case None => 1.0
      case Some(timeDecay) =>
        val tsVal = try {
          val innerValWithTsOpt = edge.propertyValue(timeDecay.labelMeta.name)
          innerValWithTsOpt.map { innerValWithTs =>
            val innerVal = innerValWithTs.innerVal
            timeDecay.labelMeta.dataType match {
              case InnerVal.LONG => innerVal.value match {
                case n: BigDecimal => n.bigDecimal.longValue()
                case _ => innerVal.toString().toLong
              }
              case _ => innerVal.toString().toLong
            }
          } getOrElse (edge.ts)
        } catch {
          case e: Exception =>
            logger.error(s"processTimeDecay error. ${edge.toLogString}", e)
            edge.ts
        }
        val timeDiff = queryParam.timestamp - tsVal
        timeDecay.decay(timeDiff)
    }

    tsVal
  }

  def processDuplicates[T](queryParam: QueryParam,
                           duplicates: Seq[(FilterHashKey, T)])(implicit ev: WithScore[T]): Seq[(FilterHashKey, T)] = {

    if (queryParam.label.consistencyLevel != "strong") {
      //TODO:
      queryParam.duplicatePolicy match {
        case DuplicatePolicy.First => Seq(duplicates.head)
        case DuplicatePolicy.Raw => duplicates
        case DuplicatePolicy.CountSum =>
          val countSum = duplicates.size
          val (headFilterHashKey, headEdgeWithScore) = duplicates.head
          Seq(headFilterHashKey -> ev.withNewScore(headEdgeWithScore, countSum))
        case _ =>
          val scoreSum = duplicates.foldLeft(0.0) { case (prev, current) => prev + ev.score(current._2) }
          val (headFilterHashKey, headEdgeWithScore) = duplicates.head
          Seq(headFilterHashKey -> ev.withNewScore(headEdgeWithScore, scoreSum))
      }
    } else {
      duplicates
    }
  }

  def toHashKey(queryParam: QueryParam, edge: S2Edge, isDegree: Boolean): (HashKey, FilterHashKey) = {
    val src = edge.srcVertex.innerId.hashCode()
    val tgt = edge.tgtVertex.innerId.hashCode()
    val hashKey = (src, edge.labelWithDir.labelId, edge.labelWithDir.dir, tgt, isDegree)
    val filterHashKey = (src, tgt)

    (hashKey, filterHashKey)
  }

  def filterEdges(q: Query,
                  stepIdx: Int,
                  queryRequests: Seq[QueryRequest],
                  queryResultLsFuture: Future[Seq[StepResult]],
                  queryParams: Seq[QueryParam],
                  alreadyVisited: Map[(LabelWithDirection, S2Vertex), Boolean] = Map.empty,
                  buildLastStepInnerResult: Boolean = true,
                  parentEdges: Map[VertexId, Seq[EdgeWithScore]])
                 (implicit ec: scala.concurrent.ExecutionContext): Future[StepResult] = {

    queryResultLsFuture.map { queryRequestWithResultLs =>
      val (cursors, failCount) = {
        val _cursors = ArrayBuffer.empty[Array[Byte]]
        var _failCount = 0

        queryRequestWithResultLs.foreach { stepResult =>
          _cursors.append(stepResult.cursors: _*)
          _failCount += stepResult.failCount
        }

        _cursors -> _failCount
      }


      if (queryRequestWithResultLs.isEmpty) StepResult.Empty.copy(failCount = failCount)
      else {
        val isLastStep = stepIdx == q.steps.size - 1
        val queryOption = q.queryOption
        val step = q.steps(stepIdx)

        val currentStepResults = queryRequests.view.zip(queryRequestWithResultLs)
        val shouldBuildInnerResults = !isLastStep || buildLastStepInnerResult
        val degrees = queryRequestWithResultLs.flatMap(_.degreeEdges)

        if (shouldBuildInnerResults) {
          val _results = buildResult(q, stepIdx, currentStepResults, parentEdges) { (edgeWithScore, propsSelectColumns) =>
            edgeWithScore
          }

          /** process step group by */
          val results = StepResult.filterOutStepGroupBy(_results, step.groupBy)
          StepResult(edgeWithScores = results, grouped = Nil, degreeEdges = degrees, cursors = cursors, failCount = failCount)

        } else {
          val _results = buildResult(q, stepIdx, currentStepResults, parentEdges) { (edgeWithScore, propsSelectColumns) =>
            val edge = edgeWithScore.edge
            val score = edgeWithScore.score
            val label = edgeWithScore.label

            /** Select */
            val mergedPropsWithTs = edge.propertyValuesInner(propsSelectColumns)

//            val newEdge = edge.copy(propsWithTs = mergedPropsWithTs)
            val newEdge = edge.copyEdgeWithState(mergedPropsWithTs)

            val newEdgeWithScore = edgeWithScore.copy(edge = newEdge)
            /** OrderBy */
            val orderByValues =
             if (queryOption.orderByKeys.isEmpty) (score, edge.tsInnerVal, None, None)
              else StepResult.toTuple4(newEdgeWithScore.toValues(queryOption.orderByKeys))

            /** StepGroupBy */
            val stepGroupByValues = newEdgeWithScore.toValues(step.groupBy.keys)

            /** GroupBy */
            val groupByValues = newEdgeWithScore.toValues(queryOption.groupBy.keys)

            /** FilterOut */
            val filterOutValues = newEdgeWithScore.toValues(queryOption.filterOutFields)

            newEdgeWithScore.copy(orderByValues = orderByValues,
              stepGroupByValues = stepGroupByValues,
              groupByValues = groupByValues,
              filterOutValues = filterOutValues)
          }

          /** process step group by */
          val results = StepResult.filterOutStepGroupBy(_results, step.groupBy)

          /** process ordered list */
          val ordered = if (queryOption.groupBy.keys.isEmpty) StepResult.orderBy(queryOption, results) else Nil

          /** process grouped list */
          val grouped =
          if (queryOption.groupBy.keys.isEmpty) Nil
          else {
            val agg = new scala.collection.mutable.HashMap[StepResult.GroupByKey, (Double, StepResult.Values)]()
            results.groupBy { edgeWithScore =>
              //                edgeWithScore.groupByValues.map(_.map(_.toString))
              edgeWithScore.groupByValues
            }.foreach { case (k, ls) =>
              val (scoreSum, merged) = StepResult.mergeOrdered(ls, Nil, queryOption)

              val newScoreSum = scoreSum

              /**
                * watch out here. by calling toString on Any, we lose type information which will be used
                * later for toJson.
                */
              if (merged.nonEmpty) {
                val newKey = merged.head.groupByValues
                agg += (newKey -> (newScoreSum, merged))
              }
            }
            agg.toSeq.sortBy(_._2._1 * -1)
          }

          StepResult(edgeWithScores = ordered, grouped = grouped, degreeEdges = degrees, cursors = cursors, failCount = failCount)
        }
      }
    }
  }

  private def toEdgeWithScores(queryRequest: QueryRequest,
                               stepResult: StepResult,
                               parentEdges: Map[VertexId, Seq[EdgeWithScore]]): Seq[EdgeWithScore] = {
    val queryOption = queryRequest.query.queryOption
    val queryParam = queryRequest.queryParam
    val prevScore = queryRequest.prevStepScore
    val labelWeight = queryRequest.labelWeight
    val edgeWithScores = stepResult.edgeWithScores

    val shouldBuildParents = queryOption.returnTree || queryParam.whereHasParent
    val parents = if (shouldBuildParents) {
      parentEdges.getOrElse(queryRequest.vertex.id, Nil).map { edgeWithScore =>
        val edge = edgeWithScore.edge
        val score = edgeWithScore.score
        val label = edgeWithScore.label

        /** Select */
        val mergedPropsWithTs =
          if (queryOption.selectColumns.isEmpty) {
            edge.propertyValuesInner()
          } else {
            val initial = Map(LabelMeta.timestamp -> edge.propertyValueInner(LabelMeta.timestamp))
            edge.propertyValues(queryOption.selectColumns) ++ initial
          }

        val newEdge = edge.copyEdgeWithState(mergedPropsWithTs)
        edgeWithScore.copy(edge = newEdge)
      }
    } else Nil

    // skip
    if (queryOption.ignorePrevStepCache) stepResult.edgeWithScores
    else {
      val degreeScore = 0.0

      val sampled =
        if (queryRequest.queryParam.sample >= 0) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
        else edgeWithScores

      val withScores = for {
        edgeWithScore <- sampled
      } yield {
        val edge = edgeWithScore.edge
        val edgeScore = edgeWithScore.score
        val score = queryParam.scorePropagateOp match {
          case "plus" => edgeScore + prevScore
          case "divide" =>
            if ((prevScore + queryParam.scorePropagateShrinkage) == 0) 0
            else edgeScore / (prevScore + queryParam.scorePropagateShrinkage)
          case _ => edgeScore * prevScore
        }

        val tsVal = processTimeDecay(queryParam, edge)
        val newScore = degreeScore + score
        //          val newEdge = if (queryOption.returnTree) edge.copy(parentEdges = parents) else edge
        val newEdge = edge.copy(parentEdges = parents)
        edgeWithScore.copy(edge = newEdge, score = newScore * labelWeight * tsVal)
      }

      val normalized =
        if (queryParam.shouldNormalize) normalize(withScores)
        else withScores

      normalized
    }
  }

  private def buildResult[T](query: Query,
                             stepIdx: Int,
                             stepResultLs: Seq[(QueryRequest, StepResult)],
                             parentEdges: Map[VertexId, Seq[EdgeWithScore]])
                            (createFunc: (EdgeWithScore, Seq[LabelMeta]) => T)
                            (implicit ev: WithScore[T]): ListBuffer[T] = {
    import scala.collection._

    val results = ListBuffer.empty[T]
    val sequentialLs: ListBuffer[(HashKey, FilterHashKey, T, QueryParam)] = ListBuffer.empty
    val duplicates: mutable.HashMap[HashKey, ListBuffer[(FilterHashKey, T)]] = mutable.HashMap.empty
    val edgesToExclude: mutable.HashSet[FilterHashKey] = mutable.HashSet.empty
    val edgesToInclude: mutable.HashSet[FilterHashKey] = mutable.HashSet.empty

    var numOfDuplicates = 0
    val queryOption = query.queryOption
    val step = query.steps(stepIdx)
    val excludeLabelWithDirSet = step.queryParams.filter(_.exclude).map(l => l.labelWithDir).toSet
    val includeLabelWithDirSet = step.queryParams.filter(_.include).map(l => l.labelWithDir).toSet

    stepResultLs.foreach { case (queryRequest, stepInnerResult) =>
      val queryParam = queryRequest.queryParam
      val label = queryParam.label
      val shouldBeExcluded = excludeLabelWithDirSet.contains(queryParam.labelWithDir)
      val shouldBeIncluded = includeLabelWithDirSet.contains(queryParam.labelWithDir)

      val propsSelectColumns = (for {
        column <- queryOption.propsSelectColumns
        labelMeta <- label.metaPropsInvMap.get(column)
      } yield labelMeta)

      for {
        edgeWithScore <- toEdgeWithScores(queryRequest, stepInnerResult, parentEdges)
      } {
        val edge = edgeWithScore.edge
        val (hashKey, filterHashKey) = toHashKey(queryParam, edge, isDegree = false)
        //        params += (hashKey -> queryParam) //

        /** check if this edge should be exlcuded. */
        if (shouldBeExcluded) {
          edgesToExclude.add(filterHashKey)
        } else {
          if (shouldBeIncluded) {
            edgesToInclude.add(filterHashKey)
          }
          val newEdgeWithScore = createFunc(edgeWithScore, propsSelectColumns)

          sequentialLs += ((hashKey, filterHashKey, newEdgeWithScore, queryParam))
          duplicates.get(hashKey) match {
            case None =>
              val newLs = ListBuffer.empty[(FilterHashKey, T)]
              newLs += (filterHashKey -> newEdgeWithScore)
              duplicates += (hashKey -> newLs) //
            case Some(old) =>
              numOfDuplicates += 1
              old += (filterHashKey -> newEdgeWithScore) //
          }
        }
      }
    }


    if (numOfDuplicates == 0) {
      // no duplicates at all.
      for {
        (hashKey, filterHashKey, edgeWithScore, _) <- sequentialLs
        if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
      } {
        results += edgeWithScore
      }
    } else {
      // need to resolve duplicates.
      val seen = new mutable.HashSet[HashKey]()
      for {
        (hashKey, filterHashKey, edgeWithScore, queryParam) <- sequentialLs
        if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
        if !seen.contains(hashKey)
      } {
        //        val queryParam = params(hashKey)
        processDuplicates(queryParam, duplicates(hashKey)).foreach { case (_, duplicate) =>
          if (ev.score(duplicate) >= queryParam.threshold) {
            seen += hashKey
            results += duplicate
          }
        }
      }
    }
    results
  }

}

class S2Graph(_config: Config)(implicit val ec: ExecutionContext) extends Graph {

  import S2Graph._

  val config = _config.withFallback(S2Graph.DefaultConfig)

  Model.apply(config)
  Model.loadCache()

  val MaxRetryNum = config.getInt("max.retry.number")
  val MaxBackOff = config.getInt("max.back.off")
  val BackoffTimeout = config.getInt("back.off.timeout")
  val DeleteAllFetchCount = config.getInt("delete.all.fetch.count")
  val DeleteAllFetchSize = config.getInt("delete.all.fetch.size")
  val FailProb = config.getDouble("hbase.fail.prob")
  val LockExpireDuration = config.getInt("lock.expire.time")
  val MaxSize = config.getInt("future.cache.max.size")
  val ExpireAfterWrite = config.getInt("future.cache.expire.after.write")
  val ExpireAfterAccess = config.getInt("future.cache.expire.after.access")
  val WaitTimeout = Duration(60, TimeUnit.SECONDS)
  val scheduledEx = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  private def confWithFallback(conf: Config): Config = {
    conf.withFallback(config)
  }

  /**
    * TODO: we need to some way to handle malformed configuration for storage.
    */
  val storagePool: scala.collection.mutable.Map[String, Storage[_, _]] = {
    val labels = Label.findAll()
    val services = Service.findAll()

    val labelConfigs = labels.flatMap(_.toStorageConfig)
    val serviceConfigs = services.flatMap(_.toStorageConfig)

    val configs = (labelConfigs ++ serviceConfigs).map { conf =>
      confWithFallback(conf)
    }.toSet

    val pools = new java.util.HashMap[Config, Storage[_, _]]()
    configs.foreach { config =>
      pools.put(config, S2Graph.initStorage(this, config)(ec))
    }

    val m = new java.util.concurrent.ConcurrentHashMap[String, Storage[_, _]]()

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

  val defaultStorage: Storage[_, _] = S2Graph.initStorage(this, config)(ec)

  /** QueryLevel FutureCache */
  val queryFutureCache = new DeferCache[StepResult, Promise, Future](parseCacheConfig(config, "query."), empty = StepResult.Empty)

  for {
    entry <- config.entrySet() if S2Graph.DefaultConfigs.contains(entry.getKey)
    (k, v) = (entry.getKey, entry.getValue)
  } logger.info(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")

  def getStorage(service: Service): Storage[_, _] = {
    storagePool.getOrElse(s"service:${service.serviceName}", defaultStorage)
  }

  def getStorage(label: Label): Storage[_, _] = {
    storagePool.getOrElse(s"label:${label.label}", defaultStorage)
  }

  def flushStorage(): Unit = {
    storagePool.foreach { case (_, storage) =>

      /** flush is blocking */
      storage.flush()
    }
  }

  def fallback = Future.successful(StepResult.Empty)

  def checkEdges(edges: Seq[S2Edge]): Future[StepResult] = {
    val futures = for {
      edge <- edges
    } yield {
      fetchSnapshotEdge(edge).map { case (queryParam, edgeOpt, kvOpt) =>
        edgeOpt.toSeq.map(e => EdgeWithScore(e, 1.0, queryParam.label))
      }
    }

    Future.sequence(futures).map { edgeWithScoreLs =>
      val edgeWithScores = edgeWithScoreLs.flatten
      StepResult(edgeWithScores = edgeWithScores, grouped = Nil, degreeEdges = Nil)
    }
  }

  //  def checkEdges(edges: Seq[Edge]): Future[StepResult] = storage.checkEdges(edges)

  def getEdges(q: Query): Future[StepResult] = {
    Try {
      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        fallback
      } else {
        val filterOutFuture = q.queryOption.filterOutQuery match {
          case None => fallback
          case Some(filterOutQuery) => getEdgesStepInner(filterOutQuery)
        }
        for {
          stepResult <- getEdgesStepInner(q)
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

  def getEdgesStepInner(q: Query, buildLastStepInnerResult: Boolean = false): Future[StepResult] = {
    Try {
      if (q.steps.isEmpty) fallback
      else {

        val queryOption = q.queryOption
        def fetch: Future[StepResult] = {
          val startStepInnerResult = QueryResult.fromVertices(this, q)
          q.steps.zipWithIndex.foldLeft(Future.successful(startStepInnerResult)) { case (prevStepInnerResultFuture, (step, stepIdx)) =>
            for {
              prevStepInnerResult <- prevStepInnerResultFuture
              currentStepInnerResult <- fetchStep(q, stepIdx, prevStepInnerResult, buildLastStepInnerResult)
            } yield {
              currentStepInnerResult.copy(
                accumulatedCursors = prevStepInnerResult.accumulatedCursors :+ currentStepInnerResult.cursors,
                failCount = currentStepInnerResult.failCount + prevStepInnerResult.failCount
              )
            }
          }
        }

        fetch
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        fallback
    } get
  }

  def fetchStep(orgQuery: Query,
                stepIdx: Int,
                stepInnerResult: StepResult,
                buildLastStepInnerResult: Boolean = false): Future[StepResult] = {
    if (stepInnerResult.isEmpty) Future.successful(StepResult.Empty)
    else {
      val edgeWithScoreLs = stepInnerResult.edgeWithScores

      val q = orgQuery
      val queryOption = orgQuery.queryOption
      val prevStepOpt = if (stepIdx > 0) Option(q.steps(stepIdx - 1)) else None
      val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.DefaultThreshold)
      val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
      val step = q.steps(stepIdx)

     val alreadyVisited =
        if (stepIdx == 0) Map.empty[(LabelWithDirection, S2Vertex), Boolean]
        else alreadyVisitedVertices(stepInnerResult.edgeWithScores)

      val initial = (Map.empty[S2Vertex, Double], Map.empty[S2Vertex, ArrayBuffer[EdgeWithScore]])
      val (sums, grouped) = edgeWithScoreLs.foldLeft(initial) { case ((sum, group), edgeWithScore) =>
        val key = edgeWithScore.edge.tgtVertex
        val newScore = sum.getOrElse(key, 0.0) + edgeWithScore.score
        val buffer = group.getOrElse(key, ArrayBuffer.empty[EdgeWithScore])
        buffer += edgeWithScore
        (sum + (key -> newScore), group + (key -> buffer))
      }
      val groupedByFiltered = sums.filter(t => t._2 >= prevStepThreshold)
      val prevStepTgtVertexIdEdges = grouped.map(t => t._1.id -> t._2)

      val nextStepSrcVertices = if (prevStepLimit >= 0) {
        groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
      } else {
        groupedByFiltered.toSeq
      }

      val queryRequests = for {
        (vertex, prevStepScore) <- nextStepSrcVertices
        queryParam <- step.queryParams
      } yield {
        val labelWeight = step.labelWeights.getOrElse(queryParam.labelWithDir.labelId, 1.0)
        val newPrevStepScore = if (queryOption.shouldPropagateScore) prevStepScore else 1.0
        QueryRequest(q, stepIdx, vertex, queryParam, newPrevStepScore, labelWeight)
      }

      val fetchedLs = fetches(queryRequests, prevStepTgtVertexIdEdges)

      filterEdges(orgQuery, stepIdx, queryRequests,
        fetchedLs, orgQuery.steps(stepIdx).queryParams, alreadyVisited, buildLastStepInnerResult, prevStepTgtVertexIdEdges)(ec)
    }
  }


  /**
    * responsible to fire parallel fetch call into storage and create future that will return merged result.
    *
    * @param queryRequests
    * @param prevStepEdges
    * @return
    */
  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[StepResult]] = {

    val reqWithIdxs = queryRequests.zipWithIndex
    val requestsPerLabel = reqWithIdxs.groupBy(t => t._1.queryParam.label)
    val aggFuture = requestsPerLabel.foldLeft(Future.successful(Map.empty[Int, StepResult])) { case (prevFuture, (label, reqWithIdxs)) =>
      for {
        prev <- prevFuture
        cur <- getStorage(label).fetches(reqWithIdxs.map(_._1), prevStepEdges)
      } yield {
        prev ++ reqWithIdxs.map(_._2).zip(cur).toMap
      }
    }
    aggFuture.map { agg => agg.toSeq.sortBy(_._1).map(_._2) }
  }


  def getEdgesMultiQuery(mq: MultiQuery): Future[StepResult] = {
    Try {
      if (mq.queries.isEmpty) fallback
      else {
        val filterOutFuture = mq.queryOption.filterOutQuery match {
          case None => fallback
          case Some(filterOutQuery) => getEdgesStepInner(filterOutQuery)
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


  def fetchSnapshotEdge(edge: S2Edge): Future[(QueryParam, Option[S2Edge], Option[SKeyValue])] = {
    /** TODO: Fix this. currently fetchSnapshotEdge should not use future cache
      * so use empty cacheKey.
      * */
    val queryParam = QueryParam(labelName = edge.innerLabel.label,
      direction = GraphUtil.fromDirection(edge.labelWithDir.dir),
      tgtVertexIdOpt = Option(edge.tgtVertex.innerIdVal),
      cacheTTLInMillis = -1)
    val q = Query.toQuery(Seq(edge.srcVertex), Seq(queryParam))
    val queryRequest = QueryRequest(q, 0, edge.srcVertex, queryParam)

    val storage = getStorage(edge.innerLabel)
    storage.fetchSnapshotEdgeKeyValues(queryRequest).map { kvs =>
      val (edgeOpt, kvOpt) =
        if (kvs.isEmpty) (None, None)
        else {
          val snapshotEdgeOpt = storage.toSnapshotEdge(kvs.head, queryRequest, isInnerCall = true, parentEdges = Nil)
          val _kvOpt = kvs.headOption
          (snapshotEdgeOpt, _kvOpt)
        }
      (queryParam, edgeOpt, kvOpt)
    } recoverWith { case ex: Throwable =>
      logger.error(s"fetchQueryParam failed. fallback return.", ex)
      throw FetchTimeoutException(s"${edge.toLogString}")
    }
  }

  def getVertices(vertices: Seq[S2Vertex]): Future[Seq[S2Vertex]] = {
    val verticesWithIdx = vertices.zipWithIndex
    val futures = verticesWithIdx.groupBy { case (v, idx) => v.service }.map { case (service, vertexGroup) =>
      getStorage(service).getVertices(vertexGroup.map(_._1)).map(_.zip(vertexGroup.map(_._2)))
    }

    Future.sequence(futures).map { ls =>
      ls.flatten.toSeq.sortBy(_._2).map(_._1)
    }
  }

  /** mutate */
  def deleteAllAdjacentEdges(srcVertices: Seq[S2Vertex],
                             labels: Seq[Label],
                             dir: Int,
                             ts: Long): Future[Boolean] = {

    val requestTs = ts
    val vertices = srcVertices
    /** create query per label */
    val queries = for {
      label <- labels
    } yield {
      val queryParam = QueryParam(labelName = label.label, direction = GraphUtil.fromDirection(dir),
        offset = 0, limit = DeleteAllFetchSize, duplicatePolicy = DuplicatePolicy.Raw)
      val step = Step(List(queryParam))
      Query(vertices, Vector(step))
    }

    //    Extensions.retryOnSuccessWithBackoff(MaxRetryNum, Random.nextInt(MaxBackOff) + 1) {
    val retryFuture = Extensions.retryOnSuccess(DeleteAllFetchCount) {
      fetchAndDeleteAll(queries, requestTs)
    } { case (allDeleted, deleteSuccess) =>
      allDeleted && deleteSuccess
    }.map { case (allDeleted, deleteSuccess) => allDeleted && deleteSuccess }

    retryFuture onFailure {
      case ex =>
        logger.error(s"[Error]: deleteAllAdjacentEdges failed.")
    }

    retryFuture
  }

  def fetchAndDeleteAll(queries: Seq[Query], requestTs: Long): Future[(Boolean, Boolean)] = {
    val future = for {
      stepInnerResultLs <- Future.sequence(queries.map(getEdgesStepInner(_, true)))
      (allDeleted, ret) <- deleteAllFetchedEdgesLs(stepInnerResultLs, requestTs)
    } yield {
      //        logger.debug(s"fetchAndDeleteAll: ${allDeleted}, ${ret}")
      (allDeleted, ret)
    }

    Extensions.retryOnFailure(MaxRetryNum) {
      future
    } {
      logger.error(s"fetch and deleteAll failed.")
      (true, false)
    }

  }

  def deleteAllFetchedEdgesLs(stepInnerResultLs: Seq[StepResult],
                              requestTs: Long): Future[(Boolean, Boolean)] = {
    stepInnerResultLs.foreach { stepInnerResult =>
      if (stepInnerResult.isFailure) throw new RuntimeException("fetched result is fallback.")
    }
    val futures = for {
      stepInnerResult <- stepInnerResultLs
      deleteStepInnerResult = buildEdgesToDelete(stepInnerResult, requestTs)
      if deleteStepInnerResult.edgeWithScores.nonEmpty
    } yield {
      val head = deleteStepInnerResult.edgeWithScores.head
      val label = head.edge.innerLabel
      val ret = label.schemaVersion match {
        case HBaseType.VERSION3 | HBaseType.VERSION4 =>
          if (label.consistencyLevel == "strong") {
            /**
              * read: snapshotEdge on queryResult = O(N)
              * write: N x (relatedEdges x indices(indexedEdge) + 1(snapshotEdge))
              */
            mutateEdges(deleteStepInnerResult.edgeWithScores.map(_.edge), withWait = true).map(_.forall(identity))
          } else {
            getStorage(label).deleteAllFetchedEdgesAsyncOld(deleteStepInnerResult, requestTs, MaxRetryNum)
          }
        case _ =>

          /**
            * read: x
            * write: N x ((1(snapshotEdge) + 2(1 for incr, 1 for delete) x indices)
            */
          getStorage(label).deleteAllFetchedEdgesAsyncOld(deleteStepInnerResult, requestTs, MaxRetryNum)
      }
      ret
    }

    if (futures.isEmpty) {
      // all deleted.
      Future.successful(true -> true)
    } else {
      Future.sequence(futures).map { rets => false -> rets.forall(identity) }
    }
  }

  def buildEdgesToDelete(stepInnerResult: StepResult, requestTs: Long): StepResult = {
    val filtered = stepInnerResult.edgeWithScores.filter { edgeWithScore =>
      (edgeWithScore.edge.ts < requestTs) && !edgeWithScore.edge.isDegree
    }
    if (filtered.isEmpty) StepResult.Empty
    else {
      val head = filtered.head
      val label = head.edge.innerLabel
      val edgeWithScoreLs = filtered.map { edgeWithScore =>
          val edge = edgeWithScore.edge
          val copiedEdge = label.consistencyLevel match {
            case "strong" =>
              edge.copyEdge(op = GraphUtil.operations("delete"),
                version = requestTs, propsWithTs = S2Edge.propsToState(edge.updatePropsWithTs()), ts = requestTs)
            case _ =>
              edge.copyEdge(propsWithTs = S2Edge.propsToState(edge.updatePropsWithTs()), ts = requestTs)
          }
//        val (newOp, newVersion, newPropsWithTs) = label.consistencyLevel match {
//          case "strong" =>
//            val edge = edgeWithScore.edge
//            edge.property(LabelMeta.timestamp.name, requestTs)
//            val _newPropsWithTs = edge.updatePropsWithTs()
//
//            (GraphUtil.operations("delete"), requestTs, _newPropsWithTs)
//          case _ =>
//            val oldEdge = edgeWithScore.edge
//            (oldEdge.op, oldEdge.version, oldEdge.updatePropsWithTs())
//        }
//
//        val copiedEdge =
//          edgeWithScore.edge.copy(op = newOp, version = newVersion, propsWithTs = newPropsWithTs)

        val edgeToDelete = edgeWithScore.copy(edge = copiedEdge)
        //      logger.debug(s"delete edge from deleteAll: ${edgeToDelete.edge.toLogString}")
        edgeToDelete
      }
      //Degree edge?
      StepResult(edgeWithScores = edgeWithScoreLs, grouped = Nil, degreeEdges = Nil, false)
    }
  }

  //  def deleteAllAdjacentEdges(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] =
  //    storage.deleteAllAdjacentEdges(srcVertices, labels, dir, ts)

  def mutateElements(elements: Seq[GraphElement],
                     withWait: Boolean = false): Future[Seq[Boolean]] = {

    val edgeBuffer = ArrayBuffer[(S2Edge, Int)]()
    val vertexBuffer = ArrayBuffer[(S2Vertex, Int)]()

    elements.zipWithIndex.foreach {
      case (e: S2Edge, idx: Int) => edgeBuffer.append((e, idx))
      case (v: S2Vertex, idx: Int) => vertexBuffer.append((v, idx))
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

  //  def mutateEdges(edges: Seq[Edge], withWait: Boolean = false): Future[Seq[Boolean]] = storage.mutateEdges(edges, withWait)

  def mutateEdges(edges: Seq[S2Edge], withWait: Boolean = false): Future[Seq[Boolean]] = {
    val edgeWithIdxs = edges.zipWithIndex

    val (strongEdges, weakEdges) =
      edgeWithIdxs.partition { case (edge, idx) =>
        val e = edge
        e.innerLabel.consistencyLevel == "strong" && e.op != GraphUtil.operations("insertBulk")
      }

    val weakEdgesFutures = weakEdges.groupBy { case (edge, idx) => edge.innerLabel.hbaseZkAddr }.map { case (zkQuorum, edgeWithIdxs) =>
      val futures = edgeWithIdxs.groupBy(_._1.innerLabel).map { case (label, edgeGroup) =>
        val storage = getStorage(label)
        val edges = edgeGroup.map(_._1)
        val idxs = edgeGroup.map(_._2)

        /** multiple edges with weak consistency level will be processed as batch */
        val mutations = edges.flatMap { edge =>
          val (_, edgeUpdate) =
            if (edge.op == GraphUtil.operations("delete")) S2Edge.buildDeleteBulk(None, edge)
            else S2Edge.buildOperation(None, Seq(edge))

          storage.buildVertexPutsAsync(edge) ++ storage.indexedEdgeMutations(edgeUpdate) ++ storage.snapshotEdgeMutations(edgeUpdate) ++ storage.increments(edgeUpdate)
        }

        storage.writeToStorage(zkQuorum, mutations, withWait).map { ret =>
          idxs.map(idx => idx -> ret)
        }
      }
      Future.sequence(futures)
    }
    val (strongDeleteAll, strongEdgesAll) = strongEdges.partition { case (edge, idx) => edge.op == GraphUtil.operations("deleteAll") }

    val deleteAllFutures = strongDeleteAll.map { case (edge, idx) =>
      deleteAllAdjacentEdges(Seq(edge.srcVertex), Seq(edge.innerLabel), edge.labelWithDir.dir, edge.ts).map(idx -> _)
    }

    val strongEdgesFutures = strongEdgesAll.groupBy { case (edge, idx) => edge.innerLabel }.map { case (label, edgeGroup) =>
      val edges = edgeGroup.map(_._1)
      val idxs = edgeGroup.map(_._2)
      val storage = getStorage(label)
      storage.mutateStrongEdges(edges, withWait = true).map { rets =>
        idxs.zip(rets)
      }
    }

    for {
      weak <- Future.sequence(weakEdgesFutures)
      deleteAll <- Future.sequence(deleteAllFutures)
      strong <- Future.sequence(strongEdgesFutures)
    } yield {
      (deleteAll ++ weak.flatten.flatten ++ strong.flatten).sortBy(_._1).map(_._2)
    }
  }

  def mutateVertices(vertices: Seq[S2Vertex], withWait: Boolean = false): Future[Seq[Boolean]] = {
    val verticesWithIdx = vertices.zipWithIndex
    val futures = verticesWithIdx.groupBy { case (v, idx) => v.service }.map { case (service, vertexGroup) =>
      getStorage(service).mutateVertices(vertexGroup.map(_._1), withWait).map(_.zip(vertexGroup.map(_._2)))
    }
    Future.sequence(futures).map { ls => ls.flatten.toSeq.sortBy(_._2).map(_._1) }
  }

  def incrementCounts(edges: Seq[S2Edge], withWait: Boolean): Future[Seq[(Boolean, Long, Long)]] = {
    val edgesWithIdx = edges.zipWithIndex
    val futures = edgesWithIdx.groupBy { case (e, idx) => e.innerLabel }.map { case (label, edgeGroup) =>
      getStorage(label).incrementCounts(edgeGroup.map(_._1), withWait).map(_.zip(edgeGroup.map(_._2)))
    }
    Future.sequence(futures).map { ls => ls.flatten.toSeq.sortBy(_._2).map(_._1) }
  }

  def updateDegree(edge: S2Edge, degreeVal: Long = 0): Future[Boolean] = {
    val label = edge.innerLabel

    val storage = getStorage(label)
    val kvs = storage.buildDegreePuts(edge, degreeVal)

    storage.writeToStorage(edge.innerLabel.service.cluster, kvs, withWait = true)
  }

  def shutdown(): Unit = {
    flushStorage()
    Model.shutdown()
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
      logger.error(s"[toElement]: $e", e)
      None
  } get


  def toVertex(s: String): Option[S2Vertex] = {
    toVertex(GraphUtil.split(s))
  }

  def toEdge(s: String): Option[S2Edge] = {
    toEdge(GraphUtil.split(s))
  }

  def toEdge(parts: Array[String]): Option[S2Edge] = Try {
    val (ts, operation, logType, srcId, tgtId, label) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) fromJsonToProperties(Json.parse(parts(6)).asOpt[JsObject].getOrElse(Json.obj())) else Map.empty[String, Any]
    val tempDirection = if (parts.length >= 8) parts(7) else "out"
    val direction = if (tempDirection != "out" && tempDirection != "in") "out" else tempDirection
    val edge = toEdge(srcId, tgtId, label, direction, props, ts.toLong, operation)
    Option(edge)
  } recover {
    case e: Exception =>
      logger.error(s"[toEdge]: $e", e)
      throw e
  } get

  def toVertex(parts: Array[String]): Option[S2Vertex] = Try {
    val (ts, operation, logType, srcId, serviceName, colName) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) fromJsonToProperties(Json.parse(parts(6)).asOpt[JsObject].getOrElse(Json.obj())) else Map.empty[String, Any]
    val vertex = toVertex(serviceName, colName, srcId, props, ts.toLong, operation)
    Option(vertex)
  } recover {
    case e: Throwable =>
      logger.error(s"[toVertex]: $e", e)
      throw e
  } get

  def toEdge(srcId: Any,
             tgtId: Any,
             labelName: String,
             direction: String,
             props: Map[String, Any] = Map.empty,
             ts: Long = System.currentTimeMillis(),
             operation: String = "insert"): S2Edge = {
    val label = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(labelName))

    val srcVertexIdInnerVal = toInnerVal(srcId, label.srcColumn.columnType, label.schemaVersion)
    val tgtVertexIdInnerVal = toInnerVal(tgtId, label.tgtColumn.columnType, label.schemaVersion)

    val srcVertex = newVertex(SourceVertexId(label.srcColumn, srcVertexIdInnerVal), System.currentTimeMillis())
    val tgtVertex = newVertex(TargetVertexId(label.tgtColumn, tgtVertexIdInnerVal), System.currentTimeMillis())
    val dir = GraphUtil.toDir(direction).getOrElse(throw new RuntimeException(s"$direction is not supported."))

    val propsPlusTs = props ++ Map(LabelMeta.timestamp.name -> ts)
    val propsWithTs = label.propsToInnerValsWithTs(propsPlusTs, ts)
    val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

    new S2Edge(this, srcVertex, tgtVertex, label, dir, op = op, version = ts).copyEdgeWithState(propsWithTs)
  }

  def toVertex(serviceName: String,
               columnName: String,
               id: Any,
               props: Map[String, Any] = Map.empty,
               ts: Long = System.currentTimeMillis(),
               operation: String = "insert"): S2Vertex = {

    val service = Service.findByName(serviceName).getOrElse(throw new RuntimeException(s"$serviceName is not found."))
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse(throw new RuntimeException(s"$columnName is not found."))
    val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

    val srcVertexId = VertexId(column, toInnerVal(id, column.columnType, column.schemaVersion))
    val propsInner = column.propsToInnerVals(props) ++
      Map(ColumnMeta.timestamp -> InnerVal.withLong(ts, column.schemaVersion))

    val vertex = new S2Vertex(this, srcVertexId, ts, S2Vertex.EmptyProps, op)
    S2Vertex.fillPropsWithTs(vertex, propsInner)
    vertex
  }

  /**
   * helper to create new Edge instance from given parameters on memory(not actually stored in storage).
   *
   * Since we are using mutable map for property value(propsWithTs),
   * we should make sure that reference for mutable map never be shared between multiple Edge instances.
   * To guarantee this, we never create Edge directly, but rather use this helper which is available on S2Graph.
   *
   * Note that we are using following convention
   * 1. `add*` for method that actually store instance into storage,
   * 2. `new*` for method that only create instance on memory, but not store it into storage.
   *
   * @param srcVertex
   * @param tgtVertex
   * @param innerLabel
   * @param dir
   * @param op
   * @param version
   * @param propsWithTs
   * @param parentEdges
   * @param originalEdgeOpt
   * @param pendingEdgeOpt
   * @param statusCode
   * @param lockTs
   * @param tsInnerValOpt
   * @return
   */
  def newEdge(srcVertex: S2Vertex,
              tgtVertex: S2Vertex,
              innerLabel: Label,
              dir: Int,
              op: Byte = GraphUtil.defaultOpByte,
              version: Long = System.currentTimeMillis(),
              propsWithTs: S2Edge.State,
              parentEdges: Seq[EdgeWithScore] = Nil,
              originalEdgeOpt: Option[S2Edge] = None,
              pendingEdgeOpt: Option[S2Edge] = None,
              statusCode: Byte = 0,
              lockTs: Option[Long] = None,
              tsInnerValOpt: Option[InnerValLike] = None): S2Edge = {
    val edge = new S2Edge(this, srcVertex, tgtVertex, innerLabel, dir, op, version, S2Edge.EmptyProps,
      parentEdges, originalEdgeOpt, pendingEdgeOpt, statusCode, lockTs, tsInnerValOpt)
    S2Edge.fillPropsWithTs(edge, propsWithTs)
    edge
  }

  /**
   * helper to create new SnapshotEdge instance from given parameters on memory(not actually stored in storage).
   *
   * Note that this is only available to S2Graph, not structure.Graph so only internal code should use this method.
   * @param srcVertex
   * @param tgtVertex
   * @param label
   * @param dir
   * @param op
   * @param version
   * @param propsWithTs
   * @param pendingEdgeOpt
   * @param statusCode
   * @param lockTs
   * @param tsInnerValOpt
   * @return
   */
  private[core] def newSnapshotEdge(srcVertex: S2Vertex,
                                    tgtVertex: S2Vertex,
                                    label: Label,
                                    dir: Int,
                                    op: Byte,
                                    version: Long,
                                    propsWithTs: S2Edge.State,
                                    pendingEdgeOpt: Option[S2Edge],
                                    statusCode: Byte = 0,
                                    lockTs: Option[Long],
                                    tsInnerValOpt: Option[InnerValLike] = None): SnapshotEdge = {
    val snapshotEdge = new SnapshotEdge(this, srcVertex, tgtVertex, label, dir, op, version, S2Edge.EmptyProps,
      pendingEdgeOpt, statusCode, lockTs, tsInnerValOpt)
    S2Edge.fillPropsWithTs(snapshotEdge, propsWithTs)
    snapshotEdge
  }

  /**
   * internal helper to actually store a single edge based on given peramters.
   *
   * Note that this is used from S2Vertex to implement blocking interface from Tp3.
   * Once tp3 provide AsyncStep, then this can be changed to return Java's CompletableFuture.
   *
   * @param srcVertex
   * @param tgtVertex
   * @param labelName
   * @param direction
   * @param props
   * @param ts
   * @param operation
   * @return
   */
  private[core] def addEdgeInner(srcVertex: S2Vertex,
                                 tgtVertex: S2Vertex,
                                 labelName: String,
                                 direction: String = "out",
                                 props: Map[String, AnyRef] = Map.empty,
                                 ts: Long = System.currentTimeMillis(),
                                 operation: String = "insert"): S2Edge = {
    Await.result(addEdgeInnerAsync(srcVertex, tgtVertex, labelName, direction, props, ts, operation), WaitTimeout)
  }

  private[core] def addEdgeInnerAsync(srcVertex: S2Vertex,
                                      tgtVertex: S2Vertex,
                                      labelName: String,
                                      direction: String = "out",
                                      props: Map[String, AnyRef] = Map.empty,
                                      ts: Long = System.currentTimeMillis(),
                                      operation: String = "insert"): Future[S2Edge] = {
    // Validations on input parameter
    val label = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(labelName))
    val dir = GraphUtil.toDir(direction).getOrElse(throw new RuntimeException(s"$direction is not supported."))
//    if (srcVertex.id.column != label.srcColumnWithDir(dir)) throw new RuntimeException(s"srcVertex's column[${srcVertex.id.column}] is not matched to label's srcColumn[${label.srcColumnWithDir(dir)}")
//    if (tgtVertex.id.column != label.tgtColumnWithDir(dir)) throw new RuntimeException(s"tgtVertex's column[${tgtVertex.id.column}] is not matched to label's tgtColumn[${label.tgtColumnWithDir(dir)}")

    // Convert given Map[String, AnyRef] property into internal class.
    val propsPlusTs = props ++ Map(LabelMeta.timestamp.name -> ts)
    val propsWithTs = label.propsToInnerValsWithTs(propsPlusTs, ts)
    val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

    val edge = newEdge(srcVertex, tgtVertex, label, dir, op = op, version = ts, propsWithTs = propsWithTs)
    // store edge into storage withWait option.
    mutateEdges(Seq(edge), withWait = true).map { rets =>
      if (!rets.headOption.getOrElse(false)) throw new RuntimeException("add edge failed.")
      else edge
    }
  }


  def newVertexId(serviceName: String)(columnName: String)(id: Any): VertexId = {
    val service = Service.findByName(serviceName).getOrElse(throw new RuntimeException(s"$serviceName is not found."))
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse(throw new RuntimeException(s"$columnName is not found."))
    newVertexId(service, column, id)
  }

  /**
   * helper to create S2Graph's internal VertexId instance with given parameters.
   * @param service
   * @param column
   * @param id
   * @return
   */
  def newVertexId(service: Service,
                  column: ServiceColumn,
                  id: Any): VertexId = {
    val innerVal = CanInnerValLike.anyToInnerValLike.toInnerVal(id)(column.schemaVersion)
    new VertexId(column, innerVal)
  }

  def newVertex(id: VertexId,
                ts: Long = System.currentTimeMillis(),
                props: S2Vertex.Props = S2Vertex.EmptyProps,
                op: Byte = 0,
                belongLabelIds: Seq[Int] = Seq.empty): S2Vertex = {
    val vertex = new S2Vertex(this, id, ts, S2Vertex.EmptyProps, op, belongLabelIds)
    S2Vertex.fillPropsWithTs(vertex, props)
    vertex
  }

  def getVertex(vertexId: VertexId): Option[S2Vertex] = {
    val v = newVertex(vertexId)
    Await.result(getVertices(Seq(v)).map { vertices => vertices.headOption }, WaitTimeout)
  }

  def fetchEdges(vertex: S2Vertex, labelNames: Seq[String], direction: String = "out"): util.Iterator[Edge] = {
    Await.result(fetchEdgesAsync(vertex, labelNames, direction), WaitTimeout)
  }

  def fetchEdgesAsync(vertex: S2Vertex, labelNames: Seq[String], direction: String = "out"): Future[util.Iterator[Edge]] = {
    val queryParams = labelNames.map { l =>
      QueryParam(labelName = l, direction = direction)
    }
    val query = Query.toQuery(Seq(vertex), queryParams)
    getEdges(query).map { stepResult =>
      val ls = new util.ArrayList[Edge]()
      stepResult.edgeWithScores.foreach(es => ls.add(es.edge))
      ls.iterator()
    }
  }

  /**
   * used by graph.traversal().V()
   * @param vertexIds: array of VertexId values. note that last parameter can be used to control if actually fetch vertices from storage or not.
   *                 since S2Graph use user-provided id as part of edge, it is possible to
   *                 fetch edge without fetch start vertex. default is false which means we are not fetching vertices from storage.
   * @return
   */
  override def vertices(vertexIds: AnyRef*): util.Iterator[structure.Vertex] = {
    val fetchVertices = vertexIds.lastOption.map { lastParam =>
      if (lastParam.isInstanceOf[Boolean]) lastParam.asInstanceOf[Boolean]
      else true
    }.getOrElse(true)

    if (vertexIds.isEmpty) {
      //TODO: default storage need to be fixed.
      Await.result(defaultStorage.fetchVerticesAll(), WaitTimeout).iterator
    } else {
      val vertices = for {
        vertexId <- vertexIds if vertexId.isInstanceOf[VertexId]
      } yield newVertex(vertexId.asInstanceOf[VertexId])

      if (fetchVertices) {
        val future = getVertices(vertices).map { vs =>
          val ls = new util.ArrayList[structure.Vertex]()
          ls.addAll(vs)
          ls.iterator()
        }
        Await.result(future, WaitTimeout)
      } else {
        vertices.iterator
      }
    }
  }

  override def edges(edgeIds: AnyRef*): util.Iterator[structure.Edge] = {
    if (edgeIds.isEmpty) {
      Await.result(defaultStorage.fetchEdgesAll(), WaitTimeout).iterator
    } else {
      Await.result(edgesAsync(edgeIds: _*), WaitTimeout)
    }
  }

  def edgesAsync(edgeIds: AnyRef*): Future[util.Iterator[structure.Edge]] = {
    val s2EdgeIds = edgeIds.filter(_.isInstanceOf[EdgeId]).map(_.asInstanceOf[EdgeId])
    val edgesToFetch = for {
      id <- s2EdgeIds
    } yield {
        toEdge(id.srcVertexId, id.tgtVertexId, id.labelName, id.direction)
      }

    checkEdges(edgesToFetch).map { stepResult =>
      val ls = new util.ArrayList[structure.Edge]
      stepResult.edgeWithScores.foreach { es => ls.add(es.edge) }
      ls.iterator()
    }
  }
  override def tx(): Transaction = ???



  override def variables(): Variables = ???

  override def configuration(): Configuration = ???

  override def addVertex(kvs: AnyRef*): structure.Vertex = {
    val kvsMap = ElementHelper.asMap(kvs: _*).asScala.toMap
    val id = kvsMap.getOrElse(T.id.toString, throw new RuntimeException("T.id is required."))
    val serviceColumnNames = kvsMap.getOrElse(T.label.toString, throw new RuntimeException("ServiceName::ColumnName is required.")).toString
    val names = serviceColumnNames.split(S2Vertex.VertexLabelDelimiter)
    if (names.length != 2) throw new RuntimeException("malformed data on vertex label.")
    val serviceName = names(0)
    val columnName = names(1)

    val vertex = toVertex(serviceName, columnName, id, kvsMap)
    val future = mutateVertices(Seq(vertex), withWait = true).map { vs =>
      if (vs.forall(identity)) vertex
      else throw new RuntimeException("addVertex failed.")
    }
    Await.result(future, WaitTimeout)
  }

  def addVertex(id: VertexId,
                ts: Long = System.currentTimeMillis(),
                props: S2Vertex.Props = S2Vertex.EmptyProps,
                op: Byte = 0,
                belongLabelIds: Seq[Int] = Seq.empty): S2Vertex = {
    val vertex = newVertex(id, ts, props, op, belongLabelIds)
    val future = mutateVertices(Seq(vertex), withWait = true).map { rets =>
      if (rets.forall(identity)) vertex
      else throw new RuntimeException("addVertex failed.")
    }
    Await.result(future, WaitTimeout)
  }

  override def close(): Unit = {
    shutdown()
  }

  override def compute[C <: GraphComputer](aClass: Class[C]): C = ???

  override def compute(): GraphComputer = ???
}
