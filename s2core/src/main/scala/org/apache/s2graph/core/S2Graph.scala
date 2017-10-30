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
import java.util.concurrent.{Executors, TimeUnit}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.features.S2GraphVariables
import org.apache.s2graph.core.index.IndexProvider
import org.apache.s2graph.core.io.tinkerpop.optimize.S2GraphStepStrategy
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorage
import org.apache.s2graph.core.storage.{ MutateResponse, SKeyValue, Storage}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.{DeferCache, Extensions, logger}
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies
import org.apache.tinkerpop.gremlin.structure
import org.apache.tinkerpop.gremlin.structure.Edge.Exceptions
import org.apache.tinkerpop.gremlin.structure.Graph.{Features, Variables}
import org.apache.tinkerpop.gremlin.structure.io.{GraphReader, GraphWriter, Io, Mapper}
import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Graph, T, Transaction, Vertex}
import play.api.libs.json.{JsObject, Json}
import scala.annotation.tailrec
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
    "db.default.driver" ->  "org.h2.Driver",
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
    val config  = ConfigFactory.parseMap(m).withFallback(DefaultConfig)
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

  def open(configuration: Configuration): S2Graph = {
    new S2Graph(configuration)(ec)
  }

  def initStorage(graph: S2Graph, config: Config)(ec: ExecutionContext): Storage = {
    val storageBackend = config.getString("s2graph.storage.backend")
    logger.info(s"[InitStorage]: $storageBackend")

    storageBackend match {
      case "hbase" => new AsynchbaseStorage(graph, config)
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

  def processDuplicates[R](queryParam: QueryParam,
                           duplicates: Seq[(FilterHashKey, R)])(implicit ev: WithScore[R]): Seq[(FilterHashKey, R)] = {

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

          /* process step group by */
          val results = StepResult.filterOutStepGroupBy(_results, step.groupBy)
          StepResult(edgeWithScores = results, grouped = Nil, degreeEdges = degrees, cursors = cursors, failCount = failCount)

        } else {
          val _results = buildResult(q, stepIdx, currentStepResults, parentEdges) { (edgeWithScore, propsSelectColumns) =>
            val edge = edgeWithScore.edge
            val score = edgeWithScore.score
            val label = edgeWithScore.label

            /* Select */
            val mergedPropsWithTs = edge.propertyValuesInner(propsSelectColumns)

//            val newEdge = edge.copy(propsWithTs = mergedPropsWithTs)
            val newEdge = edge.copyEdgeWithState(mergedPropsWithTs)

            val newEdgeWithScore = edgeWithScore.copy(edge = newEdge)
            /* OrderBy */
            val orderByValues =
             if (queryOption.orderByKeys.isEmpty) (score, edge.tsInnerVal, None, None)
              else StepResult.toTuple4(newEdgeWithScore.toValues(queryOption.orderByKeys))

            /* StepGroupBy */
            val stepGroupByValues = newEdgeWithScore.toValues(step.groupBy.keys)

            /* GroupBy */
            val groupByValues = newEdgeWithScore.toValues(queryOption.groupBy.keys)

            /* FilterOut */
            val filterOutValues = newEdgeWithScore.toValues(queryOption.filterOutFields)

            newEdgeWithScore.copy(orderByValues = orderByValues,
              stepGroupByValues = stepGroupByValues,
              groupByValues = groupByValues,
              filterOutValues = filterOutValues)
          }

          /* process step group by */
          val results = StepResult.filterOutStepGroupBy(_results, step.groupBy)

          /* process ordered list */
          val ordered = if (queryOption.groupBy.keys.isEmpty) StepResult.orderBy(queryOption, results) else Nil

          /* process grouped list */
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

              /*
                * watch out here. by calling toString on Any, we lose type information which will be used
                * later for toJson.
                */
              if (merged.nonEmpty) {
                val newKey = merged.head.groupByValues
                agg += ((newKey, (newScoreSum, merged)))
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

        /* Select */
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

  private def buildResult[R](query: Query,
                             stepIdx: Int,
                             stepResultLs: Seq[(QueryRequest, StepResult)],
                             parentEdges: Map[VertexId, Seq[EdgeWithScore]])
                            (createFunc: (EdgeWithScore, Seq[LabelMeta]) => R)
                            (implicit ev: WithScore[R]): ListBuffer[R] = {
    import scala.collection._

    val results = ListBuffer.empty[R]
    val sequentialLs: ListBuffer[(HashKey, FilterHashKey, R, QueryParam)] = ListBuffer.empty
    val duplicates: mutable.HashMap[HashKey, ListBuffer[(FilterHashKey, R)]] = mutable.HashMap.empty
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

        /* check if this edge should be exlcuded. */
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
              val newLs = ListBuffer.empty[(FilterHashKey, R)]
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


@Graph.OptIns(value = Array(
  new Graph.OptIn(value = Graph.OptIn.SUITE_PROCESS_STANDARD),
  new Graph.OptIn(value = Graph.OptIn.SUITE_STRUCTURE_STANDARD)
))
@Graph.OptOuts(value = Array(
  /* Process */
  /* branch: passed all. */
//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchTest$Traversals", method = "*", reason = "no"),
// passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest$Traversals", method = "*", reason = "no"),
// passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest$Traversals", method = "*", reason = "no"),
//  passed: all


  /* filter */
//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest$Traversals", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropTest$Traversals", method = "g_V_properties_drop", reason = "please find bug on this case."),
//  passed: all, failed: g_V_properties_drop

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest$Traversals", method = "*", reason = "no"),
//  passed: all,

  /* map */
//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantTest$Traversals", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals", method = "g_V_both_both_count", reason = "count differ very little. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals", method = "g_V_repeatXoutX_timesX3X_count", reason = "count differ very little. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals", method = "g_V_repeatXoutX_timesX8X_count", reason = "count differ very litter. fix this."),
//  passed: all, failed: g_V_both_both_count, g_V_repeatXoutX_timesX3X_count, g_V_repeatXoutX_timesX8X_count

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapKeysTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapValuesTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$CountMatchTraversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$GreedyMatchTraversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanTest$Traversals", method = "*", reason = "no"),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest$Traversals", method = "*", reason = "no"),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest$Traversals", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "testProfileStrategyCallback", reason = "NullPointerException. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "g_V_whereXinXcreatedX_count_isX1XX_name_profile", reason = "java.lang.AssertionError: There should be 3 top-level metrics. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX", reason = "expected 2, actual 6. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "grateful_V_out_out_profileXmetricsX", reason = "expected 8049, actual 8046. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "grateful_V_out_out_profile", reason = "expected 8049, actual 8046. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "modern_V_out_out_profileXmetricsX", reason = "expected 2, actual 6. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "modern_V_out_out_profile", reason = "expected 2, actual 6. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "testProfileStrategyCallbackSideEffect", reason = "NullPointerException. fix this."),
//  failed: grateful_V_out_out_profileXmetricsX, g_V_repeat_both_profileXmetricsX, grateful_V_out_out_profile, g_V_repeat_both_profile

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest$Traversals", method = "*", reason = "no"),
//  passed: all

  /* sideEffect */
//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTestV3d0$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest$Traversals", method = "*", reason = "no"),
//  passed: all

  //  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreTest$Traversals", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup", reason = "Expected 5, Actual 6."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX", reason = "Expected 3, Actual 6"),
//  passed: all, failed: g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup, g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest$Traversals", method = "*", reason = "no"),
//  passed: all


  /* compliance */
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest", method = "shouldThrowExceptionWhenIdsMixed", reason = "VertexId is not Element."),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest", method = "*", reason = "not supported yet."),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategyProcessTest", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithSpecifiedId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithGeneratedCustomId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithGeneratedDefaultId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithGeneratedDefaultId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithGeneratedCustomId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithSpecifiedId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
//  failed: shouldGenerateDefaultIdOnAddVWithSpecifiedId, shouldGenerateDefaultIdOnAddVWithGeneratedCustomId, shouldGenerateDefaultIdOnGraphAddVWithGeneratedDefaultId,
//  shouldGenerateDefaultIdOnAddVWithGeneratedDefaultId, shouldGenerateDefaultIdOnGraphAddVWithGeneratedCustomId, shouldGenerateDefaultIdOnGraphAddVWithSpecifiedId

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "*", reason = "not supported yet."),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest", method = "*", reason = "not supported yet."),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest", method = "*", reason = "no"),
//  passed: all

  /* Structure */
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.EdgeTest$BasicEdgeTest", method="shouldValidateIdEquality", reason="reference equals on EdgeId is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.EdgeTest$BasicEdgeTest", method="shouldValidateEquality", reason="reference equals on EdgeId is not supported."),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.GraphConstructionTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.PropertyTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.VertexPropertyTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.FeatureSupportTest", method="*", reason="no"),
  // passed: all, failed: none

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest", method="shouldHaveExceptionConsistencyWhenAssigningSameIdOnEdge", reason="S2Vertex.addEdge behave as upsert."),
  // passed: , failed: shouldHaveExceptionConsistencyWhenAssigningSameIdOnEdge

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdgeTest", method="shouldNotEvaluateToEqualDifferentId", reason="reference equals is not supported."),
  // passed: all, failed: shouldNotEvaluateToEqualDifferentId

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedGraphTest", method="*", reason="no"),
  // passed: all, failed: none,  all ignored

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPropertyTest", method="shouldNotBeEqualPropertiesAsThereIsDifferentKey", reason="reference equals is not supported."),
//  // passed: , failed: shouldNotBeEqualPropertiesAsThereIsDifferentKey

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexPropertyTest", method="*", reason="no"),
  // passed: all, failed: none

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.GraphTest", method="shouldRemoveVertices", reason="random label creation is not supported. all label need to be pre-configured."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.GraphTest", method="shouldHaveExceptionConsistencyWhenAssigningSameIdOnVertex", reason="Assigning the same ID to an Element update instead of throwing exception."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.GraphTest", method="shouldRemoveEdges", reason="random label creation is not supported. all label need to be pre-configured."),
  // passed: , failed:

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdgeTest", method="shouldNotEvaluateToEqualDifferentId", reason="Assigning the same ID to an Element update instead of throwing exception."),
  // passed: all, skip: shouldNotEvaluateToEqualDifferentId


//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexPropertyTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceGraphTest", method="*", reason="no"),
  // passed: all, failed: none, all ignored
//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexTest", method="*", reason="no"),
  // passed: all, failed: none, all ignored

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest", method="*", reason="no"),
  // passed: all,

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.1)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.5)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.5)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.1)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(PowerLawDistribution{gamma=2.3, multiplier=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.25)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(PowerLawDistribution{gamma=2.3, multiplier=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.25)", reason="graphson-v2-embedded is not supported."),
  // passed: all, except shouldGenerateDifferentGraph method.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest", method="*", reason="non-deterministic test."),
  // all failed.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoTest", method="shouldSerializeTree", reason="order of children is reversed. not sure why."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONTest", method="shouldSerializeTraversalMetrics", reason="expected 2, actual 3."),
  // passed: all, failed: $GryoTest.shouldSerializeTree

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoCustomTest", method="*", reason="no"),
  // all ignored.

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoPropertyTest", method="*", reason="no"),
  // all passed.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteVertexWithBOTHEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteVertexWithINEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteDetachedVertexAsReferenceNoEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteVertexNoEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteVertexWithOUTEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteDetachedVertexNoEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  // passed: all, except graphson-v2-embedded.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest", method="shouldReadWriteDetachedEdgeAsReference", specific="graphson-v2-embedded", reason="no"),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest", method="shouldReadWriteEdge", specific="graphson-v2-embedded", reason="no"),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest", method="shouldReadWriteDetachedEdge", specific="graphson-v2-embedded", reason="no"),
  // passed: all, except graphson-v2-embedded.

  // TODO:
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method="*", reason="no"), // all failed.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoTest", method="*", reason="no")
  // all failed.
))
class S2Graph(_config: Config)(implicit val ec: ExecutionContext) extends Graph {

  import S2Graph._

  private var apacheConfiguration: Configuration = _

  def dbSession() = scalikejdbc.AutoSession

  def this(apacheConfiguration: Configuration)(ec: ExecutionContext) = {
    this(S2Graph.toTypeSafeConfig(apacheConfiguration))(ec)
    this.apacheConfiguration = apacheConfiguration
  }

  private val running = new AtomicBoolean(true)

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
  val WaitTimeout = Duration(600, TimeUnit.SECONDS)

  val management = new Management(this)

  def getManagement() = management

  private val localLongId = new AtomicLong()

  def nextLocalLongId = localLongId.getAndIncrement()

  private def confWithFallback(conf: Config): Config = {
    conf.withFallback(config)
  }

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

  val defaultStorage: Storage = S2Graph.initStorage(this, config)(ec)

  /** QueryLevel FutureCache */
  val queryFutureCache = new DeferCache[StepResult, Promise, Future](parseCacheConfig(config, "query."), empty = StepResult.Empty)

  for {
    entry <- config.entrySet() if S2Graph.DefaultConfigs.contains(entry.getKey)
    (k, v) = (entry.getKey, entry.getValue)
  } logger.info(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")

  val indexProvider = IndexProvider.apply(config)

  def getStorage(service: Service): Storage = {
    storagePool.getOrElse(s"service:${service.serviceName}", defaultStorage)
  }

  def getStorage(label: Label): Storage = {
    storagePool.getOrElse(s"label:${label.label}", defaultStorage)
  }

  def flushStorage(): Unit = {
    storagePool.foreach { case (_, storage) =>

      /* flush is blocking */
      storage.flush()
    }
  }

  def fallback = Future.successful(StepResult.Empty)

  def checkEdges(edges: Seq[S2Edge]): Future[StepResult] = {
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

  def getVertices(vertices: Seq[S2Vertex]): Future[Seq[S2Vertex]] = {
    val verticesWithIdx = vertices.zipWithIndex
    val futures = verticesWithIdx.groupBy { case (v, idx) => v.service }.map { case (service, vertexGroup) =>
      getStorage(service).fetchVertices(vertices).map(_.zip(vertexGroup.map(_._2)))
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
    /* create query per label */
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
    val futures = queries.map(getEdgesStepInner(_, true))
    val future = for {
      stepInnerResultLs <- Future.sequence(futures)
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
      logger.error(s"[!!!!!!]: ${stepInnerResult.edgeWithScores.size}")
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
            /*
              * read: snapshotEdge on queryResult = O(N)
              * write: N x (relatedEdges x indices(indexedEdge) + 1(snapshotEdge))
              */
            mutateEdges(deleteStepInnerResult.edgeWithScores.map(_.edge), withWait = true).map(_.forall(_.isSuccess))
          } else {
            deleteAllFetchedEdgesAsyncOld(getStorage(label))(deleteStepInnerResult, requestTs, MaxRetryNum)
          }
        case _ =>

          /*
            * read: x
            * write: N x ((1(snapshotEdge) + 2(1 for incr, 1 for delete) x indices)
            */
          deleteAllFetchedEdgesAsyncOld(getStorage(label))(deleteStepInnerResult, requestTs, MaxRetryNum)
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

  private def deleteAllFetchedEdgesAsyncOld(storage: Storage)(stepInnerResult: StepResult,
                                                         requestTs: Long,
                                                         retryNum: Int): Future[Boolean] = {
    if (stepInnerResult.isEmpty) Future.successful(true)
    else {
      val head = stepInnerResult.edgeWithScores.head
      val zkQuorum = head.edge.innerLabel.hbaseZkAddr
      val futures = for {
        edgeWithScore <- stepInnerResult.edgeWithScores
      } yield {
        val edge = edgeWithScore.edge
        val score = edgeWithScore.score

        val edgeSnapshot = edge.copyEdge(propsWithTs = S2Edge.propsToState(edge.updatePropsWithTs()))
        val reversedSnapshotEdgeMutations = storage.snapshotEdgeSerializer(edgeSnapshot.toSnapshotEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put))

        val edgeForward = edge.copyEdge(propsWithTs = S2Edge.propsToState(edge.updatePropsWithTs()))
        val forwardIndexedEdgeMutations = edgeForward.edgesWithIndex.flatMap { indexEdge =>
          storage.indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
            storage.buildIncrementsAsync(indexEdge, -1L)
        }

        /* reverted direction */
        val edgeRevert = edge.copyEdge(propsWithTs = S2Edge.propsToState(edge.updatePropsWithTs()))
        val reversedIndexedEdgesMutations = edgeRevert.duplicateEdge.edgesWithIndex.flatMap { indexEdge =>
          storage.indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
            storage.buildIncrementsAsync(indexEdge, -1L)
        }

        val mutations = reversedIndexedEdgesMutations ++ reversedSnapshotEdgeMutations ++ forwardIndexedEdgeMutations

        storage.writeToStorage(zkQuorum, mutations, withWait = true)
      }

      Future.sequence(futures).map { rets => rets.forall(_.isSuccess) }
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

        val edgeToDelete = edgeWithScore.copy(edge = copiedEdge)
        //      logger.debug(s"delete edge from deleteAll: ${edgeToDelete.edge.toLogString}")
        edgeToDelete
      }
      //Degree edge?
      StepResult(edgeWithScores = edgeWithScoreLs, grouped = Nil, degreeEdges = Nil, false)
    }
  }


  def mutateElements(elements: Seq[GraphElement],
                     withWait: Boolean = false): Future[Seq[MutateResponse]] = {

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

  def mutateEdges(edges: Seq[S2Edge], withWait: Boolean = false): Future[Seq[MutateResponse]] = {
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

        /* multiple edges with weak consistency level will be processed as batch */
        val mutations = edges.flatMap { edge =>
          val (_, edgeUpdate) =
            if (edge.op == GraphUtil.operations("delete")) S2Edge.buildDeleteBulk(None, edge)
            else S2Edge.buildOperation(None, Seq(edge))

          val (bufferIncr, nonBufferIncr) = storage.increments(edgeUpdate.deepCopy)

          if (bufferIncr.nonEmpty) storage.writeToStorage(zkQuorum, bufferIncr, withWait = false)
          storage.buildVertexPutsAsync(edge) ++ storage.indexedEdgeMutations(edgeUpdate.deepCopy) ++ storage.snapshotEdgeMutations(edgeUpdate.deepCopy) ++ nonBufferIncr
        }

        storage.writeToStorage(zkQuorum, mutations, withWait).map { ret =>
          idxs.map(idx => idx -> ret.isSuccess)
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
      mutateStrongEdges(storage)(edges, withWait = true).map { rets =>
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

  private def mutateStrongEdges(storage: Storage)(_edges: Seq[S2Edge], withWait: Boolean): Future[Seq[Boolean]] = {

    val edgeWithIdxs = _edges.zipWithIndex
    val grouped = edgeWithIdxs.groupBy { case (edge, idx) =>
      (edge.innerLabel, edge.srcVertex.innerId, edge.tgtVertex.innerId)
    } toSeq

    val mutateEdges = grouped.map { case ((_, _, _), edgeGroup) =>
      val edges = edgeGroup.map(_._1)
      val idxs = edgeGroup.map(_._2)
      // After deleteAll, process others
      val mutateEdgeFutures = edges.toList match {
        case head :: tail =>
          val edgeFuture = mutateEdgesInner(storage)(edges, checkConsistency = true , withWait)

          //TODO: decide what we will do on failure on vertex put
          val puts = storage.buildVertexPutsAsync(head)
          val vertexFuture = storage.writeToStorage(head.innerLabel.hbaseZkAddr, puts, withWait)
          Seq(edgeFuture, vertexFuture)
        case Nil => Nil
      }

      val composed = for {
      //        deleteRet <- Future.sequence(deleteAllFutures)
        mutateRet <- Future.sequence(mutateEdgeFutures)
      } yield mutateRet

      composed.map(_.forall(_.isSuccess)).map { ret => idxs.map( idx => idx -> ret) }
    }

    Future.sequence(mutateEdges).map { squashedRets =>
      squashedRets.flatten.sortBy { case (idx, ret) => idx }.map(_._2)
    }
  }


  private def mutateEdgesInner(storage: Storage)(edges: Seq[S2Edge],
                       checkConsistency: Boolean,
                       withWait: Boolean): Future[MutateResponse] = {
    assert(edges.nonEmpty)
    // TODO:: remove after code review: unreachable code
    if (!checkConsistency) {

      val zkQuorum = edges.head.innerLabel.hbaseZkAddr
      val futures = edges.map { edge =>
        val (_, edgeUpdate) = S2Edge.buildOperation(None, Seq(edge))

        val (bufferIncr, nonBufferIncr) = storage.increments(edgeUpdate.deepCopy)
        val mutations =
          storage.indexedEdgeMutations(edgeUpdate.deepCopy) ++ storage.snapshotEdgeMutations(edgeUpdate.deepCopy) ++ nonBufferIncr

        if (bufferIncr.nonEmpty) storage.writeToStorage(zkQuorum, bufferIncr, withWait = false)

        storage.writeToStorage(zkQuorum, mutations, withWait)
      }
      Future.sequence(futures).map { rets => new MutateResponse(rets.forall(_.isSuccess)) }
    } else {
      storage.fetchSnapshotEdgeInner(edges.head).flatMap { case (snapshotEdgeOpt, kvOpt) =>
        storage.retry(1)(edges, 0, snapshotEdgeOpt).map(new MutateResponse(_))
      }
    }
  }

  def mutateVertices(vertices: Seq[S2Vertex], withWait: Boolean = false): Future[Seq[MutateResponse]] = {
    def mutateVertex(storage: Storage)(vertex: S2Vertex, withWait: Boolean): Future[MutateResponse] = {
      if (vertex.op == GraphUtil.operations("delete")) {
        storage.writeToStorage(vertex.hbaseZkAddr,
          storage.vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Delete)), withWait)
      } else if (vertex.op == GraphUtil.operations("deleteAll")) {
        logger.info(s"deleteAll for vertex is truncated. $vertex")
        Future.successful(MutateResponse.Success) // Ignore withWait parameter, because deleteAll operation may takes long time
      } else {
        storage.writeToStorage(vertex.hbaseZkAddr, storage.buildPutsAll(vertex), withWait)
      }
    }

    def mutateVertices(storage: Storage)(vertices: Seq[S2Vertex],
                       withWait: Boolean = false): Future[Seq[MutateResponse]] = {
      val futures = vertices.map { vertex => mutateVertex(storage)(vertex, withWait) }
      Future.sequence(futures)
    }

    val verticesWithIdx = vertices.zipWithIndex
    val futures = verticesWithIdx.groupBy { case (v, idx) => v.service }.map { case (service, vertexGroup) =>
      mutateVertices(getStorage(service))(vertexGroup.map(_._1), withWait).map(_.zip(vertexGroup.map(_._2)))
    }
    Future.sequence(futures).map { ls => ls.flatten.toSeq.sortBy(_._2).map(_._1) }
  }



  def incrementCounts(edges: Seq[S2Edge], withWait: Boolean): Future[Seq[MutateResponse]] = {
    def incrementCounts(storage: Storage)(edges: Seq[S2Edge], withWait: Boolean): Future[Seq[MutateResponse]] = {
      val futures = for {
        edge <- edges
      } yield {
        val kvs = for {
          relEdge <- edge.relatedEdges
          edgeWithIndex <- EdgeMutate.filterIndexOption(relEdge.edgesWithIndexValid)
        } yield {
          val countWithTs = edge.propertyValueInner(LabelMeta.count)
          val countVal = countWithTs.innerVal.toString().toLong
          storage.buildIncrementsCountAsync(edgeWithIndex, countVal).head
        }
        storage.writeToStorage(edge.innerLabel.hbaseZkAddr, kvs, withWait = withWait)
      }

      Future.sequence(futures)
    }

    val edgesWithIdx = edges.zipWithIndex
    val futures = edgesWithIdx.groupBy { case (e, idx) => e.innerLabel }.map { case (label, edgeGroup) =>
      incrementCounts(getStorage(label))(edgeGroup.map(_._1), withWait).map(_.zip(edgeGroup.map(_._2)))
    }
    Future.sequence(futures).map { ls =>
      ls.flatten.toSeq.sortBy(_._2).map(_._1)
    }
  }

  def updateDegree(edge: S2Edge, degreeVal: Long = 0): Future[MutateResponse] = {
    val label = edge.innerLabel

    val storage = getStorage(label)
    val kvs = storage.buildDegreePuts(edge, degreeVal)

    storage.writeToStorage(edge.innerLabel.service.cluster, kvs, withWait = true)
  }

  def isRunning(): Boolean = running.get()

  def shutdown(modelDataDelete: Boolean = false): Unit =
    if (running.compareAndSet(true, false)) {
      flushStorage()
      Model.shutdown(modelDataDelete)
      defaultStorage.shutdown()
      localLongId.set(0l)
    }

  def toGraphElement(s: String, labelMapping: Map[String, String] = Map.empty): Option[GraphElement] = Try {
    val parts = GraphUtil.split(s)
    val logType = parts(2)
    val element = if (logType == "edge" | logType == "e") {
      /* current only edge is considered to be bulk loaded */
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

    val srcColumn = if (direction == "out") label.srcColumn else label.tgtColumn
    val tgtColumn = if (direction == "out") label.tgtColumn else label.srcColumn

    val srcVertexIdInnerVal = toInnerVal(srcId, srcColumn.columnType, label.schemaVersion)
    val tgtVertexIdInnerVal = toInnerVal(tgtId, tgtColumn.columnType, label.schemaVersion)

    val srcVertex = newVertex(SourceVertexId(srcColumn, srcVertexIdInnerVal), System.currentTimeMillis())
    val tgtVertex = newVertex(TargetVertexId(tgtColumn, tgtVertexIdInnerVal), System.currentTimeMillis())
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

    val service = Service.findByName(serviceName).getOrElse(throw new java.lang.IllegalArgumentException(s"$serviceName is not found."))
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse(throw new java.lang.IllegalArgumentException(s"$columnName is not found."))
    val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

    val srcVertexId = id match {
      case vid: VertexId => id.asInstanceOf[VertexId]
      case _ => VertexId(column, toInnerVal(id, column.columnType, column.schemaVersion))
    }

    val propsInner = column.propsToInnerVals(props) ++
      Map(ColumnMeta.timestamp -> InnerVal.withLong(ts, column.schemaVersion))

    val vertex = new S2Vertex(this, srcVertexId, ts, S2Vertex.EmptyProps, op)
    S2Vertex.fillPropsWithTs(vertex, propsInner)
    vertex
  }

  def toRequestEdge(queryRequest: QueryRequest, parentEdges: Seq[EdgeWithScore]): S2Edge = {
    val srcVertex = queryRequest.vertex
    val queryParam = queryRequest.queryParam
    val tgtVertexIdOpt = queryParam.tgtVertexInnerIdOpt
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val propsWithTs = label.EmptyPropsWithTs

    tgtVertexIdOpt match {
      case Some(tgtVertexId) => // _to is given.
        /* we use toSnapshotEdge so dont need to swap src, tgt */
        val src = srcVertex.innerId
        val tgt = tgtVertexId
        val (srcVId, tgtVId) = (SourceVertexId(srcColumn, src), TargetVertexId(tgtColumn, tgt))
        val (srcV, tgtV) = (newVertex(srcVId), newVertex(tgtVId))

        newEdge(srcV, tgtV, label, labelWithDir.dir, propsWithTs = propsWithTs)
      case None =>
        val src = srcVertex.innerId
        val srcVId = SourceVertexId(srcColumn, src)
        val srcV = newVertex(srcVId)

        newEdge(srcV, srcV, label, labelWithDir.dir, propsWithTs = propsWithTs, parentEdges = parentEdges)
    }
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
    val edge = S2Edge(
      this,
      srcVertex,
      tgtVertex,
      innerLabel,
      dir,
      op,
      version,
      S2Edge.EmptyProps,
      parentEdges,
      originalEdgeOpt,
      pendingEdgeOpt,
      statusCode,
      lockTs,
      tsInnerValOpt)
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

  def fetchEdges(vertex: S2Vertex, labelNameWithDirs: Seq[(String, String)]): util.Iterator[Edge] = {
    Await.result(fetchEdgesAsync(vertex, labelNameWithDirs), WaitTimeout)
  }

  def fetchEdgesAsync(vertex: S2Vertex, labelNameWithDirs: Seq[(String, String)]): Future[util.Iterator[Edge]] = {
    val queryParams = labelNameWithDirs.map { case (l, direction) =>
      QueryParam(labelName = l, direction = direction.toLowerCase)
    }

    val query = Query.toQuery(Seq(vertex), queryParams)
    val queryRequests = queryParams.map { param => QueryRequest(query, 0, vertex, param) }
    val ls = new util.ArrayList[Edge]()
    fetches(queryRequests, Map.empty).map { stepResultLs =>
      stepResultLs.foreach(_.edgeWithScores.foreach(es => ls.add(es.edge)))
      ls.iterator()
    }
//    getEdges(query).map { stepResult =>
//      val ls = new util.ArrayList[Edge]()
//      stepResult.edgeWithScores.foreach(es => ls.add(es.edge))
//      ls.iterator()
//    }
  }

  /**
   * used by graph.traversal().V()
   * @param ids: array of VertexId values. note that last parameter can be used to control if actually fetch vertices from storage or not.
   *                 since S2Graph use user-provided id as part of edge, it is possible to
   *                 fetch edge without fetch start vertex. default is false which means we are not fetching vertices from storage.
   * @return
   */
  override def vertices(ids: AnyRef*): util.Iterator[structure.Vertex] = {
    val fetchVertices = ids.lastOption.map { lastParam =>
      if (lastParam.isInstanceOf[Boolean]) lastParam.asInstanceOf[Boolean]
      else true
    }.getOrElse(true)

    if (ids.isEmpty) {
      //TODO: default storage need to be fixed.
      Await.result(defaultStorage.fetchVerticesAll(), WaitTimeout).iterator
    } else {
      val vertices = ids.collect {
        case s2Vertex: S2Vertex => s2Vertex
        case vId: VertexId => newVertex(vId)
        case vertex: Vertex => newVertex(vertex.id().asInstanceOf[VertexId])
        case other @ _ => newVertex(VertexId.fromString(other.toString))
      }

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
      // FIXME
      Await.result(defaultStorage.fetchEdgesAll(), WaitTimeout).iterator
    } else {
      Await.result(edgesAsync(edgeIds: _*), WaitTimeout)
    }
  }

  def edgesAsync(edgeIds: AnyRef*): Future[util.Iterator[structure.Edge]] = {
    val s2EdgeIds = edgeIds.collect {
      case s2Edge: S2Edge => s2Edge.id().asInstanceOf[EdgeId]
      case id: EdgeId => id
      case s: String => EdgeId.fromString(s)
    }
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
  override def tx(): Transaction = {
    if (!features.graph.supportsTransactions) throw Graph.Exceptions.transactionsNotSupported
    ???
  }

  override def variables(): Variables = new S2GraphVariables

  override def configuration(): Configuration = apacheConfiguration

  override def addVertex(label: String): Vertex = {
    if (label == null) throw Element.Exceptions.labelCanNotBeNull
    if (label.isEmpty) throw Element.Exceptions.labelCanNotBeEmpty

    addVertex(Seq(T.label, label): _*)
  }

  def makeVertex(idValue: AnyRef, kvsMap: Map[String, AnyRef]): S2Vertex = {
    idValue match {
      case vId: VertexId =>
        toVertex(vId.column.service.serviceName, vId.column.columnName, vId, kvsMap)
      case _ =>
        val serviceColumnNames = kvsMap.getOrElse(T.label.toString, DefaultColumnName).toString

        val names = serviceColumnNames.split(S2Vertex.VertexLabelDelimiter)
        val (serviceName, columnName) =
          if (names.length == 1) (DefaultServiceName, names(0))
          else throw new RuntimeException("malformed data on vertex label.")

        toVertex(serviceName, columnName, idValue, kvsMap)
    }
  }

  override def addVertex(kvs: AnyRef*): structure.Vertex = {
    if (!features().vertex().supportsUserSuppliedIds() && kvs.contains(T.id)) {
      throw Vertex.Exceptions.userSuppliedIdsNotSupported
    }

    val kvsMap = S2Property.kvsToProps(kvs)
    kvsMap.get(T.id.name()) match {
      case Some(idValue) if !S2Property.validType(idValue) =>
        throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported()
      case _ =>
    }

    kvsMap.foreach { case (k, v) => S2Property.assertValidProp(k, v) }

    if (kvsMap.contains(T.label.name()) && kvsMap(T.label.name).toString.isEmpty)
      throw Element.Exceptions.labelCanNotBeEmpty

    val vertex = kvsMap.get(T.id.name()) match {
      case None => // do nothing
        val id = nextLocalLongId
        makeVertex(Long.box(id), kvsMap)
      case Some(idValue) if S2Property.validType(idValue) =>
        makeVertex(idValue, kvsMap)
      case _ =>
        throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported
    }

    addVertexInner(vertex)

    vertex
  }

  def addVertex(id: VertexId,
                ts: Long = System.currentTimeMillis(),
                props: S2Vertex.Props = S2Vertex.EmptyProps,
                op: Byte = 0,
                belongLabelIds: Seq[Int] = Seq.empty): S2Vertex = {
    val vertex = newVertex(id, ts, props, op, belongLabelIds)

    val future = mutateVertices(Seq(vertex), withWait = true).map { rets =>
      if (rets.forall(_.isSuccess)) vertex
      else throw new RuntimeException("addVertex failed.")
    }
    Await.ready(future, WaitTimeout)

    vertex
  }

  def addVertexInner(vertex: S2Vertex): S2Vertex = {
    val future = mutateVertices(Seq(vertex), withWait = true).flatMap { rets =>
      if (rets.forall(_.isSuccess)) {
        indexProvider.mutateVerticesAsync(Seq(vertex))
      } else throw new RuntimeException("addVertex failed.")
    }
    Await.ready(future, WaitTimeout)

    vertex
  }

  /* tp3 only */
  def addEdge(srcVertex: S2Vertex, labelName: String, tgtVertex: Vertex, kvs: AnyRef*): Edge = {
    val containsId = kvs.contains(T.id)

    tgtVertex match {
      case otherV: S2Vertex =>
        if (!features().edge().supportsUserSuppliedIds() && containsId) {
          throw Exceptions.userSuppliedIdsNotSupported()
        }

        val props = S2Property.kvsToProps(kvs)

        props.foreach { case (k, v) => S2Property.assertValidProp(k, v) }

        //TODO: direction, operation, _timestamp need to be reserved property key.

        try {
          val direction = props.get("direction").getOrElse("out").toString
          val ts = props.get(LabelMeta.timestamp.name).map(_.toString.toLong).getOrElse(System.currentTimeMillis())
          val operation = props.get("operation").map(_.toString).getOrElse("insert")
          val label = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(labelName))
          val dir = GraphUtil.toDir(direction).getOrElse(throw new RuntimeException(s"$direction is not supported."))
          val propsPlusTs = props ++ Map(LabelMeta.timestamp.name -> ts)
          val propsWithTs = label.propsToInnerValsWithTs(propsPlusTs, ts)
          val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

          val edge = newEdge(srcVertex, otherV, label, dir, op = op, version = ts, propsWithTs = propsWithTs)

          val future = mutateEdges(Seq(edge), withWait = true).flatMap { rets =>
            indexProvider.mutateEdgesAsync(Seq(edge))
          }
          Await.ready(future, WaitTimeout)

          edge
        } catch {
          case e: LabelNotExistException => throw new java.lang.IllegalArgumentException(e)
        }
      case null => throw new java.lang.IllegalArgumentException
      case _ => throw new RuntimeException("only S2Graph vertex can be used.")
    }
  }

  override def close(): Unit = {
    shutdown()
  }

  override def compute[C <: GraphComputer](aClass: Class[C]): C = ???

  override def compute(): GraphComputer = {
    if (!features.graph.supportsComputer) {
      throw Graph.Exceptions.graphComputerNotSupported
    }
    ???
  }

  class S2GraphFeatures extends Features {
    import org.apache.s2graph.core.{features => FS}
    override def edge(): Features.EdgeFeatures = new FS.S2EdgeFeatures

    override def graph(): Features.GraphFeatures = new FS.S2GraphFeatures

    override def supports(featureClass: Class[_ <: Features.FeatureSet], feature: String): Boolean =
      super.supports(featureClass, feature)

    override def vertex(): Features.VertexFeatures = new FS.S2VertexFeatures

    override def toString: String = {
      s"FEATURES:\nEdgeFeatures:${edge}\nGraphFeatures:${graph}\nVertexFeatures:${vertex}"
    }
  }

  private val s2Features = new S2GraphFeatures

  override def features() = s2Features

  override def toString(): String = "[s2graph]"

  override def io[I <: Io[_ <: GraphReader.ReaderBuilder[_ <: GraphReader], _ <: GraphWriter.WriterBuilder[_ <: GraphWriter], _ <: Mapper.Builder[_]]](builder: Io.Builder[I]): I = {
    builder.graph(this).registry(S2GraphIoRegistry.instance).create().asInstanceOf[I]
  }


}
