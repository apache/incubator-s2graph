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
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.mysqls.{Label, Model}
import org.apache.s2graph.core.parsers.WhereParser
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorage
import org.apache.s2graph.core.types.{InnerVal, LabelWithDirection}
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.{JsObject, Json}

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
    "s2graph.storage.backend" -> "hbase",
    "query.hardlimit" -> java.lang.Integer.valueOf(100000)
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

  def alreadyVisitedVertices(edgeWithScoreLs: Seq[EdgeWithScore]): Map[(LabelWithDirection, Vertex), Boolean] = {
    val vertices = for {
      edgeWithScore <- edgeWithScoreLs
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
    /* process time decay */
    val tsVal = queryParam.timeDecay match {
      case None => 1.0
      case Some(timeDecay) =>
        val tsVal = try {
          val labelMeta = edge.label.metaPropsMap(timeDecay.labelMetaSeq)
          val innerValWithTsOpt = edge.propsWithTs.get(timeDecay.labelMetaSeq)
          innerValWithTsOpt.map { innerValWithTs =>
            val innerVal = innerValWithTs.innerVal
            labelMeta.dataType match {
              case InnerVal.LONG => innerVal.value match {
                case n: BigDecimal => n.bigDecimal.longValue()
                case _ => innerVal.toString().toLong
              }
              case _ => innerVal.toString().toLong
            }
          } getOrElse(edge.ts)
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

  def processDuplicates(queryParam: QueryParam,
                        duplicates: Seq[(FilterHashKey, EdgeWithScore)]): Seq[(FilterHashKey, EdgeWithScore)] = {

    if (queryParam.label.consistencyLevel != "strong") {
      //TODO:
      queryParam.duplicatePolicy match {
        case Query.DuplicatePolicy.First => Seq(duplicates.head)
        case Query.DuplicatePolicy.Raw => duplicates
        case Query.DuplicatePolicy.CountSum =>
          val countSum = duplicates.size
          Seq(duplicates.head._1 -> duplicates.head._2.copy(score = countSum))
        case _ =>
          val scoreSum = duplicates.foldLeft(0.0) { case (prev, current) => prev + current._2.score }
          Seq(duplicates.head._1 -> duplicates.head._2.copy(score = scoreSum))
      }
    } else {
      duplicates
    }
  }
  def filterEdges(q: Query,
                  stepIdx: Int,
                  queryRequests: Seq[QueryRequest],
                  queryResultLsFuture: Future[Seq[StepInnerResult]],
                  queryParams: Seq[QueryParam],
                  alreadyVisited: Map[(LabelWithDirection, Vertex), Boolean] = Map.empty[(LabelWithDirection, Vertex), Boolean])
                 (implicit ec: scala.concurrent.ExecutionContext): Future[StepInnerResult] = {

    queryResultLsFuture.map { queryRequestWithResultLs =>
      if (queryRequestWithResultLs.isEmpty) StepInnerResult.Empty
      else {
        val step = q.steps(stepIdx)

        val nextStepOpt = if (stepIdx < q.steps.size - 1) Option(q.steps(stepIdx + 1)) else None

        val excludeLabelWithDirSet = new util.HashSet[(Int, Int)]
        val includeLabelWithDirSet = new util.HashSet[(Int, Int)]
        step.queryParams.filter(_.exclude).foreach(l => excludeLabelWithDirSet.add(l.labelWithDir.labelId -> l.labelWithDir.dir))
        step.queryParams.filter(_.include).foreach(l => includeLabelWithDirSet.add(l.labelWithDir.labelId -> l.labelWithDir.dir))

        val edgesToExclude = new util.HashSet[FilterHashKey]()
        val edgesToInclude = new util.HashSet[FilterHashKey]()

        val sequentialLs = new ListBuffer[(HashKey, FilterHashKey, EdgeWithScore)]()
        val agg = new mutable.HashMap[HashKey, ListBuffer[(FilterHashKey, EdgeWithScore)]]()
        val params = new mutable.HashMap[HashKey, QueryParam]()
        var numOfDuplicates = 0
        var numOfTotal = 0
        queryRequests.zip(queryRequestWithResultLs).foreach { case (queryRequest, stepInnerResult) =>
          val queryParam = queryRequest.queryParam
          val labelWeight = step.labelWeights.getOrElse(queryParam.labelWithDir.labelId, 1.0)
          val includeExcludeKey = queryParam.labelWithDir.labelId -> queryParam.labelWithDir.dir
          val shouldBeExcluded = excludeLabelWithDirSet.contains(includeExcludeKey)
          val shouldBeIncluded = includeLabelWithDirSet.contains(includeExcludeKey)
          val where = queryParam.where.get

          for {
            edgeWithScore <- stepInnerResult.edgesWithScoreLs
            if where == WhereParser.success || where.filter(edgeWithScore.edge)
            (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
          } {
            numOfTotal += 1
            if (queryParam.transformer.isDefault) {
              val convertedEdge = edge

              val (hashKey, filterHashKey) = toHashKey(queryParam, convertedEdge, isDegree = false)

              /** check if this edge should be exlcuded. */
              if (shouldBeExcluded) {
                edgesToExclude.add(filterHashKey)
              } else {
                if (shouldBeIncluded) {
                  edgesToInclude.add(filterHashKey)
                }
                val tsVal = processTimeDecay(queryParam, convertedEdge)
                val newScore = labelWeight * score * tsVal
                val newEdgeWithScore = EdgeWithScore(convertedEdge, newScore)
                sequentialLs += ((hashKey, filterHashKey, newEdgeWithScore))
                agg.get(hashKey) match {
                  case None =>
                    val newLs = new ListBuffer[(FilterHashKey, EdgeWithScore)]()
                    newLs += (filterHashKey -> newEdgeWithScore)
                    agg += (hashKey -> newLs)
                  case Some(old) =>
                    numOfDuplicates += 1
                    old += (filterHashKey -> newEdgeWithScore)
                }
                params += (hashKey -> queryParam)
              }
            } else {
              convertEdges(queryParam, edge, nextStepOpt).foreach { convertedEdge =>
                val (hashKey, filterHashKey) = toHashKey(queryParam, convertedEdge, isDegree = false)

                /** check if this edge should be exlcuded. */
                if (shouldBeExcluded) {
                  edgesToExclude.add(filterHashKey)
                } else {
                  if (shouldBeIncluded) {
                    edgesToInclude.add(filterHashKey)
                  }
                  val tsVal = processTimeDecay(queryParam, convertedEdge)
                  val newScore = labelWeight * score * tsVal
                  val newEdgeWithScore = EdgeWithScore(convertedEdge, newScore)
                  sequentialLs += ((hashKey, filterHashKey, newEdgeWithScore))
                  agg.get(hashKey) match {
                    case None =>
                      val newLs = new ListBuffer[(FilterHashKey, EdgeWithScore)]()
                      newLs += (filterHashKey -> newEdgeWithScore)
                      agg += (hashKey -> newLs)
                    case Some(old) =>
                      numOfDuplicates += 1
                      old += (filterHashKey -> newEdgeWithScore)
                  }
                  params += (hashKey -> queryParam)
                }
              }
            }
          }
        }


        val edgeWithScoreLs = new ListBuffer[EdgeWithScore]()
        if (numOfDuplicates == 0) {
          // no duplicates at all.
          for {
            (hashKey, filterHashKey, edgeWithScore) <- sequentialLs
            if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
          } {
            edgeWithScoreLs += edgeWithScore
          }
        } else {
          // need to resolve duplicates.
          val seen = new mutable.HashSet[HashKey]()
          for {
            (hashKey, filterHashKey, edgeWithScore) <- sequentialLs
            if !seen.contains(hashKey)
            if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
          } {
            val queryParam = params(hashKey)
            val duplicates = processDuplicates(queryParam, agg(hashKey))
            duplicates.foreach { case (_, duplicate) =>
              if (duplicate.score >= queryParam.threshold) {
                seen += hashKey
                edgeWithScoreLs += duplicate
              }
            }
          }
        }

        val degrees = queryRequestWithResultLs.flatMap(_.degreeEdges)
        StepInnerResult(edgesWithScoreLs = edgeWithScoreLs, degreeEdges = degrees)
      }
    }
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


  def toVertex(s: String): Option[Vertex] = {
    toVertex(GraphUtil.split(s))
  }

  def toEdge(s: String): Option[Edge] = {
    toEdge(GraphUtil.split(s))
  }

  def toEdge(parts: Array[String]): Option[Edge] = Try {
    val (ts, operation, logType, srcId, tgtId, label) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) fromJsonToProperties(Json.parse(parts(6)).asOpt[JsObject].getOrElse(Json.obj())) else Map.empty[String, Any]
    val tempDirection = if (parts.length >= 8) parts(7) else "out"
    val direction = if (tempDirection != "out" && tempDirection != "in") "out" else tempDirection
    val edge = Edge.toEdge(srcId, tgtId, label, direction, props, ts.toLong, operation)
    Option(edge)
  } recover {
    case e: Exception =>
      logger.error(s"[toEdge]: $e", e)
      throw e
  } get

  def toVertex(parts: Array[String]): Option[Vertex] = Try {
    val (ts, operation, logType, srcId, serviceName, colName) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) fromJsonToProperties(Json.parse(parts(6)).asOpt[JsObject].getOrElse(Json.obj())) else Map.empty[String, Any]
    val vertex = Vertex.toVertex(serviceName, colName, srcId, props, ts.toLong, operation)
    Option(vertex)
  } recover {
    case e: Throwable =>
      logger.error(s"[toVertex]: $e", e)
      throw e
  } get

  def initStorage(graph: Graph, config: Config)(ec: ExecutionContext) = {
    config.getString("s2graph.storage.backend") match {
      case "hbase" => new AsynchbaseStorage(graph, config)(ec)
      case _ => throw new RuntimeException("not supported storage.")
    }
  }
}

class Graph(_config: Config)(implicit val ec: ExecutionContext) {
  val config = _config.withFallback(Graph.DefaultConfig)

  Model.apply(config)
  Model.loadCache()

  // TODO: Make storage client by config param
  val storage = Graph.initStorage(this, config)(ec)


  for {
    entry <- config.entrySet() if Graph.DefaultConfigs.contains(entry.getKey)
    (k, v) = (entry.getKey, entry.getValue)
  } logger.info(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")

  /** select */
  def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[StepResult] = storage.checkEdges(params)

  def getEdges(q: Query): Future[StepResult] = storage.getEdges(q)

  def getEdgesMultiQuery(mq: MultiQuery): Future[StepResult] = storage.getEdgesMultiQuery(mq)

  def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = storage.getVertices(vertices)

  /** write */
  def deleteAllAdjacentEdges(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] =
    storage.deleteAllAdjacentEdges(srcVertices, labels, dir, ts)

  def mutateElements(elements: Seq[GraphElement], withWait: Boolean = false): Future[Seq[Boolean]] =
    storage.mutateElements(elements, withWait)

  def mutateEdges(edges: Seq[Edge], withWait: Boolean = false): Future[Seq[Boolean]] = storage.mutateEdges(edges, withWait)

  def mutateVertices(vertices: Seq[Vertex], withWait: Boolean = false): Future[Seq[Boolean]] = storage.mutateVertices(vertices, withWait)

  def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = storage.incrementCounts(edges, withWait)

  def shutdown(): Unit = {
    storage.flush()
    Model.shutdown()
  }
}
