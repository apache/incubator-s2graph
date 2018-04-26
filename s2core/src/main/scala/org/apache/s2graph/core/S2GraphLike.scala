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
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.Optional

import com.typesafe.config.Config
import org.apache.commons.configuration.Configuration
import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.S2Graph.{DefaultColumnName, DefaultServiceName}
import org.apache.s2graph.core.features.{S2Features, S2GraphVariables}
import org.apache.s2graph.core.index.IndexProvider
import org.apache.s2graph.core.model.ModelManager
import org.apache.s2graph.core.schema.{Label, LabelMeta, Service, ServiceColumn}
import org.apache.s2graph.core.storage.{MutateResponse, Storage}
import org.apache.s2graph.core.types.{InnerValLike, VertexId}
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure
import org.apache.tinkerpop.gremlin.structure.Edge.Exceptions
import org.apache.tinkerpop.gremlin.structure.Graph.Variables
import org.apache.tinkerpop.gremlin.structure.io.{GraphReader, GraphWriter, Io, Mapper}
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, Element, Graph, T, Transaction, Vertex}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._


trait S2GraphLike extends Graph {
  implicit val ec: ExecutionContext

  var apacheConfiguration: Configuration

  protected val localLongId = new AtomicLong()

  protected val s2Features = new S2Features

  val config: Config

  val management: Management

  val indexProvider: IndexProvider

  val elementBuilder: GraphElementBuilder

  val traversalHelper: TraversalHelper

  val modelManager: ModelManager

  lazy val MaxRetryNum: Int = config.getInt("max.retry.number")
  lazy val MaxBackOff: Int = config.getInt("max.back.off")
  lazy val BackoffTimeout: Int = config.getInt("back.off.timeout")
  lazy val DeleteAllFetchCount: Int = config.getInt("delete.all.fetch.count")
  lazy val DeleteAllFetchSize: Int = config.getInt("delete.all.fetch.size")
  lazy val FailProb: Double = config.getDouble("hbase.fail.prob")
  lazy val LockExpireDuration: Int = config.getInt("lock.expire.time")
  lazy val MaxSize: Int = config.getInt("future.cache.max.size")
  lazy val ExpireAfterWrite: Int = config.getInt("future.cache.expire.after.write")
  lazy val ExpireAfterAccess: Int = config.getInt("future.cache.expire.after.access")
  lazy val WaitTimeout: Duration = Duration(600, TimeUnit.SECONDS)

  override def features() = s2Features

  def fallback = Future.successful(StepResult.Empty)

  def defaultStorage: Storage

  def getStorage(service: Service): Storage

  def getStorage(label: Label): Storage

  def getFetcher(column: ServiceColumn): Fetcher

  def getFetcher(label: Label): Fetcher

  def flushStorage(): Unit

  def shutdown(modelDataDelete: Boolean = false): Unit

  def getVertices(vertices: Seq[S2VertexLike]): Future[Seq[S2VertexLike]]

  def getVerticesJava(vertices: util.List[S2VertexLike]): CompletableFuture[util.List[S2VertexLike]] =
    getVertices(vertices.toSeq).map(_.asJava).toJava.toCompletableFuture

  def checkEdges(edges: Seq[S2EdgeLike]): Future[StepResult]

  def checkEdgesJava(edges: util.List[S2EdgeLike]): CompletableFuture[StepResult] =
    checkEdges(edges.asScala).toJava.toCompletableFuture

  def mutateVertices(vertices: Seq[S2VertexLike], withWait: Boolean = false): Future[Seq[MutateResponse]]

  def mutateVerticesJava(vertices: util.List[S2VertexLike], withWait: JBoolean): CompletableFuture[util.List[MutateResponse]] =
    mutateVertices(vertices.asScala, withWait.booleanValue()).map(_.asJava).toJava.toCompletableFuture

  def mutateEdges(edges: Seq[S2EdgeLike], withWait: Boolean = false): Future[Seq[MutateResponse]]

  def mutateEdgesJava(edges: util.List[S2EdgeLike], withWait: JBoolean): CompletableFuture[util.List[MutateResponse]] =
    mutateEdges(edges.asScala, withWait.booleanValue()).map(_.asJava).toJava.toCompletableFuture

  def mutateElements(elements: Seq[GraphElement],
                     withWait: Boolean = false): Future[Seq[MutateResponse]]

  def mutateElementsJava(elements: util.List[GraphElement], withWait: JBoolean): CompletableFuture[util.List[MutateResponse]] =
    mutateElements(elements.asScala, withWait.booleanValue()).map(_.asJava).toJava.toCompletableFuture

  def getEdges(q: Query): Future[StepResult]

  def getEdgesJava(q: Query): CompletableFuture[StepResult] =
    getEdges(q).toJava.toCompletableFuture

  def getEdgesMultiQuery(mq: MultiQuery): Future[StepResult]

  def getEdgesMultiQueryJava(mq: MultiQuery): CompletableFuture[StepResult] =
    getEdgesMultiQuery(mq).toJava.toCompletableFuture

  def deleteAllAdjacentEdges(srcVertices: Seq[S2VertexLike],
                             labels: Seq[Label],
                             dir: Int,
                             ts: Long): Future[Boolean]

  def deleteAllAdjacentEdgesJava(srcVertices: util.List[S2VertexLike], labels: util.List[Label], direction: Direction): CompletableFuture[JBoolean] =
    deleteAllAdjacentEdges(srcVertices.asScala, labels.asScala, GraphUtil.toDirection(direction), System.currentTimeMillis()).map(JBoolean.valueOf(_)).toJava.toCompletableFuture

  def incrementCounts(edges: Seq[S2EdgeLike], withWait: Boolean): Future[Seq[MutateResponse]]

  def incrementCountsJava(edges: util.List[S2EdgeLike], withWait: JBoolean): CompletableFuture[util.List[MutateResponse]] =
    incrementCounts(edges.asScala, withWait.booleanValue()).map(_.asJava).toJava.toCompletableFuture

  def updateDegree(edge: S2EdgeLike, degreeVal: Long = 0): Future[MutateResponse]

  def updateDegreeJava(edge: S2EdgeLike, degreeVal: JLong): CompletableFuture[MutateResponse] =
    updateDegree(edge, degreeVal.longValue()).toJava.toCompletableFuture

  def getVertex(vertexId: VertexId): Option[S2VertexLike]

  def getVertexJava(vertexId: VertexId): Optional[S2VertexLike] =
    getVertex(vertexId).asJava

  def fetchEdges(vertex: S2VertexLike, labelNameWithDirs: Seq[(String, String)]): util.Iterator[Edge]

  def fetchEdgesJava(vertex: S2VertexLike, labelNameWithDirs: util.List[(String, String)]): util.Iterator[Edge] =
    fetchEdges(vertex, labelNameWithDirs.asScala)

  def edgesAsync(vertex: S2VertexLike, direction: Direction, labelNames: String*): Future[util.Iterator[Edge]]

  def edgesAsyncJava(vertex: S2VertexLike, direction: Direction, labelNames: String*): CompletableFuture[util.Iterator[Edge]] =
    edgesAsync(vertex, direction, labelNames: _*).toJava.toCompletableFuture

  /** Convert to Graph Element **/
  def toVertex(serviceName: String,
               columnName: String,
               id: Any,
               props: Map[String, Any] = Map.empty,
               ts: Long = System.currentTimeMillis(),
               operation: String = "insert"): S2VertexLike =
    elementBuilder.toVertex(serviceName, columnName, id, props, ts, operation)

  def toEdge(srcId: Any,
             tgtId: Any,
             labelName: String,
             direction: String,
             props: Map[String, Any] = Map.empty,
             ts: Long = System.currentTimeMillis(),
             operation: String = "insert"): S2EdgeLike =
    elementBuilder.toEdge(srcId, tgtId, labelName, direction, props, ts, operation)

  def toGraphElement(s: String, labelMapping: Map[String, String] = Map.empty): Option[GraphElement] =
    elementBuilder.toGraphElement(s, labelMapping)

  /** TinkerPop Interfaces **/
  def vertices(ids: AnyRef*): util.Iterator[structure.Vertex] = {
    val fetchVertices = ids.lastOption.map { lastParam =>
      if (lastParam.isInstanceOf[Boolean]) lastParam.asInstanceOf[Boolean]
      else true
    }.getOrElse(true)

    if (ids.isEmpty) {
      //TODO: default storage need to be fixed.
      Await.result(defaultStorage.fetchVerticesAll(), WaitTimeout).iterator
    } else {
      val vertices = ids.collect {
        case s2Vertex: S2VertexLike => s2Vertex
        case vId: VertexId => elementBuilder.newVertex(vId)
        case vertex: Vertex => elementBuilder.newVertex(vertex.id().asInstanceOf[VertexId])
        case other@_ => elementBuilder.newVertex(VertexId.fromString(other.toString))
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

  def edges(edgeIds: AnyRef*): util.Iterator[structure.Edge] = {
    if (edgeIds.isEmpty) {
      // FIXME
      Await.result(defaultStorage.fetchEdgesAll(), WaitTimeout).iterator
    } else {
      Await.result(edgesAsync(edgeIds: _*), WaitTimeout)
    }
  }

  def edgesAsync(edgeIds: AnyRef*): Future[util.Iterator[structure.Edge]] = {
    val s2EdgeIds = edgeIds.collect {
      case s2Edge: S2EdgeLike => s2Edge.id().asInstanceOf[EdgeId]
      case id: EdgeId => id
      case s: String => EdgeId.fromString(s)
    }
    val edgesToFetch = for {
      id <- s2EdgeIds
    } yield {
      elementBuilder.toEdge(id.srcVertexId, id.tgtVertexId, id.labelName, id.direction)
    }

    checkEdges(edgesToFetch).map { stepResult =>
      val ls = new util.ArrayList[structure.Edge]
      stepResult.edgeWithScores.foreach { es => ls.add(es.edge) }
      ls.iterator()
    }
  }

  def tx(): Transaction = {
    if (!features.graph.supportsTransactions) throw Graph.Exceptions.transactionsNotSupported
    ???
  }

  def variables(): Variables = new S2GraphVariables

  def configuration(): Configuration = apacheConfiguration

  def addVertex(label: String): Vertex = {
    if (label == null) throw Element.Exceptions.labelCanNotBeNull
    if (label.isEmpty) throw Element.Exceptions.labelCanNotBeEmpty

    addVertex(Seq(T.label, label): _*)
  }

  def addVertex(kvs: AnyRef*): structure.Vertex = {
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
        val id = localLongId.getAndIncrement()
        elementBuilder.makeVertex(Long.box(id), kvsMap)
      case Some(idValue) if S2Property.validType(idValue) =>
        elementBuilder.makeVertex(idValue, kvsMap)
      case _ =>
        throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported
    }

    addVertex(vertex.id, vertex.ts, vertex.props, vertex.op, vertex.belongLabelIds)

    vertex
  }

  def addVertex(id: VertexId,
                ts: Long = System.currentTimeMillis(),
                props: S2Vertex.Props = S2Vertex.EmptyProps,
                op: Byte = 0,
                belongLabelIds: Seq[Int] = Seq.empty): S2VertexLike = {
    val vertex = elementBuilder.newVertex(id, ts, props, op, belongLabelIds)

    val future = mutateVertices(Seq(vertex), withWait = true).flatMap { rets =>
      if (rets.forall(_.isSuccess)) {
        indexProvider.mutateVerticesAsync(Seq(vertex)).map { ls =>
          if (ls.forall(identity)) vertex
          else {
            throw new RuntimeException("indexVertex failed.")
          }
        }
      }
      else throw new RuntimeException("addVertex failed.")
    }
    Await.ready(future, WaitTimeout)

    vertex
  }

  /* tp3 only */
  def addEdge(srcVertex: S2VertexLike, labelName: String, tgtVertex: Vertex, kvs: AnyRef*): Edge = {
    val containsId = kvs.contains(T.id)

    tgtVertex match {
      case otherV: S2VertexLike =>
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

          val edge = elementBuilder.newEdge(srcVertex, otherV, label, dir, op = op, version = ts, propsWithTs = propsWithTs)

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

  def close(): Unit = {
    shutdown()
  }


  def compute[C <: GraphComputer](aClass: Class[C]): C = ???

  def compute(): GraphComputer = {
    if (!features.graph.supportsComputer) {
      throw Graph.Exceptions.graphComputerNotSupported
    }
    ???
  }

  def io[I <: Io[_ <: GraphReader.ReaderBuilder[_ <: GraphReader], _ <: GraphWriter.WriterBuilder[_ <: GraphWriter], _ <: Mapper.Builder[_]]](builder: Io.Builder[I]): I = {
    builder.graph(this).registry(S2GraphIoRegistry.instance).create().asInstanceOf[I]
  }

  override def toString(): String = "[s2graph]"
}
