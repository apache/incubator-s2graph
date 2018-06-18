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

package org.apache.s2graph.graphql.repository

import org.apache.s2graph.core.Management.JsonModel._
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.core.types._
import org.apache.s2graph.graphql.types.S2Type._
import org.slf4j.{Logger, LoggerFactory}
import sangria.execution.deferred._
import sangria.schema._

import scala.collection.immutable
import scala.concurrent._
import scala.util.{Failure, Success, Try}

object GraphRepository {

  implicit val vertexHasId = new HasId[(VertexQueryParam, Seq[S2VertexLike]), VertexQueryParam] {
    override def id(value: (VertexQueryParam, Seq[S2VertexLike])): VertexQueryParam = value._1
  }

  implicit val edgeHasId = new HasId[(EdgeQueryParam, Seq[S2EdgeLike]), EdgeQueryParam] {
    override def id(value: (EdgeQueryParam, Seq[S2EdgeLike])): EdgeQueryParam = value._1
  }

  val vertexFetcher =
    Fetcher((ctx: GraphRepository, queryParams: Seq[VertexQueryParam]) => {
      implicit val ec = ctx.ec

      Future.traverse(queryParams)(ctx.getVertices).map(vs => queryParams.zip(vs))
    })

  val edgeFetcher = Fetcher((ctx: GraphRepository, edgeQueryParams: Seq[EdgeQueryParam]) => {
    implicit val ec = ctx.ec

    val edgesByParam = edgeQueryParams.groupBy(_.qp).toSeq.map { case (qp, edgeQueryParams) =>
      val vertices = edgeQueryParams.map(_.v)
      ctx.getEdges(vertices, qp).map(edges => qp -> edges)
    }

    val f: Future[Seq[(QueryParam, Seq[S2EdgeLike])]] = Future.sequence(edgesByParam)
    val grouped: Future[Seq[(EdgeQueryParam, Seq[S2EdgeLike])]] = f.map { tpLs =>
      tpLs.flatMap { case (qp, edges) =>
        edges.groupBy(_.srcForVertex).map {
          case (v, edges) => EdgeQueryParam(v, qp) -> edges
        }
      }
    }

    grouped
  })

  def withLogTryResponse[A](opName: String, tryObj: Try[A])(implicit logger: Logger): Try[A] = {
    tryObj match {
      case Success(v) => logger.info(s"${opName} Success:", v)
      case Failure(e) => logger.warn(s"${opName} Failed:", e)
    }

    tryObj
  }
}

class GraphRepository(val graph: S2GraphLike) {
  implicit val logger = LoggerFactory.getLogger(this.getClass)

  import GraphRepository._

  val management = graph.management
  val parser = new RequestParser(graph)

  implicit val ec = graph.ec

  def toS2EdgeLike(labelName: String, param: AddEdgeParam): S2EdgeLike = {
    graph.toEdge(
      srcId = param.from,
      tgtId = param.to,
      labelName = labelName,
      props = param.props,
      direction = param.direction
    )
  }

  def toS2VertexLike(vid: Any, column: ServiceColumn): S2VertexLike = {
    graph.toVertex(column.service.serviceName, column.columnName, vid)
  }

  def toS2VertexLike(serviceName: String, param: AddVertexParam): S2VertexLike = {
    graph.toVertex(
      serviceName = serviceName,
      columnName = param.columnName,
      id = param.id,
      props = param.props,
      ts = param.timestamp)
  }

  def addVertices(vertices: Seq[S2VertexLike]): Future[Seq[MutateResponse]] = {
    graph.mutateVertices(vertices, withWait = true)
  }

  def addEdges(edges: Seq[S2EdgeLike]): Future[Seq[MutateResponse]] = {
    graph.mutateEdges(edges, withWait = true)
  }

  def getVertices(queryParam: VertexQueryParam): Future[Seq[S2VertexLike]] = {
    graph.asInstanceOf[S2Graph].searchVertices(queryParam).map { v =>
      v
    }
  }

  def getEdges(vertices: Seq[S2VertexLike], queryParam: QueryParam): Future[Seq[S2EdgeLike]] = {
    val step = Step(Seq(queryParam))
    val q = Query(vertices, steps = Vector(step))

    graph.getEdges(q).map(_.edgeWithScores.map(_.edge))
  }

  def getEdges(vertex: S2VertexLike, queryParam: QueryParam): Future[Seq[S2EdgeLike]] = {
    val step = Step(Seq(queryParam))
    val q = Query(Seq(vertex), steps = Vector(step))

    graph.getEdges(q).map(_.edgeWithScores.map(_.edge))
  }

  def createService(args: Args): Try[Service] = {
    val serviceName = args.arg[String]("name")

    val serviceTry = Service.findByName(serviceName) match {
      case Some(_) => Failure(new RuntimeException(s"Service (${serviceName}) already exists"))
      case None =>
        val cluster = args.argOpt[String]("cluster").getOrElse(parser.DefaultCluster)
        val hTableName = args.argOpt[String]("hTableName").getOrElse(s"${serviceName}-${parser.DefaultPhase}")
        val preSplitSize = args.argOpt[Int]("preSplitSize").getOrElse(1)
        val hTableTTL = args.argOpt[Int]("hTableTTL")
        val compressionAlgorithm = args.argOpt[String]("compressionAlgorithm").getOrElse(parser.DefaultCompressionAlgorithm)

        val serviceTry = management
          .createService(serviceName,
            cluster,
            hTableName,
            preSplitSize,
            hTableTTL,
            compressionAlgorithm)

        serviceTry
    }

    withLogTryResponse("createService", serviceTry)
  }

  def createServiceColumn(args: Args): Try[ServiceColumn] = {
    val serviceName = args.arg[String]("serviceName")
    val columnName = args.arg[String]("columnName")
    val columnType = args.arg[String]("columnType")
    val props = args.argOpt[Vector[Prop]]("props").getOrElse(Vector.empty)

    val tryColumn = Try {
      management.createServiceColumn(serviceName, columnName, columnType, props)
    }

    withLogTryResponse("createServiceColumn", tryColumn)
  }

  def deleteServiceColumn(args: Args): Try[ServiceColumn] = {
    val serviceColumnParam = args.arg[ServiceColumnParam]("service")

    val serviceName = serviceColumnParam.serviceName
    val columnName = serviceColumnParam.columnName

    val deleteTry = Management.deleteColumn(serviceName, columnName)

    withLogTryResponse("deleteServiceColumn", deleteTry)
  }

  def addPropsToLabel(args: Args): Try[Label] = {
    val addPropToLabelTry = Try {
      val labelName = args.arg[String]("labelName")
      val props = args.arg[Vector[Prop]]("props").toList

      props.foreach { prop =>
        Management.addProp(labelName, prop).get
      }

      Label.findByName(labelName, false).get
    }

    withLogTryResponse("addPropToLabel", addPropToLabelTry)
  }

  def addPropsToServiceColumn(args: Args): Try[ServiceColumn] = {
    val addPropsToServiceColumnTry = Try {
      val serviceColumnParam = args.arg[ServiceColumnParam]("service")

      val serviceName = serviceColumnParam.serviceName
      val columnName = serviceColumnParam.columnName

      serviceColumnParam.props.foreach { prop =>
        Management.addVertexProp(serviceName, columnName, prop.name, prop.dataType, prop.defaultValue, prop.storeInGlobalIndex)
      }

      val src = Service.findByName(serviceName)
      ServiceColumn.find(src.get.id.get, columnName, false).get
    }

    withLogTryResponse("addPropsToServiceColumn", addPropsToServiceColumnTry)
  }

  def createLabel(args: Args): Try[Label] = {
    val labelName = args.arg[String]("name")

    val srcServiceProp = args.arg[ServiceColumnParam]("sourceService")
    val srcServiceColumn = ServiceColumn.find(Service.findByName(srcServiceProp.serviceName).get.id.get, srcServiceProp.columnName).get
    val tgtServiceProp = args.arg[ServiceColumnParam]("targetService")
    val tgtServiceColumn = ServiceColumn.find(Service.findByName(tgtServiceProp.serviceName).get.id.get, tgtServiceProp.columnName).get

    val allProps = args.argOpt[Vector[Prop]]("props").getOrElse(Vector.empty)
    val indices = args.argOpt[Vector[Index]]("indices").getOrElse(Vector.empty)

    val serviceName = args.argOpt[String]("serviceName").getOrElse(tgtServiceColumn.service.serviceName)
    val consistencyLevel = args.argOpt[String]("consistencyLevel").getOrElse("weak")
    val hTableName = args.argOpt[String]("hTableName")
    val hTableTTL = args.argOpt[Int]("hTableTTL")
    val schemaVersion = args.argOpt[String]("schemaVersion").getOrElse(HBaseType.DEFAULT_VERSION)
    val isAsync = args.argOpt("isAsync").getOrElse(false)
    val compressionAlgorithm = args.argOpt[String]("compressionAlgorithm").getOrElse(parser.DefaultCompressionAlgorithm)
    val isDirected = args.argOpt[Boolean]("isDirected").getOrElse(true)
    //    val options = args.argOpt[String]("options") // TODO: support option type
    val options = Option("""{"storeVertex": true}""")

    val labelTry: scala.util.Try[Label] = management.createLabel(
      labelName,
      srcServiceProp.serviceName,
      srcServiceColumn.columnName,
      srcServiceColumn.columnType,
      tgtServiceProp.serviceName,
      tgtServiceColumn.columnName,
      tgtServiceColumn.columnType,
      serviceName,
      indices,
      allProps,
      isDirected,
      consistencyLevel,
      hTableName,
      hTableTTL,
      schemaVersion,
      isAsync,
      compressionAlgorithm,
      options
    )

    withLogTryResponse("createLabel", labelTry)
  }

  def deleteLabel(args: Args): Try[Label] = {
    val labelName = args.arg[String]("name")

    val deleteLabelTry = Management.deleteLabel(labelName)
    withLogTryResponse("deleteLabel", deleteLabelTry)
  }

  def services(): List[Service] = Service.findAll()

  def serviceColumns(): List[ServiceColumn] = ServiceColumn.findAll()

  def labels() = Label.findAll()
}
