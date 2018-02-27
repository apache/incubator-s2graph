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
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.core.types._
import org.apache.s2graph.graphql.types.S2ManagementType._
import org.apache.s2graph.graphql.types.S2Type._
import sangria.schema.{Action, Args}

import scala.concurrent._
import scala.util.{Failure, Try}


/**
  *
  * @param graph
  */
class GraphRepository(val graph: S2GraphLike) {
  val management = graph.management
  val parser = new RequestParser(graph)

  implicit val ec = graph.ec

  def partialServiceParamToVertex(column: ServiceColumn, param: PartialServiceParam): S2VertexLike = {
    val vid = JSONParser.toInnerVal(param.vid, column.columnType, column.schemaVersion)
    graph.toVertex(param.service.serviceName, column.columnName, vid)
  }

  def partialVertexParamToS2Vertex(serviceName: String, columnName: String, param: PartialVertexParam): S2VertexLike = {
    graph.toVertex(
      serviceName = serviceName,
      columnName = columnName,
      id = param.id,
      props = param.props,
      ts = param.ts)
  }

  def partialEdgeParamToS2Edge(labelName: String, param: PartialEdgeParam): S2EdgeLike = {
    graph.toEdge(
      srcId = param.from,
      tgtId = param.to,
      labelName = labelName,
      props = param.props,
      direction = param.direction
    )
  }

  def addVertex(args: Args): Future[Option[MutateResponse]] = {
    val vertices: Seq[S2VertexLike] = args.raw.keys.toList.flatMap { serviceName =>
      val innerMap = args.arg[Vector[PartialServiceVertexParam]](serviceName)
      val ret = innerMap.map { param =>
        partialVertexParamToS2Vertex(serviceName, param.columnName, param.vertexParam)
      }

      ret
    }

    graph.mutateVertices(vertices, withWait = true).map(_.headOption)
  }

  def addVertices(args: Args): Future[Seq[MutateResponse]] = {
    val vertices: Seq[S2VertexLike] = args.raw.keys.toList.flatMap { serviceName =>
      val innerMap = args.arg[Map[String, Vector[PartialVertexParam]]](serviceName)

      innerMap.flatMap { case (columnName, params) =>
          params.map { param =>
            partialVertexParamToS2Vertex(serviceName, columnName, param)
          }
      }
    }
    graph.mutateVertices(vertices, withWait = true)
  }

  def addEdges(args: Args): Future[Seq[MutateResponse]] = {
    val edges: Seq[S2EdgeLike] = args.raw.keys.toList.flatMap { labelName =>
      val params = args.arg[Vector[PartialEdgeParam]](labelName)
      params.map(param => partialEdgeParamToS2Edge(labelName, param))
    }

    graph.mutateEdges(edges, withWait = true)
  }

  def addEdge(args: Args): Future[Option[MutateResponse]] = {
    val edges: Seq[S2EdgeLike] = args.raw.keys.toList.map { labelName =>
      val param = args.arg[PartialEdgeParam](labelName)
      partialEdgeParamToS2Edge(labelName, param)
    }

    graph.mutateEdges(edges, withWait = true).map(_.headOption)
  }

  def getVertex(vertex: S2VertexLike): Future[Seq[S2VertexLike]] = {
    val f = graph.getVertices(Seq(vertex))
    f.foreach{ a =>
      println(a)
    }
    f
  }

  def getEdges(vertex: S2VertexLike, label: Label, _dir: String): Future[Seq[S2EdgeLike]] = {
    val dir = GraphUtil.directions(_dir)
    val labelWithDir = LabelWithDirection(label.id.get, dir)
    val step = Step(Seq(QueryParam(labelWithDir)))
    val q = Query(Seq(vertex), steps = Vector(step))

    graph.getEdges(q).map(_.edgeWithScores.map(_.edge))
  }

  def createService(args: Args): Try[Service] = {
    val serviceName = args.arg[String]("name")

    Service.findByName(serviceName) match {
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
  }

  def createServiceColumn(args: Args): Try[ServiceColumn] = {
    val serviceName = args.arg[String]("serviceName")
    val columnName = args.arg[String]("columnName")
    val columnType = args.arg[String]("columnType")
    val props = args.argOpt[Vector[Prop]]("props").getOrElse(Vector.empty)

    Try { management.createServiceColumn(serviceName, columnName, columnType, props) }
  }

  def deleteServiceColumn(args: Args): Try[ServiceColumn] = {
    val serviceName = args.arg[String]("serviceName")
    val columnName = args.arg[String]("columnName")

    Management.deleteColumn(serviceName, columnName)
  }

  def createLabel(args: Args): Try[Label] = {
    val labelName = args.arg[String]("name")

    val srcServiceProp = args.arg[LabelServiceProp]("sourceService")
    val tgtServiceProp = args.arg[LabelServiceProp]("targetService")

    val allProps = args.argOpt[Vector[Prop]]("props").getOrElse(Vector.empty)
    val indices = args.argOpt[Vector[Index]]("indices").getOrElse(Vector.empty)

    val serviceName = args.argOpt[String]("serviceName").getOrElse(tgtServiceProp.name)
    val consistencyLevel = args.argOpt[String]("consistencyLevel").getOrElse("weak")
    val hTableName = args.argOpt[String]("hTableName")
    val hTableTTL = args.argOpt[Int]("hTableTTL")
    val schemaVersion = args.argOpt[String]("schemaVersion").getOrElse(HBaseType.DEFAULT_VERSION)
    val isAsync = args.argOpt("isAsync").getOrElse(false)
    val compressionAlgorithm = args.argOpt[String]("compressionAlgorithm").getOrElse(parser.DefaultCompressionAlgorithm)
    val isDirected = args.argOpt[Boolean]("isDirected").getOrElse(true)
    val options = args.argOpt[String]("options") // TODO: support option type

    val labelTry: scala.util.Try[Label] = management.createLabel(
      labelName,
      srcServiceProp.name,
      srcServiceProp.columnName,
      srcServiceProp.dataType,
      tgtServiceProp.name,
      tgtServiceProp.columnName,
      tgtServiceProp.dataType,
      isDirected,
      serviceName,
      indices,
      allProps,
      consistencyLevel,
      hTableName,
      hTableTTL,
      schemaVersion,
      isAsync,
      compressionAlgorithm,
      options
    )

    labelTry
  }

  def deleteLabel(args: Args): Try[Label] = {
    val labelName = args.arg[String]("name")

    Management.deleteLabel(labelName)
  }

  def allServices: List[Service] = Service.findAll()

  def allServiceColumns: List[ServiceColumn] = ServiceColumn.findAll()

  def findServiceByName(name: String): Option[Service] = Service.findByName(name)

  def allLabels: List[Label] = Label.findAll()

  def findLabelByName(name: String): Option[Label] = Label.findByName(name)

}
