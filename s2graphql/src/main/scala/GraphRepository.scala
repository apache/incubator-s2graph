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

package org.apache.s2graph

import org.apache.s2graph.S2Type._
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, Service, ServiceColumn}
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.core.types.{HBaseType, LabelWithDirection}
import play.api.libs.json._
import sangria.schema.{Action, Args}

import scala.concurrent._
import scala.util.{Failure, Success, Try}


/**
  *
  * @param graph
  */
class GraphRepository(graph: S2GraphLike) {

  val management = graph.management
  val parser = new RequestParser(graph)

  implicit val ec = graph.ec

  def partialVertexParamToVertex(column: ServiceColumn, param: PartialVertexParam): S2VertexLike = {
    val vid = JSONParser.jsValueToInnerVal(param.vid, column.columnType, column.schemaVersion).get
    graph.toVertex(param.service.serviceName, column.columnName, vid)
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

  def addEdges(args: Args): Future[Seq[MutateResponse]] = {
    val edges: Seq[S2EdgeLike] = args.raw.keys.toList.flatMap { labelName =>
      val params = args.arg[Vector[PartialEdgeParam]](labelName)
      params.map(param => partialEdgeParamToS2Edge(labelName, param))
    }

    graph.mutateEdges(edges)
  }

  def addEdge(args: Args): Future[Option[MutateResponse]] = {
    val edges: Seq[S2EdgeLike] = args.raw.keys.toList.map { labelName =>
      val param = args.arg[PartialEdgeParam](labelName)
      partialEdgeParamToS2Edge(labelName, param)
    }

    graph.mutateEdges(edges).map(_.headOption)
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

  def allServices: List[Service] = Service.findAll()

  def findServiceByName(name: String): Option[Service] = Service.findByName(name)

  def allLabels: List[Label] = Label.findAll()

  def findLabelByName(name: String): Option[Label] = Label.findByName(name)

}
