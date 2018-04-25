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

import org.apache.s2graph.core.S2Edge.State
import org.apache.s2graph.core.schema.{Label, LabelIndex, LabelMeta}
import org.apache.s2graph.core.types.{InnerValLike, TargetVertexId, VertexId}
import org.apache.tinkerpop.gremlin.structure.Property

import scala.collection.JavaConverters._

class S2EdgeBuilder(edge: S2EdgeLike) {
  def srcForVertex = S2Edge.srcForVertex(edge)

  def tgtForVertex = S2Edge.tgtForVertex(edge)

  def duplicateEdge = reverseSrcTgtEdge.reverseDirEdge

  def reverseDirEdge = copyEdge(dir = GraphUtil.toggleDir(edge.getDir))

  def reverseSrcTgtEdge = copyEdge(srcVertex = edge.tgtVertex, tgtVertex = edge.srcVertex)

  def isDegree = edge.getPropsWithTs().containsKey(LabelMeta.degree.name)

  def propsPlusTsValid = edge.getPropsWithTs().asScala.filter(kv => LabelMeta.isValidSeq(kv._2.labelMeta.seq)).asJava

  def labelOrders = LabelIndex.findByLabelIdAll(edge.getLabelId())

  def edgesWithIndex = for (labelOrder <- labelOrders) yield {
    IndexEdge(edge.innerGraph, edge.srcVertex, edge.tgtVertex, edge.innerLabel, edge.getDir(), edge.getOp(),
      edge.getVersion(), labelOrder.seq, edge.getPropsWithTs(), tsInnerValOpt = edge.getTsInnerValOpt())
  }

  def edgesWithIndexValid = for (labelOrder <- labelOrders) yield {
    IndexEdge(edge.innerGraph, edge.srcVertex, edge.tgtVertex, edge.innerLabel, edge.getDir(), edge.getOp(),
      edge.getVersion(), labelOrder.seq, propsPlusTsValid, tsInnerValOpt = edge.getTsInnerValOpt())
  }

  def relatedEdges: Seq[S2EdgeLike] = {
    if (edge.isDirected()) {
      val skipReverse = edge.innerLabel.extraOptions.get("skipReverse").map(_.as[Boolean]).getOrElse(false)
      if (skipReverse) Seq(edge) else Seq(edge, duplicateEdge)
    } else {
      //      val outDir = labelWithDir.copy(dir = GraphUtil.directions("out"))
      //      val base = copy(labelWithDir = outDir)
      val base = copyEdge(dir = GraphUtil.directions("out"))
      Seq(base, base.reverseSrcTgtEdge)
    }
  }

  def copyEdge(innerGraph: S2GraphLike = edge.innerGraph,
               srcVertex: S2VertexLike = edge.srcVertex,
               tgtVertex: S2VertexLike = edge.tgtVertex,
               innerLabel: Label = edge.innerLabel,
               dir: Int = edge.getDir(),
               op: Byte = edge.getOp(),
               version: Long = edge.getVersion(),
               propsWithTs: State = S2Edge.propsToState(edge.getPropsWithTs()),
               parentEdges: Seq[EdgeWithScore] = edge.getParentEdges(),
               originalEdgeOpt: Option[S2EdgeLike] = edge.getOriginalEdgeOpt(),
               pendingEdgeOpt: Option[S2EdgeLike] = edge.getPendingEdgeOpt(),
               statusCode: Byte = edge.getStatusCode(),
               lockTs: Option[Long] = edge.getLockTs(),
               tsInnerValOpt: Option[InnerValLike] = edge.getTsInnerValOpt(),
               ts: Long = edge.getTs()): S2EdgeLike = {
    val edge = new S2Edge(innerGraph, srcVertex, tgtVertex, innerLabel, dir, op, version, S2Edge.EmptyProps,
      parentEdges, originalEdgeOpt, pendingEdgeOpt, statusCode, lockTs, tsInnerValOpt)
    S2Edge.fillPropsWithTs(edge, propsWithTs)
    edge.propertyInner(LabelMeta.timestamp.name, ts, ts)
    edge
  }

  def copyEdgeWithState(state: State): S2EdgeLike = {
    val newEdge = new S2Edge(edge.innerGraph, edge.srcVertex, edge.tgtVertex, edge.innerLabel,
      edge.getDir(), edge.getOp(), edge.getVersion(), S2Edge.EmptyProps,
      edge.getParentEdges(), edge.getOriginalEdgeOpt(), edge.getPendingEdgeOpt(),
      edge.getStatusCode(), edge.getLockTs(), edge.getTsInnerValOpt())

    S2Edge.fillPropsWithTs(newEdge, state)
    newEdge
  }

  def updateTgtVertex(id: InnerValLike): S2EdgeLike = {
    val newId = TargetVertexId(edge.tgtVertex.id.column, id)
    val newTgtVertex = edge.innerGraph.elementBuilder.newVertex(newId, edge.tgtVertex.ts, edge.tgtVertex.props)
    copyEdge(tgtVertex = newTgtVertex)
  }

  def edgeId: EdgeId = {
    val timestamp = if (edge.innerLabel.consistencyLevel == "strong") 0l else edge.ts
    //    EdgeId(srcVertex.innerId, tgtVertex.innerId, label(), "out", timestamp)
    val (srcColumn, tgtColumn) = edge.innerLabel.srcTgtColumn(edge.getDir())
    if (edge.getDir() == GraphUtil.directions("out"))
      EdgeId(VertexId(srcColumn, edge.srcVertex.id.innerId), VertexId(tgtColumn, edge.tgtVertex.id.innerId), edge.label(), "out", timestamp)
    else
      EdgeId(VertexId(tgtColumn, edge.tgtVertex.id.innerId), VertexId(srcColumn, edge.srcVertex.id.innerId), edge.label(), "out", timestamp)
  }
}
