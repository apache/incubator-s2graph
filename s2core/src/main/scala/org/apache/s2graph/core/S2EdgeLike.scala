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
import java.util.function.BiConsumer

import org.apache.s2graph.core.S2Edge.{Props, State}
import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.types._
import org.apache.tinkerpop.gremlin.structure
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, Graph, Property, T, Vertex}
import play.api.libs.json.Json

import scala.concurrent.Await

trait S2EdgeLike extends Edge with GraphElement {
  val innerGraph: S2GraphLike
  val srcVertex: S2VertexLike
  var tgtVertex: S2VertexLike
  val innerLabel: Label
  val dir: Int

  val builder: S2EdgeBuilder = new S2EdgeBuilder(this)

  var op: Byte
  var version: Long
  var tsInnerValOpt: Option[InnerValLike]

  val propsWithTs: Props = S2Edge.EmptyProps

  val parentEdges: Seq[EdgeWithScore] = Nil
  val originalEdgeOpt: Option[S2EdgeLike] = None
  val pendingEdgeOpt: Option[S2EdgeLike] = None
  val statusCode: Byte = 0
  val lockTs: Option[Long] = None

  lazy val ts = propsWithTs.get(LabelMeta.timestamp.name).innerVal.value match {
    case b: BigDecimal => b.longValue()
    case l: Long => l
    case i: Int => i.toLong
    case _ => throw new RuntimeException("ts should be in [BigDecimal/Long/Int].")
  }

  private lazy val operation = GraphUtil.fromOp(op)
  private lazy val direction = GraphUtil.fromDirection(dir)
  private lazy val tsInnerVal = tsInnerValOpt.get.value

  def graph(): Graph = innerGraph

  lazy val edgeId: EdgeId = builder.edgeId

  def id(): AnyRef = edgeId

  def label(): String = innerLabel.label

  def getLabelId(): Int = innerLabel.id.get

  def getDirection(): String = direction

  def getOperation(): String = operation

  def getTsInnerValValue(): Any = tsInnerVal

  def isDirected(): Boolean =
    getDir() == 0 || getDir() == 1

  def getTs(): Long = ts
  def getOriginalEdgeOpt(): Option[S2EdgeLike] = originalEdgeOpt
  def getParentEdges(): Seq[EdgeWithScore] = parentEdges
  def getPendingEdgeOpt(): Option[S2EdgeLike] = pendingEdgeOpt
  def getPropsWithTs(): Props = propsWithTs
  def getLockTs(): Option[Long] = lockTs
  def getStatusCode(): Byte = statusCode
  def getDir(): Int = dir
  def setTgtVertex(v: S2VertexLike): Unit = tgtVertex = v
  def getOp(): Byte = op
  def setOp(newOp: Byte): Unit = op = newOp
  def getVersion(): Long = version
  def setVersion(newVersion: Long): Unit = version = newVersion
  def getTsInnerValOpt(): Option[InnerValLike] = tsInnerValOpt
  def setTsInnerValOpt(newTsInnerValOpt: Option[InnerValLike]): Unit = tsInnerValOpt = newTsInnerValOpt

  def toIndexEdge(labelIndexSeq: Byte): IndexEdge = IndexEdge(innerGraph, srcVertex, tgtVertex, innerLabel, dir, op, version, labelIndexSeq, propsWithTs)

  def updatePropsWithTs(others: Props = S2Edge.EmptyProps): Props =
    S2EdgePropertyHelper.updatePropsWithTs(this, others)

  def propertyValue(key: String): Option[InnerValLikeWithTs] = S2EdgePropertyHelper.propertyValue(this, key)

  def propertyValueInner(labelMeta: LabelMeta): InnerValLikeWithTs =
    S2EdgePropertyHelper.propertyValueInner(this, labelMeta)

  def propertyValues(keys: Seq[String] = Nil): Map[LabelMeta, InnerValLikeWithTs] = {
    S2EdgePropertyHelper.propertyValuesInner(this, S2EdgePropertyHelper.toLabelMetas(this, keys))
  }

  def propertyValuesInner(labelMetas: Seq[LabelMeta] = Nil): Map[LabelMeta, InnerValLikeWithTs] =
    S2EdgePropertyHelper.propertyValuesInner(this, labelMetas)

  def relatedEdges = builder.relatedEdges

  def srcForVertex = builder.srcForVertex

  def tgtForVertex = builder.tgtForVertex

  def duplicateEdge = builder.duplicateEdge

  def reverseDirEdge = builder.reverseDirEdge

  def reverseSrcTgtEdge = builder.reverseSrcTgtEdge

  def isDegree = builder.isDegree

  def edgesWithIndex = builder.edgesWithIndex

  def edgesWithIndexValid = builder.edgesWithIndexValid

  /** force direction as out on invertedEdge */
  def toSnapshotEdge: SnapshotEdge = SnapshotEdge.apply(this)

  def checkProperty(key: String): Boolean = propsWithTs.containsKey(key)

  def copyEdgeWithState(state: State): S2EdgeLike = {
    builder.copyEdgeWithState(state)
  }

  def copyOp(newOp: Byte): S2EdgeLike = {
    builder.copyEdge(op = newOp)
  }

  def copyVersion(newVersion: Long): S2EdgeLike = {
    builder.copyEdge(version = newVersion)
  }

  def copyParentEdges(parents: Seq[EdgeWithScore]): S2EdgeLike = {
    builder.copyEdge(parentEdges = parents)
  }

  def copyOriginalEdgeOpt(newOriginalEdgeOpt: Option[S2EdgeLike]): S2EdgeLike =
    builder.copyEdge(originalEdgeOpt = newOriginalEdgeOpt)

  def copyStatusCode(newStatusCode: Byte): S2EdgeLike = {
    builder.copyEdge(statusCode = newStatusCode)
  }

  def copyLockTs(newLockTs: Option[Long]): S2EdgeLike = {
    builder.copyEdge(lockTs = newLockTs)
  }

  def copyTs(newTs: Long): S2EdgeLike =
    builder.copyEdge(ts = newTs)

  def updateTgtVertex(id: InnerValLike): S2EdgeLike =
    builder.updateTgtVertex(id)

  def vertices(direction: Direction): util.Iterator[structure.Vertex] = {
    val arr = new util.ArrayList[Vertex]()

    direction match {
      case Direction.OUT =>
        val newVertexId = edgeId.srcVertexId
        innerGraph.getVertex(newVertexId).foreach(arr.add)
      case Direction.IN =>
        val newVertexId = edgeId.tgtVertexId
        innerGraph.getVertex(newVertexId).foreach(arr.add)
      case _ =>
        import scala.collection.JavaConversions._
        vertices(Direction.OUT).foreach(arr.add)
        vertices(Direction.IN).foreach(arr.add)
    }
    arr.iterator()
  }

  def properties[V](keys: String*): util.Iterator[Property[V]] = {
    val ls = new util.ArrayList[Property[V]]()
    if (keys.isEmpty) {
      propsWithTs.forEach(new BiConsumer[String, S2Property[_]] {
        override def accept(key: String, property: S2Property[_]): Unit = {
          if (!LabelMeta.reservedMetaNamesSet(key) && property.isPresent && key != T.id.name)
            ls.add(property.asInstanceOf[S2Property[V]])
        }
      })
    } else {
      keys.foreach { key =>
        val prop = property[V](key)
        if (prop.isPresent) ls.add(prop)
      }
    }
    ls.iterator()
  }

  def property[V](key: String): Property[V] = {
    if (propsWithTs.containsKey(key)) propsWithTs.get(key).asInstanceOf[Property[V]]
    else Property.empty()
  }

  def property[V](key: String, value: V): Property[V] = {
    S2Property.assertValidProp(key, value)

    val v = propertyInner(key, value, System.currentTimeMillis())
    val newTs =
      if (propsWithTs.containsKey(LabelMeta.timestamp.name))
        propsWithTs.get(LabelMeta.timestamp.name).innerVal.toString().toLong + 1
      else
        System.currentTimeMillis()

    val newEdge = builder.copyEdge(ts = newTs)

    Await.result(innerGraph.mutateEdges(Seq(newEdge), withWait = true), innerGraph.WaitTimeout)

    v
  }

  def propertyInner[V](key: String, value: V, ts: Long): Property[V] =
    S2EdgePropertyHelper.propertyInner(this, key, value, ts)

  def remove(): Unit = {
    if (graph.features().edge().supportsRemoveEdges()) {
      val requestTs = System.currentTimeMillis()
      val edgeToDelete = builder.copyEdge(op = GraphUtil.operations("delete"),
        version = version + S2Edge.incrementVersion, propsWithTs = S2Edge.propsToState(updatePropsWithTs()), ts = requestTs)
      // should we delete related edges also?
      val future = innerGraph.mutateEdges(Seq(edgeToDelete), withWait = true)
      val mutateSuccess = Await.result(future, innerGraph.WaitTimeout)
      if (!mutateSuccess.forall(_.isSuccess)) throw new RuntimeException("edge remove failed.")
    } else {
      throw Edge.Exceptions.edgeRemovalNotSupported()
    }
  }

  def toLogString: String = {
    //    val allPropsWithName = defaultPropsWithName ++ Json.toJson(propsWithName).asOpt[JsObject].getOrElse(Json.obj())
    val propsWithName = PostProcess.s2EdgePropsJsonString(this)

    List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, innerLabel.label, Json.toJson(propsWithName)).mkString("\t")
  }
}
