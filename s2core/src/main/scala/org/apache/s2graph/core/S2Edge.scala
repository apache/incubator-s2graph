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
import java.util.function.{Consumer, BiConsumer}

import org.apache.s2graph.core.S2Edge.{Props, State}
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, LabelMeta}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.structure
import org.apache.tinkerpop.gremlin.structure.{Edge, Graph, Vertex, Direction, Property}
import play.api.libs.json.{JsNumber, JsObject, Json}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}
import scala.util.hashing.MurmurHash3

case class SnapshotEdge(graph: S2Graph,
                        srcVertex: S2Vertex,
                        tgtVertex: S2Vertex,
                        label: Label,
                        dir: Int,
                        op: Byte,
                        version: Long,
                        private val propsWithTs: Props,
                        pendingEdgeOpt: Option[S2Edge],
                        statusCode: Byte = 0,
                        lockTs: Option[Long],
                        tsInnerValOpt: Option[InnerValLike] = None) {
  lazy val direction = GraphUtil.fromDirection(dir)
  lazy val operation = GraphUtil.fromOp(op)
  lazy val edge = toEdge
  lazy val labelWithDir = LabelWithDirection(label.id.get, dir)
//  if (!propsWithTs.contains(LabelMeta.timestamp.name)) throw new Exception("Timestamp is required.")

//  val label = Label.findById(labelWithDir.labelId)
  lazy val schemaVer = label.schemaVersion
  lazy val ts = propsWithTs.get(LabelMeta.timestamp.name).innerVal.toString().toLong

  def propsToKeyValuesWithTs = HBaseSerializable.propsToKeyValuesWithTs(propsWithTs.asScala.map(kv => kv._2.labelMeta.seq -> kv._2.innerValWithTs).toSeq)

  def allPropsDeleted = S2Edge.allPropsDeleted(propsWithTs)

  def toEdge: S2Edge = {
    S2Edge(graph, srcVertex, tgtVertex, label, dir, op,
      version, propsWithTs, pendingEdgeOpt = pendingEdgeOpt,
      statusCode = statusCode, lockTs = lockTs, tsInnerValOpt = tsInnerValOpt)
  }

  def propsWithName = (for {
    (_, v) <- propsWithTs.asScala
    meta = v.labelMeta
    jsValue <- innerValToJsValue(v.innerVal, meta.dataType)
  } yield meta.name -> jsValue) ++ Map("version" -> JsNumber(version))

  // only for debug
  def toLogString() = {
    List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, label.label, propsWithName).mkString("\t")
  }

  def property[V](key: String, value: V, ts: Long): S2Property[V] = {
    val labelMeta = label.metaPropsInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on IndexEdge."))
    val newProps = new S2Property(edge, labelMeta, key, value, ts)
    propsWithTs.put(key, newProps)
    newProps
  }
  override def hashCode(): Int = {
    MurmurHash3.stringHash(srcVertex.innerId + "," + labelWithDir + "," + tgtVertex.innerId)
  }

  override def equals(other: Any): Boolean = other match {
    case e: SnapshotEdge =>
      srcVertex.innerId == e.srcVertex.innerId &&
        tgtVertex.innerId == e.tgtVertex.innerId &&
        labelWithDir == e.labelWithDir && op == e.op && version == e.version &&
        pendingEdgeOpt == e.pendingEdgeOpt && lockTs == lockTs && statusCode == statusCode
    case _ => false
  }

  override def toString(): String = {
    Map("srcVertex" -> srcVertex.toString, "tgtVertex" -> tgtVertex.toString, "label" -> label.label, "direction" -> direction,
      "operation" -> operation, "version" -> version, "props" -> propsWithTs.asScala.map(kv => kv._1 -> kv._2.value).toString,
      "statusCode" -> statusCode, "lockTs" -> lockTs).toString
  }
}

case class IndexEdge(graph: S2Graph,
                     srcVertex: S2Vertex,
                     tgtVertex: S2Vertex,
                     label: Label,
                     dir: Int,
                     op: Byte,
                     version: Long,
                     labelIndexSeq: Byte,
                     private val propsWithTs: Props,
                     tsInnerValOpt: Option[InnerValLike] = None)  {
//  if (!props.contains(LabelMeta.timeStampSeq)) throw new Exception("Timestamp is required.")
  //  assert(props.contains(LabelMeta.timeStampSeq))
  lazy val direction = GraphUtil.fromDirection(dir)
  lazy val operation = GraphUtil.fromOp(op)
  lazy val edge = toEdge
  lazy val labelWithDir = LabelWithDirection(label.id.get, dir)

  lazy val isInEdge = labelWithDir.dir == GraphUtil.directions("in")
  lazy val isOutEdge = !isInEdge

  lazy val ts = propsWithTs.get(LabelMeta.timestamp.name).innerVal.toString.toLong
  lazy val degreeEdge = propsWithTs.containsKey(LabelMeta.degree.name)

  lazy val schemaVer = label.schemaVersion
  lazy val labelIndex = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelIndexSeq).get
  lazy val defaultIndexMetas = labelIndex.sortKeyTypes.map { meta =>
    val innerVal = toInnerVal(meta.defaultValue, meta.dataType, schemaVer)
    meta.seq -> innerVal
  }.toMap

  lazy val labelIndexMetaSeqs = labelIndex.sortKeyTypes

  def indexOption = if (isInEdge) labelIndex.inDirOption else labelIndex.outDirOption

  /** TODO: make sure call of this class fill props as this assumes */
  lazy val orders = for (meta <- labelIndexMetaSeqs) yield {
    propsWithTs.get(meta.name) match {
      case null =>

        /**
          * TODO: agly hack
          * now we double store target vertex.innerId/srcVertex.innerId for easy development. later fix this to only store id once
          */
        val v = meta match {
          case LabelMeta.timestamp=> InnerVal.withLong(version, schemaVer)
          case LabelMeta.to => toEdge.tgtVertex.innerId
          case LabelMeta.from => toEdge.srcVertex.innerId
          case LabelMeta.fromHash => indexOption.map { option =>
            InnerVal.withLong(MurmurHash3.stringHash(toEdge.srcVertex.innerId.toString()).abs % option.totalModular, schemaVer)
          }.getOrElse(throw new RuntimeException("from_hash must be used with sampling"))
          // for now, it does not make sense to build index on srcVertex.innerId since all edges have same data.
          case _ => toInnerVal(meta.defaultValue, meta.dataType, schemaVer)
        }

        meta -> v
      case v => meta -> v.innerVal
    }
  }

  lazy val ordersKeyMap = orders.map { case (meta, _) => meta.name }.toSet
  lazy val metas = for ((meta, v) <- propsWithTs.asScala if !ordersKeyMap.contains(meta)) yield v.labelMeta -> v.innerVal

//  lazy val propsWithTs = props.map { case (k, v) => k -> InnerValLikeWithTs(v, version) }

  //TODO:
  //  lazy val kvs = Graph.client.indexedEdgeSerializer(this).toKeyValues.toList

  lazy val hasAllPropsForIndex = orders.length == labelIndexMetaSeqs.length

  def propsWithName = for {
    (_, v) <- propsWithTs.asScala
    meta = v.labelMeta
    jsValue <- innerValToJsValue(v.innerVal, meta.dataType)
  } yield meta.name -> jsValue


  def toEdge: S2Edge = S2Edge(graph, srcVertex, tgtVertex, label, dir, op, version, propsWithTs, tsInnerValOpt = tsInnerValOpt)

  // only for debug
  def toLogString() = {
    List(version, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, label.label, Json.toJson(propsWithName)).mkString("\t")
  }

  def property(key: String): Option[InnerValLikeWithTs] = {
    label.metaPropsInvMap.get(key).map(labelMeta => property(labelMeta))
  }

  def property(labelMeta: LabelMeta): InnerValLikeWithTs = {
//    propsWithTs.get(labelMeta.name).map(_.innerValWithTs).getOrElse(label.metaPropsDefaultMapInner(labelMeta))
    if (propsWithTs.containsKey(labelMeta.name)) {
      propsWithTs.get(labelMeta.name).innerValWithTs
    } else {
      label.metaPropsDefaultMapInner(labelMeta)
    }
  }

  def updatePropsWithTs(others: Props = S2Edge.EmptyProps): Props = {
    if (others.isEmpty) propsWithTs
    else {
      val iter = others.entrySet().iterator()
      while (iter.hasNext) {
        val e = iter.next()
        propsWithTs.put(e.getKey, e.getValue)
      }
      propsWithTs
    }
  }

  def property[V](key: String, value: V, ts: Long): S2Property[V] = {
    val labelMeta = label.metaPropsInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on IndexEdge."))
    val newProps = new S2Property(edge, labelMeta, key, value, ts)
    propsWithTs.put(key, newProps)
    newProps
  }
  override def hashCode(): Int = {
    MurmurHash3.stringHash(srcVertex.innerId + "," + labelWithDir + "," + tgtVertex.innerId + "," + labelIndexSeq)
  }

  override def equals(other: Any): Boolean = other match {
    case e: IndexEdge =>
      srcVertex.innerId == e.srcVertex.innerId &&
        tgtVertex.innerId == e.tgtVertex.innerId &&
        labelWithDir == e.labelWithDir && op == e.op && version == e.version &&
        labelIndexSeq == e.labelIndexSeq
    case _ => false
  }

  override def toString(): String = {
    Map("srcVertex" -> srcVertex.toString, "tgtVertex" -> tgtVertex.toString, "label" -> label.label, "direction" -> dir,
      "operation" -> operation, "version" -> version, "props" -> propsWithTs.asScala.map(kv => kv._1 -> kv._2.value).toString
    ).toString
  }
}

case class S2Edge(innerGraph: S2Graph,
                srcVertex: S2Vertex,
                var tgtVertex: S2Vertex,
                innerLabel: Label,
                dir: Int,
                var op: Byte = GraphUtil.defaultOpByte,
                var version: Long = System.currentTimeMillis(),
                propsWithTs: Props = S2Edge.EmptyProps,
                parentEdges: Seq[EdgeWithScore] = Nil,
                originalEdgeOpt: Option[S2Edge] = None,
                pendingEdgeOpt: Option[S2Edge] = None,
                statusCode: Byte = 0,
                lockTs: Option[Long] = None,
                var tsInnerValOpt: Option[InnerValLike] = None) extends GraphElement with Edge {

  lazy val labelWithDir = LabelWithDirection(innerLabel.id.get, dir)
  lazy val schemaVer = innerLabel.schemaVersion
  lazy val ts = propsWithTs.get(LabelMeta.timestamp.name).innerVal.value match {
    case b: BigDecimal => b.longValue()
    case l: Long => l
    case i: Int => i.toLong
    case _ => throw new RuntimeException("ts should be in [BigDecimal/Long/Int].")
  }

  lazy val operation = GraphUtil.fromOp(op)
  lazy val tsInnerVal = tsInnerValOpt.get.value
  lazy val srcId = srcVertex.innerIdVal
  lazy val tgtId = tgtVertex.innerIdVal
  lazy val labelName = innerLabel.label
  lazy val direction = GraphUtil.fromDirection(dir)
  
  def toIndexEdge(labelIndexSeq: Byte): IndexEdge = IndexEdge(innerGraph, srcVertex, tgtVertex, innerLabel, dir, op, version, labelIndexSeq, propsWithTs)

  def serializePropsWithTs(): Array[Byte] = HBaseSerializable.propsToKeyValuesWithTs(propsWithTs.asScala.map(kv => kv._2.labelMeta.seq -> kv._2.innerValWithTs).toSeq)

  def updatePropsWithTs(others: Props = S2Edge.EmptyProps): Props = {
    val emptyProp = S2Edge.EmptyProps

    propsWithTs.forEach(new BiConsumer[String, S2Property[_]] {
      override def accept(key: String, value: S2Property[_]): Unit = emptyProp.put(key, value)
    })

    others.forEach(new BiConsumer[String, S2Property[_]] {
      override def accept(key: String, value: S2Property[_]): Unit = emptyProp.put(key, value)
    })

    emptyProp
  }

  def propertyValue(key: String): Option[InnerValLikeWithTs] = {
    key match {
      case "from" | "_from" => Option(InnerValLikeWithTs(srcVertex.innerId, ts))
      case "to" | "_to" => Option(InnerValLikeWithTs(tgtVertex.innerId, ts))
      case "label" => Option(InnerValLikeWithTs(InnerVal.withStr(innerLabel.label, schemaVer), ts))
      case "direction" => Option(InnerValLikeWithTs(InnerVal.withStr(direction, schemaVer), ts))
      case _ =>
        innerLabel.metaPropsInvMap.get(key).map(labelMeta => propertyValueInner(labelMeta))
    }
  }

  def propertyValueInner(labelMeta: LabelMeta): InnerValLikeWithTs= {
    //    propsWithTs.get(labelMeta.name).map(_.innerValWithTs).getOrElse()
    if (propsWithTs.containsKey(labelMeta.name)) {
      propsWithTs.get(labelMeta.name).innerValWithTs
    } else {
      innerLabel.metaPropsDefaultMapInner(labelMeta)
    }
  }

  def propertyValues(keys: Seq[String] = Nil): Map[LabelMeta, InnerValLikeWithTs] = {
    val labelMetas = for {
        key <- keys
        labelMeta <- innerLabel.metaPropsInvMap.get(key)
      } yield labelMeta

    propertyValuesInner(labelMetas)
  }

  def propertyValuesInner(labelMetas: Seq[LabelMeta] = Nil): Map[LabelMeta, InnerValLikeWithTs] = {
    if (labelMetas.isEmpty) {
      innerLabel.metaPropsDefaultMapInner.map { case (labelMeta, defaultVal) =>
        labelMeta -> propertyValueInner(labelMeta)
      }
    } else {
      // This is important since timestamp is required for all edges.
      (LabelMeta.timestamp +: labelMetas).map { labelMeta =>
        labelMeta -> propertyValueInner(labelMeta)
      }.toMap
    }
  }

//  if (!props.contains(LabelMeta.timestamp)) throw new Exception("Timestamp is required.")
  //  assert(propsWithTs.contains(LabelMeta.timeStampSeq))

  lazy val properties = toProps()

  def props = propsWithTs.asScala.mapValues(_.innerVal)


  private def toProps(): Map[String, Any] = {
    for {
      (labelMeta, defaultVal) <- innerLabel.metaPropsDefaultMapInner
    } yield {
      //      labelMeta.name -> propsWithTs.get(labelMeta.name).map(_.innerValWithTs).getOrElse(defaultVal).innerVal.value
      val value =
        if (propsWithTs.containsKey(labelMeta.name)) {
          propsWithTs.get(labelMeta.name).value
        } else {
          defaultVal.innerVal.value
        }
      labelMeta.name -> value
    }
  }

  def relatedEdges = {
    if (labelWithDir.isDirected) {
      val skipReverse = innerLabel.extraOptions.get("skipReverse").map(_.as[Boolean]).getOrElse(false)
      if (skipReverse) List(this) else List(this, duplicateEdge)
    } else {
//      val outDir = labelWithDir.copy(dir = GraphUtil.directions("out"))
//      val base = copy(labelWithDir = outDir)
      val base = copy(dir = GraphUtil.directions("out"))
      List(base, base.reverseSrcTgtEdge)
    }
  }

  //    def relatedEdges = List(this)

  def srcForVertex = {
    val belongLabelIds = Seq(labelWithDir.labelId)
    if (labelWithDir.dir == GraphUtil.directions("in")) {
      innerGraph.newVertex(VertexId(innerLabel.tgtColumn, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props, belongLabelIds = belongLabelIds)
    } else {
      innerGraph.newVertex(VertexId(innerLabel.srcColumn, srcVertex.innerId), srcVertex.ts, srcVertex.props, belongLabelIds = belongLabelIds)
    }
  }

  def tgtForVertex = {
    val belongLabelIds = Seq(labelWithDir.labelId)
    if (labelWithDir.dir == GraphUtil.directions("in")) {
      innerGraph.newVertex(VertexId(innerLabel.srcColumn, srcVertex.innerId), srcVertex.ts, srcVertex.props, belongLabelIds = belongLabelIds)
    } else {
      innerGraph.newVertex(VertexId(innerLabel.tgtColumn, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props, belongLabelIds = belongLabelIds)
    }
  }

  def duplicateEdge = reverseSrcTgtEdge.reverseDirEdge

//  def reverseDirEdge = copy(labelWithDir = labelWithDir.dirToggled)
  def reverseDirEdge = copy(dir = GraphUtil.toggleDir(dir))

  def reverseSrcTgtEdge = copy(srcVertex = tgtVertex, tgtVertex = srcVertex)

  def labelOrders = LabelIndex.findByLabelIdAll(labelWithDir.labelId)

  override def serviceName = innerLabel.serviceName

  override def queueKey = Seq(ts.toString, tgtVertex.serviceName).mkString("|")

  override def queuePartitionKey = Seq(srcVertex.innerId, tgtVertex.innerId).mkString("|")

  override def isAsync = innerLabel.isAsync

  def isDegree = propsWithTs.containsKey(LabelMeta.degree.name)

//  def propsPlusTs = propsWithTs.get(LabelMeta.timeStampSeq) match {
//    case Some(_) => props
//    case None => props ++ Map(LabelMeta.timeStampSeq -> InnerVal.withLong(ts, schemaVer))
//  }

  def propsPlusTsValid = propsWithTs.asScala.filter(kv => LabelMeta.isValidSeq(kv._2.labelMeta.seq)).asJava

  def edgesWithIndex = for (labelOrder <- labelOrders) yield {
    IndexEdge(innerGraph, srcVertex, tgtVertex, innerLabel, dir, op, version, labelOrder.seq, propsWithTs, tsInnerValOpt = tsInnerValOpt)
  }

  def edgesWithIndexValid = for (labelOrder <- labelOrders) yield {
    IndexEdge(innerGraph, srcVertex, tgtVertex, innerLabel, dir, op, version, labelOrder.seq, propsPlusTsValid, tsInnerValOpt = tsInnerValOpt)
  }

  /** force direction as out on invertedEdge */
  def toSnapshotEdge: SnapshotEdge = {
    val (smaller, larger) = (srcForVertex, tgtForVertex)

//    val newLabelWithDir = LabelWithDirection(labelWithDir.labelId, GraphUtil.directions("out"))

    property(LabelMeta.timestamp.name, ts, ts)
    val ret = SnapshotEdge(innerGraph, smaller, larger, innerLabel,
      GraphUtil.directions("out"), op, version, propsWithTs,
      pendingEdgeOpt = pendingEdgeOpt, statusCode = statusCode, lockTs = lockTs, tsInnerValOpt = tsInnerValOpt)
    ret
  }

  def defaultPropsWithName = Json.obj("from" -> srcVertex.innerId.toString(), "to" -> tgtVertex.innerId.toString(),
    "label" -> innerLabel.label, "service" -> innerLabel.serviceName)

  def propsWithName =
    for {
      (_, v) <- propsWithTs.asScala
      meta = v.labelMeta
      jsValue <- innerValToJsValue(v.innerVal, meta.dataType)
    } yield meta.name -> jsValue


  def updateTgtVertex(id: InnerValLike) = {
    val newId = TargetVertexId(tgtVertex.id.column, id)
    val newTgtVertex = innerGraph.newVertex(newId, tgtVertex.ts, tgtVertex.props)
    S2Edge(innerGraph, srcVertex, newTgtVertex, innerLabel, dir, op, version, propsWithTs, tsInnerValOpt = tsInnerValOpt)
  }

  def rank(r: RankParam): Double =
    if (r.keySeqAndWeights.size <= 0) 1.0f
    else {
      var sum: Double = 0

      for ((labelMeta, w) <- r.keySeqAndWeights) {
        if (propsWithTs.containsKey(labelMeta.name)) {
          val innerValWithTs = propsWithTs.get(labelMeta.name)
          val cost = try innerValWithTs.innerVal.toString.toDouble catch {
            case e: Exception =>
              logger.error("toInnerval failed in rank", e)
              1.0
          }
          sum += w * cost
        }
      }
      sum
    }

  def toLogString: String = {
    val allPropsWithName = defaultPropsWithName ++ Json.toJson(propsWithName).asOpt[JsObject].getOrElse(Json.obj())
    List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, innerLabel.label, allPropsWithName).mkString("\t")
  }

  override def hashCode(): Int = {
    MurmurHash3.stringHash(srcVertex.innerId + "," + labelWithDir + "," + tgtVertex.innerId)
  }

  override def equals(other: Any): Boolean = other match {
    case e: S2Edge =>
      srcVertex.innerId == e.srcVertex.innerId &&
        tgtVertex.innerId == e.tgtVertex.innerId &&
        labelWithDir == e.labelWithDir && S2Edge.sameProps(propsWithTs, e.propsWithTs) &&
        op == e.op && version == e.version &&
        pendingEdgeOpt == e.pendingEdgeOpt && lockTs == lockTs && statusCode == statusCode &&
        parentEdges == e.parentEdges && originalEdgeOpt == originalEdgeOpt
    case _ => false
  }

  override def toString(): String = {
    Map("srcVertex" -> srcVertex.toString, "tgtVertex" -> tgtVertex.toString, "label" -> labelName, "direction" -> direction,
      "operation" -> operation, "version" -> version, "props" -> propsWithTs.asScala.map(kv => kv._1 -> kv._2.value).toString,
      "parentEdges" -> parentEdges, "originalEdge" -> originalEdgeOpt, "statusCode" -> statusCode, "lockTs" -> lockTs
    ).toString
  }

  def checkProperty(key: String): Boolean = propsWithTs.containsKey(key)

  def copyEdge(srcVertex: S2Vertex = srcVertex,
               tgtVertex: S2Vertex = tgtVertex,
               innerLabel: Label = innerLabel,
               dir: Int = dir,
               op: Byte = op,
               version: Long = version,
               propsWithTs: State = S2Edge.propsToState(this.propsWithTs),
               parentEdges: Seq[EdgeWithScore] = parentEdges,
               originalEdgeOpt: Option[S2Edge] = originalEdgeOpt,
               pendingEdgeOpt: Option[S2Edge] = pendingEdgeOpt,
               statusCode: Byte = statusCode,
               lockTs: Option[Long] = lockTs,
               tsInnerValOpt: Option[InnerValLike] = tsInnerValOpt,
               ts: Long = ts): S2Edge = {
    val edge = new S2Edge(innerGraph, srcVertex, tgtVertex, innerLabel, dir, op, version, S2Edge.EmptyProps,
      parentEdges, originalEdgeOpt, pendingEdgeOpt, statusCode, lockTs, tsInnerValOpt)
    S2Edge.fillPropsWithTs(edge, propsWithTs)
    edge.property(LabelMeta.timestamp.name, ts, ts)
    edge
  }

  def copyEdgeWithState(state: State, ts: Long): S2Edge = {
    val newEdge = copy(propsWithTs = S2Edge.EmptyProps)
    S2Edge.fillPropsWithTs(newEdge, state)
    newEdge.property(LabelMeta.timestamp.name, ts, ts)
    newEdge
  }

  def copyEdgeWithState(state: State): S2Edge = {
    val newEdge = copy(propsWithTs = S2Edge.EmptyProps)
    S2Edge.fillPropsWithTs(newEdge, state)
    newEdge
  }

  override def vertices(direction: Direction): util.Iterator[structure.Vertex] = {
    val arr = new util.ArrayList[Vertex]()
    direction match {
      case Direction.OUT => arr.add(srcVertex)
      case Direction.IN => arr.add(tgtVertex)
      case _ =>
        arr.add(srcVertex)
        arr.add(tgtVertex)
    }
    arr.iterator()
  }

  override def properties[V](keys: String*): util.Iterator[Property[V]] = {
    val ls = new util.ArrayList[Property[V]]()
    keys.foreach { key => ls.add(property(key)) }
    ls.iterator()
  }

  override def property[V](key: String): Property[V] = {
    val labelMeta = innerLabel.metaPropsInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on Edge."))
    if (propsWithTs.containsKey(key)) propsWithTs.get(key).asInstanceOf[Property[V]]
    else {
      val default = innerLabel.metaPropsDefaultMapInner(labelMeta)
      property(key, default.innerVal.value, default.ts).asInstanceOf[Property[V]]
    }
  }

  override def property[V](key: String, value: V): Property[V] = {
    property(key, value, System.currentTimeMillis())
  }

  def property[V](key: String, value: V, ts: Long): Property[V] = {
    val labelMeta = innerLabel.metaPropsInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on Edge."))
    val newProp = new S2Property[V](this, labelMeta, key, value, ts)
    propsWithTs.put(key, newProp)
    newProp
  }

  override def remove(): Unit = {}

  override def graph(): Graph = innerGraph

  override def id(): AnyRef = (srcVertex.innerId, labelWithDir, tgtVertex.innerId)

  override def label(): String = innerLabel.label
}


object EdgeMutate {
  def filterIndexOptionForDegree(edges: Seq[IndexEdge]): Seq[IndexEdge] = edges.filter { ie =>
    ie.indexOption.fold(true)(_.storeDegree)
  }

  def filterIndexOption(edges: Seq[IndexEdge]): Seq[IndexEdge] = edges.filter { ie =>
    ie.indexOption.fold(true) { option =>
      val hashValueOpt = ie.orders.find { case (k, v) => k == LabelMeta.fromHash }.map { case (k, v) =>
        v.value.toString.toLong
      }

      option.sample(ie, hashValueOpt)
    }
  }
}

case class EdgeMutate(edgesToDelete: List[IndexEdge] = List.empty[IndexEdge],
                      edgesToInsert: List[IndexEdge] = List.empty[IndexEdge],
                      newSnapshotEdge: Option[SnapshotEdge] = None) {

  val edgesToInsertWithIndexOpt: Seq[IndexEdge] = EdgeMutate.filterIndexOption(edgesToInsert)

  val edgesToDeleteWithIndexOpt: Seq[IndexEdge] = EdgeMutate.filterIndexOption(edgesToDelete)

  val edgesToInsertWithIndexOptForDegree: Seq[IndexEdge] = EdgeMutate.filterIndexOptionForDegree(edgesToInsert)

  val edgesToDeleteWithIndexOptForDegree: Seq[IndexEdge] = EdgeMutate.filterIndexOptionForDegree(edgesToDelete)

  def toLogString: String = {
    val l = (0 until 50).map(_ => "-").mkString("")
    val deletes = s"deletes: ${edgesToDelete.map(e => e.toLogString).mkString("\n")}"
    val inserts = s"inserts: ${edgesToInsert.map(e => e.toLogString).mkString("\n")}"
    val updates = s"snapshot: ${newSnapshotEdge.map(e => e.toLogString).mkString("\n")}"

    List("\n", l, deletes, inserts, updates, l, "\n").mkString("\n")
  }
}

object S2Edge {
  val incrementVersion = 1L
  val minTsVal = 0L

  /** now version information is required also **/
  type Props = java.util.Map[String, S2Property[_]]
  type State = Map[LabelMeta, InnerValLikeWithTs]
  type PropsPairWithTs = (State, State, Long, String)
  type MergeState = PropsPairWithTs => (State, Boolean)
  type UpdateFunc = (Option[S2Edge], S2Edge, MergeState)

  def EmptyProps = new java.util.HashMap[String, S2Property[_]]
  def EmptyState = Map.empty[LabelMeta, InnerValLikeWithTs]
  def sameProps(base: Props, other: Props): Boolean = {
    if (base.size != other.size) false
    else {
      var ret = true
      val iter = base.entrySet().iterator()
      while (iter.hasNext) {
        val e = iter.next()
        if (!other.containsKey(e.getKey)) ret = false
        else if (e.getValue != other.get(e.getKey)) ret = false
        else {

        }
      }
      val otherIter = other.entrySet().iterator()
      while (otherIter.hasNext) {
        val e = otherIter.next()
        if (!base.containsKey(e.getKey)) ret = false
        else if (e.getValue != base.get(e.getKey)) ret = false
        else {

        }
      }
      ret
    }
  }
  def fillPropsWithTs(snapshotEdge: SnapshotEdge, state: State): Unit = {
    state.foreach { case (k, v) => snapshotEdge.property(k.name, v.innerVal.value, v.ts) }
  }
  def fillPropsWithTs(indexEdge: IndexEdge, state: State): Unit = {
    state.foreach { case (k, v) => indexEdge.property(k.name, v.innerVal.value, v.ts) }
  }
  def fillPropsWithTs(edge: S2Edge, state: State): Unit = {
    state.foreach { case (k, v) => edge.property(k.name, v.innerVal.value, v.ts) }
  }

  def propsToState(props: Props): State = {
    props.asScala.map { case (k, v) =>
      v.labelMeta -> v.innerValWithTs
    }.toMap
  }

  def stateToProps(edge: S2Edge, state: State): Props = {
    state.foreach { case (k, v) =>
      edge.property(k.name, v.innerVal.value, v.ts)
    }
    edge.propsWithTs
  }

  def allPropsDeleted(props: Map[LabelMeta, InnerValLikeWithTs]): Boolean =
    if (!props.contains(LabelMeta.lastDeletedAt)) false
    else {
      val lastDeletedAt = props.get(LabelMeta.lastDeletedAt).get.ts
      val propsWithoutLastDeletedAt = props - LabelMeta.lastDeletedAt

      propsWithoutLastDeletedAt.forall { case (_, v) => v.ts <= lastDeletedAt }
    }

  def allPropsDeleted(props: Props): Boolean =
    if (!props.containsKey(LabelMeta.lastDeletedAt.name)) false
    else {
      val lastDeletedAt = props.get(LabelMeta.lastDeletedAt.name).ts
      props.remove(LabelMeta.lastDeletedAt.name)
//      val propsWithoutLastDeletedAt = props
//
//      propsWithoutLastDeletedAt.forall { case (_, v) => v.ts <= lastDeletedAt }
      var ret = true
      val iter = props.entrySet().iterator()
      while (iter.hasNext && ret) {
        val e = iter.next()
        if (e.getValue.ts > lastDeletedAt) ret = false
      }
      ret
    }

  def buildDeleteBulk(invertedEdge: Option[S2Edge], requestEdge: S2Edge): (S2Edge, EdgeMutate) = {
    //    assert(invertedEdge.isEmpty)
    //    assert(requestEdge.op == GraphUtil.operations("delete"))

    val edgesToDelete = requestEdge.relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
    val edgeInverted = Option(requestEdge.toSnapshotEdge)

    (requestEdge, EdgeMutate(edgesToDelete, edgesToInsert = Nil, newSnapshotEdge = edgeInverted))
  }

  def buildOperation(invertedEdge: Option[S2Edge], requestEdges: Seq[S2Edge]): (S2Edge, EdgeMutate) = {
    //            logger.debug(s"oldEdge: ${invertedEdge.map(_.toStringRaw)}")
    //            logger.debug(s"requestEdge: ${requestEdge.toStringRaw}")
    val oldPropsWithTs =
      if (invertedEdge.isEmpty) Map.empty[LabelMeta, InnerValLikeWithTs]
      else propsToState(invertedEdge.get.propsWithTs)

    val funcs = requestEdges.map { edge =>
      if (edge.op == GraphUtil.operations("insert")) {
        edge.innerLabel.consistencyLevel match {
          case "strong" => S2Edge.mergeUpsert _
          case _ => S2Edge.mergeInsertBulk _
        }
      } else if (edge.op == GraphUtil.operations("insertBulk")) {
        S2Edge.mergeInsertBulk _
      } else if (edge.op == GraphUtil.operations("delete")) {
        edge.innerLabel.consistencyLevel match {
          case "strong" => S2Edge.mergeDelete _
          case _ => throw new RuntimeException("not supported")
        }
      }
      else if (edge.op == GraphUtil.operations("update")) S2Edge.mergeUpdate _
      else if (edge.op == GraphUtil.operations("increment")) S2Edge.mergeIncrement _
      else throw new RuntimeException(s"not supported operation on edge: $edge")
    }

    val oldTs = invertedEdge.map(_.ts).getOrElse(minTsVal)
    val requestWithFuncs = requestEdges.zip(funcs).filter(oldTs != _._1.ts).sortBy(_._1.ts)

    if (requestWithFuncs.isEmpty) {
      (requestEdges.head, EdgeMutate())
    } else {
      val requestEdge = requestWithFuncs.last._1
      var prevPropsWithTs = oldPropsWithTs

      for {
        (requestEdge, func) <- requestWithFuncs
      } {
        val (_newPropsWithTs, _) = func(prevPropsWithTs, propsToState(requestEdge.propsWithTs), requestEdge.ts, requestEdge.schemaVer)
        prevPropsWithTs = _newPropsWithTs
        //        logger.debug(s"${requestEdge.toLogString}\n$oldPropsWithTs\n$prevPropsWithTs\n")
      }
      val requestTs = requestEdge.ts
      /** version should be monotoniously increasing so our RPC mutation should be applied safely */
      val newVersion = invertedEdge.map(e => e.version + incrementVersion).getOrElse(requestTs)
      val maxTs = prevPropsWithTs.map(_._2.ts).max
      val newTs = if (maxTs > requestTs) maxTs else requestTs
      val propsWithTs = prevPropsWithTs ++
        Map(LabelMeta.timestamp -> InnerValLikeWithTs(InnerVal.withLong(newTs, requestEdge.innerLabel.schemaVersion), newTs))

      val edgeMutate = buildMutation(invertedEdge, requestEdge, newVersion, oldPropsWithTs, propsWithTs)

      //      logger.debug(s"${edgeMutate.toLogString}\n${propsWithTs}")
      //      logger.error(s"$propsWithTs")
      val newEdge = requestEdge.copy(propsWithTs = EmptyProps)
      fillPropsWithTs(newEdge, propsWithTs)
      (newEdge, edgeMutate)
    }
  }

  def buildMutation(snapshotEdgeOpt: Option[S2Edge],
                    requestEdge: S2Edge,
                    newVersion: Long,
                    oldPropsWithTs: Map[LabelMeta, InnerValLikeWithTs],
                    newPropsWithTs: Map[LabelMeta, InnerValLikeWithTs]): EdgeMutate = {

    if (oldPropsWithTs == newPropsWithTs) {
      // all requests should be dropped. so empty mutation.
      EdgeMutate(edgesToDelete = Nil, edgesToInsert = Nil, newSnapshotEdge = None)
    } else {
      val withOutDeletedAt = newPropsWithTs.filter(kv => kv._1 != LabelMeta.lastDeletedAtSeq)
      val newOp = snapshotEdgeOpt match {
        case None => requestEdge.op
        case Some(old) =>
          val oldMaxTs = old.propsWithTs.asScala.map(_._2.ts).max
          if (oldMaxTs > requestEdge.ts) old.op
          else requestEdge.op
      }

      val newSnapshotEdge = requestEdge.copy(op = newOp, version = newVersion).copyEdgeWithState(newPropsWithTs)

      val newSnapshotEdgeOpt = Option(newSnapshotEdge.toSnapshotEdge)
      // delete request must always update snapshot.
      if (withOutDeletedAt == oldPropsWithTs && newPropsWithTs.contains(LabelMeta.lastDeletedAt)) {
        // no mutation on indexEdges. only snapshotEdge should be updated to record lastDeletedAt.
        EdgeMutate(edgesToDelete = Nil, edgesToInsert = Nil, newSnapshotEdge = newSnapshotEdgeOpt)
      } else {
        val edgesToDelete = snapshotEdgeOpt match {
          case Some(snapshotEdge) if snapshotEdge.op != GraphUtil.operations("delete") =>
            snapshotEdge.copy(op = GraphUtil.defaultOpByte)
              .relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
          case _ => Nil
        }

        val edgesToInsert =
          if (newPropsWithTs.isEmpty || allPropsDeleted(newPropsWithTs)) Nil
          else {
            val newEdge = requestEdge.copy(
              version = newVersion,
              propsWithTs = S2Edge.EmptyProps,
              op = GraphUtil.defaultOpByte
            )
            newPropsWithTs.foreach { case (k, v) => newEdge.property(k.name, v.innerVal.value, v.ts) }

            newEdge.relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
          }


        EdgeMutate(edgesToDelete = edgesToDelete,
          edgesToInsert = edgesToInsert,
          newSnapshotEdge = newSnapshotEdgeOpt)
      }
    }
  }

  def mergeUpsert(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs, version) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt).map(v => v.ts).getOrElse(minTsVal)
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      propsWithTs.get(k) match {
        case Some(newValWithTs) =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          val v = if (oldValWithTs.ts >= newValWithTs.ts) oldValWithTs
          else {
            shouldReplace = true
            newValWithTs
          }
          Some(k -> v)

        case None =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          if (oldValWithTs.ts >= requestTs || k.seq < 0) Some(k -> oldValWithTs)
          else {
            shouldReplace = true
            None
          }
      }
    }
    val existInNew =
      for {
        (k, newValWithTs) <- propsWithTs if !oldPropsWithTs.contains(k) && newValWithTs.ts > lastDeletedAt
      } yield {
        shouldReplace = true
        Some(k -> newValWithTs)
      }

    ((existInOld.flatten ++ existInNew.flatten).toMap, shouldReplace)
  }

  def mergeUpdate(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs, version) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt).map(v => v.ts).getOrElse(minTsVal)
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      propsWithTs.get(k) match {
        case Some(newValWithTs) =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          val v = if (oldValWithTs.ts >= newValWithTs.ts) oldValWithTs
          else {
            shouldReplace = true
            newValWithTs
          }
          Some(k -> v)
        case None =>
          // important: update need to merge previous valid values.
          assert(oldValWithTs.ts >= lastDeletedAt)
          Some(k -> oldValWithTs)
      }
    }
    val existInNew = for {
      (k, newValWithTs) <- propsWithTs if !oldPropsWithTs.contains(k) && newValWithTs.ts > lastDeletedAt
    } yield {
      shouldReplace = true
      Some(k -> newValWithTs)
    }

    ((existInOld.flatten ++ existInNew.flatten).toMap, shouldReplace)
  }

  def mergeIncrement(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs, version) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt).map(v => v.ts).getOrElse(minTsVal)
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      propsWithTs.get(k) match {
        case Some(newValWithTs) =>
          if (k == LabelMeta.timestamp) {
            val v = if (oldValWithTs.ts >= newValWithTs.ts) oldValWithTs
            else {
              shouldReplace = true
              newValWithTs
            }
            Some(k -> v)
          } else {
            if (oldValWithTs.ts >= newValWithTs.ts) {
              Some(k -> oldValWithTs)
            } else {
              assert(oldValWithTs.ts < newValWithTs.ts && oldValWithTs.ts >= lastDeletedAt)
              shouldReplace = true
              // incr(t0), incr(t2), d(t1) => deleted
              Some(k -> InnerValLikeWithTs(oldValWithTs.innerVal + newValWithTs.innerVal, oldValWithTs.ts))
            }
          }

        case None =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          Some(k -> oldValWithTs)
        //          if (oldValWithTs.ts >= lastDeletedAt) Some(k -> oldValWithTs) else None
      }
    }
    val existInNew = for {
      (k, newValWithTs) <- propsWithTs if !oldPropsWithTs.contains(k) && newValWithTs.ts > lastDeletedAt
    } yield {
      shouldReplace = true
      Some(k -> newValWithTs)
    }

    ((existInOld.flatten ++ existInNew.flatten).toMap, shouldReplace)
  }

  def mergeDelete(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs, version) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt) match {
      case Some(prevDeletedAt) =>
        if (prevDeletedAt.ts >= requestTs) prevDeletedAt.ts
        else {
          shouldReplace = true
          requestTs
        }
      case None => {
        shouldReplace = true
        requestTs
      }
    }
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      if (k == LabelMeta.timestamp) {
        if (oldValWithTs.ts >= requestTs) Some(k -> oldValWithTs)
        else {
          shouldReplace = true
          Some(k -> InnerValLikeWithTs.withLong(requestTs, requestTs, version))
        }
      } else {
        if (oldValWithTs.ts >= lastDeletedAt) Some(k -> oldValWithTs)
        else {
          shouldReplace = true
          None
        }
      }
    }
    val mustExistInNew = Map(LabelMeta.lastDeletedAt -> InnerValLikeWithTs.withLong(lastDeletedAt, lastDeletedAt, version))
    ((existInOld.flatten ++ mustExistInNew).toMap, shouldReplace)
  }

  def mergeInsertBulk(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    val (_, propsWithTs, _, _) = propsPairWithTs
    (propsWithTs, true)
  }

//  def fromString(s: String): Option[Edge] = Graph.toEdge(s)


}
