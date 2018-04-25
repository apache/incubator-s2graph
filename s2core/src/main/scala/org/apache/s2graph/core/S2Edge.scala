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


import org.apache.s2graph.core.S2Edge.{Props, State}
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.schema.{Label, LabelIndex, LabelMeta, ServiceColumn}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.io.Conversions._
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, Graph, Property, T, Vertex}
import play.api.libs.json.{JsNumber, JsObject, Json}

import scala.collection.JavaConverters._
import scala.util.hashing.MurmurHash3

object SnapshotEdge {
  def apply(e: S2EdgeLike): SnapshotEdge = {
    val (smaller, larger) = (srcForVertexInner(e), tgtForVertexInner(e))

    val snapshotEdge = SnapshotEdge(e.innerGraph, smaller, larger, e.innerLabel,
      GraphUtil.directions("out"), e.getOp(), e.getVersion(), e.getPropsWithTs(),
      pendingEdgeOpt = e.getPendingEdgeOpt(), statusCode = e.getStatusCode(), lockTs = e.getLockTs(), tsInnerValOpt = e.getTsInnerValOpt())

    snapshotEdge.property(LabelMeta.timestamp.name, e.ts, e.ts)

    snapshotEdge
  }

  def copyFrom(e: SnapshotEdge): SnapshotEdge = {
    val copy =
      SnapshotEdge(
        e.graph,
        e.srcVertex,
        e.tgtVertex,
        e.label,
        e.dir,
        e.op,
        e.version,
        S2Edge.EmptyProps,
        e.pendingEdgeOpt,
        e.statusCode,
        e.lockTs,
        e.tsInnerValOpt)

    copy.updatePropsWithTs(e.propsWithTs)

    copy
  }

  private def srcForVertexInner(e: S2EdgeLike): S2VertexLike = {
    val belongLabelIds = Seq(e.getLabelId())
    if (e.getDir() == GraphUtil.directions("in")) {
      val tgtColumn = S2Edge.getServiceColumn(e.tgtVertex, e.innerLabel.tgtColumn)
      e.innerGraph.elementBuilder.newVertex(VertexId(tgtColumn, e.tgtVertex.innerId), e.tgtVertex.ts, e.tgtVertex.props, belongLabelIds = belongLabelIds)
    } else {
      val srcColumn = S2Edge.getServiceColumn(e.srcVertex, e.innerLabel.srcColumn)
      e.innerGraph.elementBuilder.newVertex(VertexId(srcColumn, e.srcVertex.innerId), e.srcVertex.ts, e.srcVertex.props, belongLabelIds = belongLabelIds)
    }
  }

  private def tgtForVertexInner(e: S2EdgeLike): S2VertexLike = {
    val belongLabelIds = Seq(e.getLabelId())
    if (e.getDir() == GraphUtil.directions("in")) {
      val srcColumn = S2Edge.getServiceColumn(e.srcVertex, e.innerLabel.srcColumn)
      e.innerGraph.elementBuilder.newVertex(VertexId(srcColumn, e.srcVertex.innerId), e.srcVertex.ts, e.srcVertex.props, belongLabelIds = belongLabelIds)
    } else {
      val tgtColumn = S2Edge.getServiceColumn(e.tgtVertex, e.innerLabel.tgtColumn)
      e.innerGraph.elementBuilder.newVertex(VertexId(tgtColumn, e.tgtVertex.innerId), e.tgtVertex.ts, e.tgtVertex.props, belongLabelIds = belongLabelIds)
    }
  }

}

case class SnapshotEdge(graph: S2GraphLike,
                        srcVertex: S2VertexLike,
                        tgtVertex: S2VertexLike,
                        label: Label,
                        dir: Int,
                        op: Byte,
                        version: Long,
                        private val propsWithTs: Props,
                        pendingEdgeOpt: Option[S2EdgeLike],
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

  def toEdge: S2EdgeLike = {
    val e = S2Edge(graph, srcVertex, tgtVertex, label, dir, op,
      version, propsWithTs, pendingEdgeOpt = pendingEdgeOpt,
      statusCode = statusCode, lockTs = lockTs, tsInnerValOpt = tsInnerValOpt)

    S2Edge(graph, e.srcForVertex, e.tgtForVertex, label, dir, op,
      version, propsWithTs, pendingEdgeOpt = pendingEdgeOpt,
      statusCode = statusCode, lockTs = lockTs, tsInnerValOpt = tsInnerValOpt)
  }

  def propsWithName = (for {
    (_, v) <- propsWithTs.asScala
    meta = v.labelMeta
    jsValue <- innerValToJsValue(v.innerVal, meta.dataType)
  } yield meta.name -> jsValue) ++ Map("version" -> JsNumber(version))

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

  // only for debug
  def toLogString() = {
    List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, label.label, propsWithName).mkString("\t")
  }

  def property[V](key: String, value: V, ts: Long): S2Property[V] = {
    S2Property.assertValidProp(key, value)

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
      "statusCode" -> statusCode, "lockTs" -> lockTs).toString()
  }
}

object IndexEdge {
  def copyFrom(e: IndexEdge): IndexEdge = {
    val copy = IndexEdge(
      e.graph,
      e.srcVertex,
      e.tgtVertex,
      e.label,
      e.dir,
      e.op,
      e.version,
      e.labelIndexSeq,
      S2Edge.EmptyProps,
      e.tsInnerValOpt
    )

    copy.updatePropsWithTs(e.propsWithTs)

    copy
  }
}

case class IndexEdge(graph: S2GraphLike,
                     srcVertex: S2VertexLike,
                     tgtVertex: S2VertexLike,
                     label: Label,
                     dir: Int,
                     op: Byte,
                     version: Long,
                     labelIndexSeq: Byte,
                     private val propsWithTs: Props,
                     tsInnerValOpt: Option[InnerValLike] = None) {
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

        /*
          * TODO: agly hack
          * now we double store target vertex.innerId/srcVertex.innerId for easy development. later fix this to only store id once
          */
        val v = meta match {
          case LabelMeta.timestamp => InnerVal.withLong(version, schemaVer)
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


  def toEdge: S2EdgeLike = S2Edge(graph, srcVertex, tgtVertex, label, dir, op, version, propsWithTs, tsInnerValOpt = tsInnerValOpt)

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
    ).toString()
  }
}

case class S2Edge(override val innerGraph: S2GraphLike,
                  override val srcVertex: S2VertexLike,
                  override var tgtVertex: S2VertexLike,
                  override val innerLabel: Label,
                  override val dir: Int,
                  var op: Byte = GraphUtil.defaultOpByte,
                  var version: Long = System.currentTimeMillis(),
                  override val propsWithTs: Props = S2Edge.EmptyProps,
                  override val parentEdges: Seq[EdgeWithScore] = Nil,
                  override val originalEdgeOpt: Option[S2EdgeLike] = None,
                  override val pendingEdgeOpt: Option[S2EdgeLike] = None,
                  override val statusCode: Byte = 0,
                  override val lockTs: Option[Long] = None,
                  var tsInnerValOpt: Option[InnerValLike] = None) extends S2EdgeLike {

  //  if (!props.contains(LabelMeta.timestamp)) throw new Exception("Timestamp is required.")
  //  assert(propsWithTs.contains(LabelMeta.timeStampSeq))


  override def serviceName = innerLabel.serviceName

  override def queueKey = Seq(ts.toString, tgtVertex.serviceName).mkString("|")

  override def queuePartitionKey = Seq(srcVertex.innerId, tgtVertex.innerId).mkString("|")

  override def isAsync = innerLabel.isAsync

  //  def propsPlusTs = propsWithTs.get(LabelMeta.timeStampSeq) match {
  //    case Some(_) => props
  //    case None => props ++ Map(LabelMeta.timeStampSeq -> InnerVal.withLong(ts, schemaVer))
  //  }


  //  def toLogString: String = {
  //    //    val allPropsWithName = defaultPropsWithName ++ Json.toJson(propsWithName).asOpt[JsObject].getOrElse(Json.obj())
  //    List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, innerLabel.label, propsWithTs).mkString("\t")
  //  }

  override def hashCode(): Int = {
    id().hashCode()
  }

  override def equals(other: Any): Boolean = other match {
    case e: Edge => id().equals(e.id())
    case _ => false
  }

  //  override def toString(): String = {
  //    Map("srcVertex" -> srcVertex.toString, "tgtVertex" -> tgtVertex.toString, "label" -> labelName, "direction" -> direction,
  //      "operation" -> operation, "version" -> version, "props" -> propsWithTs.asScala.map(kv => kv._1 -> kv._2.value).toString,
  //      "parentEdges" -> parentEdges, "originalEdge" -> originalEdgeOpt, "statusCode" -> statusCode, "lockTs" -> lockTs
  //    ).toString
  //  }

  override def toString: String = {
    // E + L_BRACKET + edge.id() + R_BRACKET + L_BRACKET + edge.outVertex().id() + DASH + edge.label() + ARROW + edge.inVertex().id() + R_BRACKET;
    s"e[${id}][${srcForVertex.id}-${innerLabel.label}->${tgtForVertex.id}]"
    //    s"e[${srcForVertex.id}-${innerLabel.label}->${tgtForVertex.id}]"
  }

}

object EdgeId {
  val EdgeIdDelimiter = ","

  def fromString(s: String): EdgeId = {
    val js = Json.parse(s)
    s2EdgeIdReads.reads(Json.parse(s)).get
  }

  def isValid(edgeId: EdgeId): Option[EdgeId] = {
    VertexId.isValid(edgeId.srcVertexId).flatMap { _ =>
      VertexId.isValid(edgeId.tgtVertexId).flatMap { _ =>
        Label.findByName(edgeId.labelName).map { _ =>
          edgeId
        }
      }
    }
  }
}

case class EdgeId(srcVertexId: VertexId,
                  tgtVertexId: VertexId,
                  labelName: String,
                  direction: String,
                  ts: Long) {
  override def toString: String = {
    import io.Conversions._
    //    Seq(srcVertexId.toIdString(), tgtVertexId.toIdString(), labelName, direction, ts.toString).mkString(EdgeId.EdgeIdDelimiter)
    s2EdgeIdWrites.writes(this).toString()
  }


}

object EdgeMutate {

  def partitionBufferedIncrement(edges: Seq[IndexEdge]): (Seq[IndexEdge], Seq[IndexEdge]) = {
    edges.partition(_.indexOption.fold(false)(_.isBufferIncrement))
  }

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

case class EdgeMutate(edgesToDelete: Seq[IndexEdge] = Nil,
                      edgesToInsert: Seq[IndexEdge] = Nil,
                      newSnapshotEdge: Option[SnapshotEdge] = None) {

  def deepCopy: EdgeMutate = copy(
    edgesToDelete = edgesToDelete.map(IndexEdge.copyFrom),
    edgesToInsert = edgesToInsert.map(IndexEdge.copyFrom),
    newSnapshotEdge = newSnapshotEdge.map(SnapshotEdge.copyFrom)
  )

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
  type UpdateFunc = (Option[S2EdgeLike], S2EdgeLike, MergeState)

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

  def fillPropsWithTs(edge: S2EdgeLike, state: State): Unit = {
    state.foreach { case (k, v) => edge.propertyInner(k.name, v.innerVal.value, v.ts) }
  }

  def propsToState(props: Props): State = {
    props.asScala.map { case (k, v) =>
      v.labelMeta -> v.innerValWithTs
    }.toMap
  }

  def stateToProps(edge: S2EdgeLike, state: State): Props = {
    state.foreach { case (k, v) =>
      edge.propertyInner(k.name, v.innerVal.value, v.ts)
    }
    edge.getPropsWithTs()
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

  def buildDeleteBulk(invertedEdge: Option[S2EdgeLike], requestEdge: S2EdgeLike): (S2EdgeLike, EdgeMutate) = {
    //    assert(invertedEdge.isEmpty)
    //    assert(requestEdge.op == GraphUtil.operations("delete"))

    val edgesToDelete = requestEdge.relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
    val edgeInverted = Option(requestEdge.toSnapshotEdge)

    (requestEdge, EdgeMutate(edgesToDelete, edgesToInsert = Nil, newSnapshotEdge = edgeInverted))
  }

  def buildOperation(invertedEdge: Option[S2EdgeLike], requestEdges: Seq[S2EdgeLike]): (S2EdgeLike, EdgeMutate) = {
    //            logger.debug(s"oldEdge: ${invertedEdge.map(_.toStringRaw)}")
    //            logger.debug(s"requestEdge: ${requestEdge.toStringRaw}")
    val oldPropsWithTs =
    if (invertedEdge.isEmpty) Map.empty[LabelMeta, InnerValLikeWithTs]
    else propsToState(invertedEdge.get.getPropsWithTs())

    val funcs = requestEdges.map { edge =>
      if (edge.getOp() == GraphUtil.operations("insert")) {
        edge.innerLabel.consistencyLevel match {
          case "strong" => S2Edge.mergeUpsert _
          case _ => S2Edge.mergeInsertBulk _
        }
      } else if (edge.getOp() == GraphUtil.operations("insertBulk")) {
        S2Edge.mergeInsertBulk _
      } else if (edge.getOp() == GraphUtil.operations("delete")) {
        edge.innerLabel.consistencyLevel match {
          case "strong" => S2Edge.mergeDelete _
          case _ => throw new RuntimeException("not supported")
        }
      }
      else if (edge.getOp() == GraphUtil.operations("update")) S2Edge.mergeUpdate _
      else if (edge.getOp() == GraphUtil.operations("increment")) S2Edge.mergeIncrement _
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
        val (_newPropsWithTs, _) = func((prevPropsWithTs, propsToState(requestEdge.getPropsWithTs()), requestEdge.ts, requestEdge.innerLabel.schemaVersion))
        prevPropsWithTs = _newPropsWithTs
        //        logger.debug(s"${requestEdge.toLogString}\n$oldPropsWithTs\n$prevPropsWithTs\n")
      }
      val requestTs = requestEdge.ts
      /* version should be monotoniously increasing so our RPC mutation should be applied safely */
      val newVersion = invertedEdge.map(e => e.getVersion() + incrementVersion).getOrElse(requestTs)
      val maxTs = prevPropsWithTs.map(_._2.ts).max
      val newTs = if (maxTs > requestTs) maxTs else requestTs
      val propsWithTs = prevPropsWithTs ++
        Map(LabelMeta.timestamp -> InnerValLikeWithTs(InnerVal.withLong(newTs, requestEdge.innerLabel.schemaVersion), newTs))

      val edgeMutate = buildMutation(invertedEdge, requestEdge, newVersion, oldPropsWithTs, propsWithTs)

      //      logger.debug(s"${edgeMutate.toLogString}\n${propsWithTs}")
      //      logger.error(s"$propsWithTs")
      val newEdge = requestEdge.copyEdgeWithState(propsWithTs)

      (newEdge, edgeMutate)
    }
  }

  def buildMutation(snapshotEdgeOpt: Option[S2EdgeLike],
                    requestEdge: S2EdgeLike,
                    newVersion: Long,
                    oldPropsWithTs: Map[LabelMeta, InnerValLikeWithTs],
                    newPropsWithTs: Map[LabelMeta, InnerValLikeWithTs]): EdgeMutate = {

    if (oldPropsWithTs == newPropsWithTs) {
      // all requests should be dropped. so empty mutation.
      EdgeMutate(edgesToDelete = Nil, edgesToInsert = Nil, newSnapshotEdge = None)
    } else {
      val withOutDeletedAt = newPropsWithTs.filter(kv => kv._1 != LabelMeta.lastDeletedAtSeq)
      val newOp = snapshotEdgeOpt match {
        case None => requestEdge.getOp()
        case Some(old) =>
          val oldMaxTs = old.getPropsWithTs().asScala.map(_._2.ts).max
          if (oldMaxTs > requestEdge.ts) old.getOp()
          else requestEdge.getOp()
      }

      val newSnapshotEdge = requestEdge.copyOp(newOp).copyVersion(newVersion).copyEdgeWithState(newPropsWithTs)

      val newSnapshotEdgeOpt = Option(newSnapshotEdge.toSnapshotEdge)
      // delete request must always update snapshot.
      if (withOutDeletedAt == oldPropsWithTs && newPropsWithTs.contains(LabelMeta.lastDeletedAt)) {
        // no mutation on indexEdges. only snapshotEdge should be updated to record lastDeletedAt.
        EdgeMutate(edgesToDelete = Nil, edgesToInsert = Nil, newSnapshotEdge = newSnapshotEdgeOpt)
      } else {
        val edgesToDelete = snapshotEdgeOpt match {
          case Some(snapshotEdge) if snapshotEdge.getOp() != GraphUtil.operations("delete") =>
            snapshotEdge.copyOp(GraphUtil.defaultOpByte)
              .relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
          case _ => Nil
        }

        val edgesToInsert =
          if (newPropsWithTs.isEmpty || allPropsDeleted(newPropsWithTs)) Nil
          else {
            val newEdge = requestEdge.copyOp(GraphUtil.defaultOpByte).copyVersion(newVersion).copyEdgeWithState(S2Edge.EmptyState)
            newPropsWithTs.foreach { case (k, v) => newEdge.propertyInner(k.name, v.innerVal.value, v.ts) }

            newEdge.relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
          }


        EdgeMutate(edgesToDelete = edgesToDelete, edgesToInsert = edgesToInsert, newSnapshotEdge = newSnapshotEdgeOpt)
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

  def getServiceColumn(vertex: S2VertexLike, defaultServiceColumn: ServiceColumn) =
    if (vertex.id.column == ServiceColumn.Default) defaultServiceColumn else vertex.id.column

  def srcForVertex(e: S2EdgeLike): S2VertexLike = {
    val belongLabelIds = Seq(e.getLabelId())

    val column = if (e.getDir() == GraphUtil.directions("in")) {
      getServiceColumn(e.tgtVertex, e.innerLabel.tgtColumn)
    } else {
      getServiceColumn(e.srcVertex, e.innerLabel.srcColumn)
    }

    e.innerGraph.elementBuilder.newVertex(VertexId(column, e.srcVertex.innerId), e.srcVertex.ts, e.srcVertex.props, belongLabelIds = belongLabelIds)
  }

  def tgtForVertex(e: S2EdgeLike): S2VertexLike = {
    val belongLabelIds = Seq(e.getLabelId())

    val column = if (e.getDir() == GraphUtil.directions("in")) {
      getServiceColumn(e.srcVertex, e.innerLabel.srcColumn)
    } else {
      getServiceColumn(e.tgtVertex, e.innerLabel.tgtColumn)
    }

    e.innerGraph.elementBuilder.newVertex(VertexId(column, e.tgtVertex.innerId), e.tgtVertex.ts, e.tgtVertex.props, belongLabelIds = belongLabelIds)
  }

  def serializePropsWithTs(edge: S2EdgeLike): Array[Byte] =
    HBaseSerializable.propsToKeyValuesWithTs(edge.getPropsWithTs().asScala.map(kv => kv._2.labelMeta.seq -> kv._2.innerValWithTs).toSeq)
}
