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

import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, LabelMeta}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.structure
import play.api.libs.json.{JsNumber, JsObject, Json}
import scala.util.hashing.MurmurHash3
import org.apache.tinkerpop.gremlin.structure.{Edge => TpEdge, Direction, Property, Graph => TpGraph}

case class SnapshotEdge(srcVertex: Vertex,
                        tgtVertex: Vertex,
                        label: Label,
                        direction: Int,
                        op: Byte,
                        version: Long,
                        private val propsWithTs: Map[LabelMeta, InnerValLikeWithTs],
                        pendingEdgeOpt: Option[Edge],
                        statusCode: Byte = 0,
                        lockTs: Option[Long],
                        tsInnerValOpt: Option[InnerValLike] = None) {

  lazy val labelWithDir = LabelWithDirection(label.id.get, direction)
  if (!propsWithTs.contains(LabelMeta.timestamp)) throw new Exception("Timestamp is required.")

//  val label = Label.findById(labelWithDir.labelId)
  lazy val schemaVer = label.schemaVersion
  lazy val propsWithoutTs = propsWithTs.mapValues(_.innerVal)
  lazy val ts = propsWithTs(LabelMeta.timestamp).innerVal.toString().toLong

  def propsToKeyValuesWithTs = HBaseSerializable.propsToKeyValuesWithTs(propsWithTs.map(kv => kv._1.seq -> kv._2).toSeq)

  def allPropsDeleted = Edge.allPropsDeleted(propsWithTs)

  def toEdge: Edge = {
    val ts = propsWithTs.get(LabelMeta.timestamp).map(v => v.ts).getOrElse(version)
    Edge(srcVertex, tgtVertex, label, direction, op,
      version, propsWithTs, pendingEdgeOpt = pendingEdgeOpt,
      statusCode = statusCode, lockTs = lockTs, tsInnerValOpt = tsInnerValOpt)
  }

  def propsWithName = (for {
    (meta, v) <- propsWithTs
    jsValue <- innerValToJsValue(v.innerVal, meta.dataType)
  } yield meta.name -> jsValue) ++ Map("version" -> JsNumber(version))

  // only for debug
  def toLogString() = {
    List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, label.label, propsWithName).mkString("\t")
  }
}

case class IndexEdge(srcVertex: Vertex,
                     tgtVertex: Vertex,
                     label: Label,
                     direction: Int,
                     op: Byte,
                     version: Long,
                     labelIndexSeq: Byte,
                     private val propsWithTs: Map[LabelMeta, InnerValLikeWithTs],
                     tsInnerValOpt: Option[InnerValLike] = None)  {
//  if (!props.contains(LabelMeta.timeStampSeq)) throw new Exception("Timestamp is required.")
  //  assert(props.contains(LabelMeta.timeStampSeq))
  lazy val labelWithDir = LabelWithDirection(label.id.get, direction)

  lazy val isInEdge = labelWithDir.dir == GraphUtil.directions("in")
  lazy val isOutEdge = !isInEdge

  lazy val ts = propsWithTs(LabelMeta.timestamp).innerVal.toString.toLong
  lazy val degreeEdge = propsWithTs.contains(LabelMeta.degree)

  lazy val schemaVer = label.schemaVersion
  lazy val labelIndex = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelIndexSeq).get
  lazy val defaultIndexMetas = labelIndex.sortKeyTypes.map { meta =>
    val innerVal = toInnerVal(meta.defaultValue, meta.dataType, schemaVer)
    meta.seq -> innerVal
  }.toMap

  lazy val labelIndexMetaSeqs = labelIndex.sortKeyTypes

  /** TODO: make sure call of this class fill props as this assumes */
  lazy val orders = for (meta <- labelIndexMetaSeqs) yield {
    propsWithTs.get(meta) match {
      case None =>

        /**
          * TODO: agly hack
          * now we double store target vertex.innerId/srcVertex.innerId for easy development. later fix this to only store id once
          */
        val v = meta match {
          case LabelMeta.timestamp=> InnerVal.withLong(version, schemaVer)
          case LabelMeta.to => toEdge.tgtVertex.innerId
          case LabelMeta.from => toEdge.srcVertex.innerId
          // for now, it does not make sense to build index on srcVertex.innerId since all edges have same data.
          //            throw new RuntimeException("_from on indexProps is not supported")
          case _ => toInnerVal(meta.defaultValue, meta.dataType, schemaVer)
        }

        meta -> v
      case Some(v) => meta -> v.innerVal
    }
  }

  lazy val ordersKeyMap = orders.map { case (byte, _) => byte }.toSet
  lazy val metas = for ((meta, v) <- propsWithTs if !ordersKeyMap.contains(meta)) yield meta -> v.innerVal

//  lazy val propsWithTs = props.map { case (k, v) => k -> InnerValLikeWithTs(v, version) }

  //TODO:
  //  lazy val kvs = Graph.client.indexedEdgeSerializer(this).toKeyValues.toList

  lazy val hasAllPropsForIndex = orders.length == labelIndexMetaSeqs.length

  def propsWithName = for {
    (meta, v) <- propsWithTs if meta.seq >= 0
    jsValue <- innerValToJsValue(v.innerVal, meta.dataType)
  } yield meta.name -> jsValue


  def toEdge: Edge = Edge(srcVertex, tgtVertex, label, direction, op, version, propsWithTs, tsInnerValOpt = tsInnerValOpt)

  // only for debug
  def toLogString() = {
    List(version, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, label.label, Json.toJson(propsWithName)).mkString("\t")
  }

  def property(key: String): Option[InnerValLikeWithTs] = {
    label.metaPropsInvMap.get(key).map(labelMeta => property(labelMeta))
  }

  def property(labelMeta: LabelMeta): InnerValLikeWithTs = {
    propsWithTs.get(labelMeta).getOrElse(label.metaPropsDefaultMapInner(labelMeta))
  }

  def updatePropsWithTs(others: Map[LabelMeta, InnerValLikeWithTs] = Map.empty): Map[LabelMeta, InnerValLikeWithTs] = {
    if (others.isEmpty) propsWithTs
    else propsWithTs ++ others
  }
}

case class Edge(srcVertex: Vertex,
                tgtVertex: Vertex,
                innerLabel: Label,
                dir: Int,
                op: Byte = GraphUtil.defaultOpByte,
                version: Long = System.currentTimeMillis(),
                private val propsWithTs: Map[LabelMeta, InnerValLikeWithTs],
                parentEdges: Seq[EdgeWithScore] = Nil,
                originalEdgeOpt: Option[Edge] = None,
                pendingEdgeOpt: Option[Edge] = None,
                statusCode: Byte = 0,
                lockTs: Option[Long] = None,
                tsInnerValOpt: Option[InnerValLike] = None) extends GraphElement with TpEdge {

  lazy val labelWithDir = LabelWithDirection(innerLabel.id.get, dir)
  lazy val schemaVer = innerLabel.schemaVersion
  lazy val ts = propsWithTs(LabelMeta.timestamp).innerVal.value match {
    case b: BigDecimal => b.longValue()
    case l: Long => l
    case i: Int => i.toLong
    case _ => throw new RuntimeException("ts should be in [BigDecimal/Long/Int].")
  }

  //FIXME
  lazy val tsInnerVal = tsInnerValOpt.get.value
  lazy val srcId = srcVertex.innerIdVal
  lazy val tgtId = tgtVertex.innerIdVal
  lazy val labelName = innerLabel.label
  lazy val direction = GraphUtil.fromDirection(dir)
  
  def toIndexEdge(labelIndexSeq: Byte): IndexEdge = IndexEdge(srcVertex, tgtVertex, innerLabel, dir, op, version, labelIndexSeq, propsWithTs)

  def serializePropsWithTs(): Array[Byte] = HBaseSerializable.propsToKeyValuesWithTs(propsWithTs.map(kv => kv._1.seq -> kv._2).toSeq)

  def updatePropsWithTs(others: Map[LabelMeta, InnerValLikeWithTs] = Map.empty): Map[LabelMeta, InnerValLikeWithTs] = {
    if (others.isEmpty) propsWithTs
    else propsWithTs ++ others
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
    propsWithTs.getOrElse(labelMeta, innerLabel.metaPropsDefaultMapInner(labelMeta))
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

  def props = propsWithTs.mapValues(_.innerVal)


  private def toProps(): Map[String, Any] = {
    for {
      (labelMeta, defaultVal) <- innerLabel.metaPropsDefaultMapInner
    } yield {
      labelMeta.name -> propsWithTs.getOrElse(labelMeta, defaultVal).innerVal.value
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
      Vertex(VertexId(innerLabel.tgtColumn.id.get, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props, belongLabelIds = belongLabelIds)
    } else {
      Vertex(VertexId(innerLabel.srcColumn.id.get, srcVertex.innerId), srcVertex.ts, srcVertex.props, belongLabelIds = belongLabelIds)
    }
  }

  def tgtForVertex = {
    val belongLabelIds = Seq(labelWithDir.labelId)
    if (labelWithDir.dir == GraphUtil.directions("in")) {
      Vertex(VertexId(innerLabel.srcColumn.id.get, srcVertex.innerId), srcVertex.ts, srcVertex.props, belongLabelIds = belongLabelIds)
    } else {
      Vertex(VertexId(innerLabel.tgtColumn.id.get, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props, belongLabelIds = belongLabelIds)
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

  def isDegree = propsWithTs.contains(LabelMeta.degree)

//  def propsPlusTs = propsWithTs.get(LabelMeta.timeStampSeq) match {
//    case Some(_) => props
//    case None => props ++ Map(LabelMeta.timeStampSeq -> InnerVal.withLong(ts, schemaVer))
//  }

  def propsPlusTsValid = propsWithTs.filter(kv => LabelMeta.isValidSeq(kv._1.seq))

  def edgesWithIndex = for (labelOrder <- labelOrders) yield {
    IndexEdge(srcVertex, tgtVertex, innerLabel, dir, op, version, labelOrder.seq, propsWithTs, tsInnerValOpt = tsInnerValOpt)
  }

  def edgesWithIndexValid = for (labelOrder <- labelOrders) yield {
    IndexEdge(srcVertex, tgtVertex, innerLabel, dir, op, version, labelOrder.seq, propsPlusTsValid, tsInnerValOpt = tsInnerValOpt)
  }

  /** force direction as out on invertedEdge */
  def toSnapshotEdge: SnapshotEdge = {
    val (smaller, larger) = (srcForVertex, tgtForVertex)

//    val newLabelWithDir = LabelWithDirection(labelWithDir.labelId, GraphUtil.directions("out"))

    val ret = SnapshotEdge(smaller, larger, innerLabel, GraphUtil.directions("out"), op, version,
      Map(LabelMeta.timestamp -> InnerValLikeWithTs(InnerVal.withLong(ts, schemaVer), ts)) ++ propsWithTs,
      pendingEdgeOpt = pendingEdgeOpt, statusCode = statusCode, lockTs = lockTs, tsInnerValOpt = tsInnerValOpt)
    ret
  }

  override def hashCode(): Int = {
    MurmurHash3.stringHash(srcVertex.innerId + "," + labelWithDir + "," + tgtVertex.innerId)
  }

  override def equals(other: Any): Boolean = other match {
    case e: Edge =>
      srcVertex.innerId == e.srcVertex.innerId &&
        tgtVertex.innerId == e.tgtVertex.innerId &&
        labelWithDir == e.labelWithDir
    case _ => false
  }

  def defaultPropsWithName = Json.obj("from" -> srcVertex.innerId.toString(), "to" -> tgtVertex.innerId.toString(),
    "label" -> innerLabel.label, "service" -> innerLabel.serviceName)

  def propsWithName =
    for {
      (meta, v) <- props if meta.seq > 0
      jsValue <- innerValToJsValue(v, meta.dataType)
    } yield meta.name -> jsValue


  def updateTgtVertex(id: InnerValLike) = {
    val newId = TargetVertexId(tgtVertex.id.colId, id)
    val newTgtVertex = Vertex(newId, tgtVertex.ts, tgtVertex.props)
    Edge(srcVertex, newTgtVertex, innerLabel, dir, op, version, propsWithTs, tsInnerValOpt = tsInnerValOpt)
  }

  def rank(r: RankParam): Double =
    if (r.keySeqAndWeights.size <= 0) 1.0f
    else {
      var sum: Double = 0

      for ((seq, w) <- r.keySeqAndWeights) {
        propsWithTs.get(seq) match {
          case None => // do nothing
          case Some(innerValWithTs) => {
            val cost = try innerValWithTs.innerVal.toString.toDouble catch {
              case e: Exception =>
                logger.error("toInnerval failed in rank", e)
                1.0
            }
            sum += w * cost
          }
        }
      }
      sum
    }

  def toLogString: String = {
    val allPropsWithName = defaultPropsWithName ++ Json.toJson(propsWithName).asOpt[JsObject].getOrElse(Json.obj())
    List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, innerLabel.label, allPropsWithName).mkString("\t")
  }

  override def vertices(direction: Direction): util.Iterator[structure.Vertex] = ???

  override def properties[V](strings: String*): util.Iterator[Property[V]] = ???

  override def property[V](key: String): Property[V] = ???

  override def property[V](key: String, value: V): Property[V] = {
    property(key, value, System.currentTimeMillis())
  }

  def property[V](key: String, value: V, ts: Long): Property[V] = ???

  override def remove(): Unit = ???

  override def graph(): TpGraph = ???
  
  override def id(): AnyRef = ???

  override def label(): String = innerLabel.label
}


case class EdgeMutate(edgesToDelete: List[IndexEdge] = List.empty[IndexEdge],
                      edgesToInsert: List[IndexEdge] = List.empty[IndexEdge],
                      newSnapshotEdge: Option[SnapshotEdge] = None) {

  def toLogString: String = {
    val l = (0 until 50).map(_ => "-").mkString("")
    val deletes = s"deletes: ${edgesToDelete.map(e => e.toLogString).mkString("\n")}"
    val inserts = s"inserts: ${edgesToInsert.map(e => e.toLogString).mkString("\n")}"
    val updates = s"snapshot: ${newSnapshotEdge.map(e => e.toLogString).mkString("\n")}"

    List("\n", l, deletes, inserts, updates, l, "\n").mkString("\n")
  }
}

object Edge {
  val incrementVersion = 1L
  val minTsVal = 0L

  def toEdge(srcId: Any,
                    tgtId: Any,
                    labelName: String,
                    direction: String,
                    props: Map[String, Any] = Map.empty,
                    ts: Long = System.currentTimeMillis(),
                    operation: String = "insert"): Edge = {
    val label = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(labelName))

    val srcVertexId = toInnerVal(srcId.toString, label.srcColumn.columnType, label.schemaVersion)
    val tgtVertexId = toInnerVal(tgtId.toString, label.tgtColumn.columnType, label.schemaVersion)

    val srcColId = label.srcColumn.id.get
    val tgtColId = label.tgtColumn.id.get

    val srcVertex = Vertex(SourceVertexId(srcColId, srcVertexId), System.currentTimeMillis())
    val tgtVertex = Vertex(TargetVertexId(tgtColId, tgtVertexId), System.currentTimeMillis())
    val dir = GraphUtil.toDir(direction).getOrElse(throw new RuntimeException(s"$direction is not supported."))

    val labelWithDir = LabelWithDirection(label.id.get, dir)
    val propsPlusTs = props ++ Map(LabelMeta.timestamp.name -> ts)
    val propsWithTs = label.propsToInnerValsWithTs(propsPlusTs, ts)
    val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

    new Edge(srcVertex, tgtVertex, label, dir, op = op, version = ts, propsWithTs = propsWithTs)
  }

  /** now version information is required also **/
  type State = Map[LabelMeta, InnerValLikeWithTs]
  type PropsPairWithTs = (State, State, Long, String)
  type MergeState = PropsPairWithTs => (State, Boolean)
  type UpdateFunc = (Option[Edge], Edge, MergeState)

  def allPropsDeleted(props: Map[LabelMeta, InnerValLikeWithTs]): Boolean =
    if (!props.contains(LabelMeta.lastDeletedAt)) false
    else {
      val lastDeletedAt = props.get(LabelMeta.lastDeletedAt).get.ts
      val propsWithoutLastDeletedAt = props - LabelMeta.lastDeletedAt

      propsWithoutLastDeletedAt.forall { case (_, v) => v.ts <= lastDeletedAt }
    }

  def buildDeleteBulk(invertedEdge: Option[Edge], requestEdge: Edge): (Edge, EdgeMutate) = {
    //    assert(invertedEdge.isEmpty)
    //    assert(requestEdge.op == GraphUtil.operations("delete"))

    val edgesToDelete = requestEdge.relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
    val edgeInverted = Option(requestEdge.toSnapshotEdge)

    (requestEdge, EdgeMutate(edgesToDelete, edgesToInsert = Nil, newSnapshotEdge = edgeInverted))
  }

  def buildOperation(invertedEdge: Option[Edge], requestEdges: Seq[Edge]): (Edge, EdgeMutate) = {
    //            logger.debug(s"oldEdge: ${invertedEdge.map(_.toStringRaw)}")
    //            logger.debug(s"requestEdge: ${requestEdge.toStringRaw}")
    val oldPropsWithTs =
      if (invertedEdge.isEmpty) Map.empty[LabelMeta, InnerValLikeWithTs] else invertedEdge.get.propsWithTs

    val funcs = requestEdges.map { edge =>
      if (edge.op == GraphUtil.operations("insert")) {
        edge.innerLabel.consistencyLevel match {
          case "strong" => Edge.mergeUpsert _
          case _ => Edge.mergeInsertBulk _
        }
      } else if (edge.op == GraphUtil.operations("insertBulk")) {
        Edge.mergeInsertBulk _
      } else if (edge.op == GraphUtil.operations("delete")) {
        edge.innerLabel.consistencyLevel match {
          case "strong" => Edge.mergeDelete _
          case _ => throw new RuntimeException("not supported")
        }
      }
      else if (edge.op == GraphUtil.operations("update")) Edge.mergeUpdate _
      else if (edge.op == GraphUtil.operations("increment")) Edge.mergeIncrement _
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
        val (_newPropsWithTs, _) = func(prevPropsWithTs, requestEdge.propsWithTs, requestEdge.ts, requestEdge.schemaVer)
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
      (requestEdge.copy(propsWithTs = propsWithTs), edgeMutate)
    }
  }

  def filterOutWithLabelOption(ls: Seq[IndexEdge]): Seq[IndexEdge] = ls.filter { ie =>
    ie.labelIndex.dir match {
      case None =>
        // both direction use same indices that is defined when label creation.
        true
      case Some(dir) =>
        if (dir != ie.direction) {
          // current labelIndex's direction is different with indexEdge's direction so don't touch
          false
        } else {
          ie.labelIndex.writeOption.map { option =>
            val hashValueOpt = ie.orders.find { case (k, v) => k == LabelMeta.fromHash }.map{ case (k, v) => v.value.toString.toLong }
            option.sample(ie, hashValueOpt)
          }.getOrElse(true)
        }
    }
  }

  def buildMutation(snapshotEdgeOpt: Option[Edge],
                    requestEdge: Edge,
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
          val oldMaxTs = old.propsWithTs.map(_._2.ts).max
          if (oldMaxTs > requestEdge.ts) old.op
          else requestEdge.op
      }

      val newSnapshotEdgeOpt =
        Option(requestEdge.copy(op = newOp, propsWithTs = newPropsWithTs, version = newVersion).toSnapshotEdge)
      // delete request must always update snapshot.
      if (withOutDeletedAt == oldPropsWithTs && newPropsWithTs.contains(LabelMeta.lastDeletedAt)) {
        // no mutation on indexEdges. only snapshotEdge should be updated to record lastDeletedAt.
        EdgeMutate(edgesToDelete = Nil, edgesToInsert = Nil, newSnapshotEdge = newSnapshotEdgeOpt)
      } else {
        val edgesToDelete = snapshotEdgeOpt match {
          case Some(snapshotEdge) if snapshotEdge.op != GraphUtil.operations("delete") =>
            snapshotEdge.copy(op = GraphUtil.defaultOpByte)
              .relatedEdges.flatMap { relEdge => filterOutWithLabelOption(relEdge.edgesWithIndexValid) }
          case _ => Nil
        }

        val edgesToInsert =
          if (newPropsWithTs.isEmpty || allPropsDeleted(newPropsWithTs)) Nil
          else
            requestEdge.copy(
              version = newVersion,
              propsWithTs = newPropsWithTs,
              op = GraphUtil.defaultOpByte
            ).relatedEdges.flatMap { relEdge => filterOutWithLabelOption(relEdge.edgesWithIndexValid) }

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
