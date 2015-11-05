package com.kakao.s2graph.core


import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.logger
import play.api.libs.json.{Writes, Json}

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3


case class SnapshotEdge(srcVertex: Vertex,
                        tgtVertex: Vertex,
                        labelWithDir: LabelWithDirection,
                        op: Byte,
                        version: Long,
                        props: Map[Byte, InnerValLikeWithTs],
                        pendingEdgeOpt: Option[Edge] = None) {


  //  logger.error(s"EdgeWithIndexInverted${this.toString}")
  val schemaVer = label.schemaVersion
  //  lazy val kvs = Graph.client.snapshotEdgeSerializer(this).toKeyValues.toList

  // only for toString.
  lazy val label = Label.findById(labelWithDir.labelId)
  lazy val propsWithoutTs = props.mapValues(_.innerVal)
  //  lazy val valueBytes = kvs.head.value

  //  def buildPut(): List[Put] = {
  //    kvs.map { kv =>
  //      val put = new Put(kv.row)
  //      put.addColumn(kv.cf, kv.qualifier, kv.timestamp, kv.value)
  //    }
  ////    val put = new Put(rowKey.bytes)
  ////    put.addColumn(edgeCf, qualifier.bytes, version, value.bytes)
  //  }

  def withNoPendingEdge() = copy(pendingEdgeOpt = None)

  def withPendingEdge(pendingEdgeOpt: Option[Edge]) = copy(pendingEdgeOpt = pendingEdgeOpt)
}

case class IndexEdge(srcVertex: Vertex,
                     tgtVertex: Vertex,
                     labelWithDir: LabelWithDirection,
                     op: Byte,
                     ts: Long,
                     labelIndexSeq: Byte,
                     props: Map[Byte, InnerValLike]) extends JSONParser {


  lazy val label = Label.findById(labelWithDir.labelId)
  val schemaVer = label.schemaVersion
  lazy val labelIndex = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelIndexSeq).get
  lazy val defaultIndexMetas = labelIndex.sortKeyTypes.map { meta =>
    val innerVal = toInnerVal(meta.defaultValue, meta.dataType, schemaVer)
    meta.seq -> innerVal
  }.toMap

  lazy val labelIndexMetaSeqs = labelIndex.metaSeqs

  /** TODO: make sure call of this class fill props as this assumes */
  lazy val orders = for (k <- labelIndexMetaSeqs) yield {
    props.get(k) match {
      case None =>

        /**
         * TODO: agly hack
         * now we double store target vertex.innerId/srcVertex.innerId for easy development. later fix this to only store id once
         */
        val v = k match {
          case LabelMeta.timeStampSeq => InnerVal.withLong(ts, schemaVer)
          case LabelMeta.toSeq => tgtVertex.innerId
          case LabelMeta.fromSeq => //srcVertex.innerId
            // for now, it does not make sense to build index on srcVertex.innerId since all edges have same data.
            throw new RuntimeException("_from on indexProps is not supported")
          case _ => defaultIndexMetas(k)
        }

        k -> v
      case Some(v) => k -> v
    }
  }

  lazy val ordersKeyMap = orders.map { case (byte, _) => byte }.toSet
  lazy val metas = for ((k, v) <- props if !ordersKeyMap.contains(k)) yield k -> v

  lazy val propsWithTs = props.map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }

  //TODO:
  //  lazy val kvs = Graph.client.indexedEdgeSerializer(this).toKeyValues.toList

  lazy val hasAllPropsForIndex = orders.length == labelIndexMetaSeqs.length
}

case class Edge(srcVertex: Vertex,
                tgtVertex: Vertex,
                labelWithDir: LabelWithDirection,
                op: Byte = GraphUtil.defaultOpByte,
                ts: Long = System.currentTimeMillis(),
                version: Long = System.currentTimeMillis(),
                propsWithTs: Map[Byte, InnerValLikeWithTs] = Map.empty[Byte, InnerValLikeWithTs],
                pendingEdgeOpt: Option[Edge] = None,
                parentEdges: Seq[EdgeWithScore] = Nil,
                originalEdgeOpt: Option[Edge] = None) extends GraphElement with JSONParser {

  val schemaVer = label.schemaVersion


  def props = propsWithTs.mapValues(_.innerVal)

  def relatedEdges = {
    if (labelWithDir.isDirected) List(this, duplicateEdge)
    else {
      val outDir = labelWithDir.copy(dir = GraphUtil.directions("out"))
      val base = copy(labelWithDir = outDir)
      List(base, base.reverseSrcTgtEdge)
    }
  }

  //  def relatedEdges = List(this)

  def srcForVertex = {
    val belongLabelIds = Seq(labelWithDir.labelId)
    if (labelWithDir.dir == GraphUtil.directions("in")) {
      Vertex(VertexId(label.tgtColumn.id.get, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props, belongLabelIds = belongLabelIds)
    } else {
      Vertex(VertexId(label.srcColumn.id.get, srcVertex.innerId), srcVertex.ts, srcVertex.props, belongLabelIds = belongLabelIds)
    }
  }

  def tgtForVertex = {
    val belongLabelIds = Seq(labelWithDir.labelId)
    if (labelWithDir.dir == GraphUtil.directions("in")) {
      Vertex(VertexId(label.srcColumn.id.get, srcVertex.innerId), srcVertex.ts, srcVertex.props, belongLabelIds = belongLabelIds)
    } else {
      Vertex(VertexId(label.tgtColumn.id.get, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props, belongLabelIds = belongLabelIds)
    }
  }

  def duplicateEdge = reverseSrcTgtEdge.reverseDirEdge

  def reverseDirEdge = copy(labelWithDir = labelWithDir.dirToggled)

  def reverseSrcTgtEdge = copy(srcVertex = tgtVertex, tgtVertex = srcVertex)

  def label = Label.findById(labelWithDir.labelId)

  def labelOrders = LabelIndex.findByLabelIdAll(labelWithDir.labelId)

  override def serviceName = label.serviceName

  override def queueKey = Seq(ts.toString, tgtVertex.serviceName).mkString("|")

  override def queuePartitionKey = Seq(srcVertex.innerId, tgtVertex.innerId).mkString("|")

  override def isAsync = label.isAsync

  def propsPlusTs = propsWithTs.get(LabelMeta.timeStampSeq) match {
    case Some(_) => props
    case None => props ++ Map(LabelMeta.timeStampSeq -> InnerVal.withLong(ts, schemaVer))
  }

  def propsPlusTsValid = propsPlusTs.filter(kv => kv._1 >= 0)

  def edgesWithIndex = for (labelOrder <- labelOrders) yield {
    IndexEdge(srcVertex, tgtVertex, labelWithDir, op, version, labelOrder.seq, propsPlusTs)
  }

  def edgesWithIndexValid = for (labelOrder <- labelOrders) yield {
    IndexEdge(srcVertex, tgtVertex, labelWithDir, op, version, labelOrder.seq, propsPlusTsValid)
  }

  def edgesWithIndexValid(newOp: Byte) = for (labelOrder <- labelOrders) yield {
    IndexEdge(srcVertex, tgtVertex, labelWithDir, newOp, version, labelOrder.seq, propsPlusTsValid)
  }

  /** force direction as out on invertedEdge */
  def toSnapshotEdge: SnapshotEdge = {
    val (smaller, larger) = (srcForVertex, tgtForVertex)

    val newLabelWithDir = LabelWithDirection(labelWithDir.labelId, GraphUtil.directions("out"))

    val ret = SnapshotEdge(smaller, larger, newLabelWithDir, op, version, propsWithTs ++
      Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(ts, schemaVer), ts)), pendingEdgeOpt)
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

  def propsWithName = for {
    (seq, v) <- props
    meta <- label.metaPropsMap.get(seq) if seq > 0
    jsValue <- innerValToJsValue(v, meta.dataType)
  } yield meta.name -> jsValue

  def updateTgtVertex(id: InnerValLike) = {
    val newId = TargetVertexId(tgtVertex.id.colId, id)
    val newTgtVertex = Vertex(newId, tgtVertex.ts, tgtVertex.props)
    Edge(srcVertex, newTgtVertex, labelWithDir, op, ts, version, propsWithTs)
  }


  def toJson = {}

  def rank(r: RankParam): Double =
    if (r.keySeqAndWeights.size <= 0) 1.0f
    else {
      var sum: Double = 0

      for ((seq, w) <- r.keySeqAndWeights) {
        seq match {
          case LabelMeta.countSeq => sum += 1
          case _ => {
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
        }
      }
      sum
    }

  def toLogString: String = {
    import Writes._
    val ret =
      if (propsWithName.nonEmpty)
        List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, label.label, Json.toJson(propsWithName))
      else
        List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, label.label)

    ret.mkString("\t")
  }
}

case class EdgeWriter(edge: Edge) {

  val op = edge.op
  val label = edge.label
  val labelWithDir = edge.labelWithDir

  /**
   * methods for build mutations.
   */
  /** This method only used by Bulk loader */
  //  def insertBulkForLoader(createRelEdges: Boolean = true) = {
  //    val relEdges = if (createRelEdges) edge.relatedEdges else List(edge)
  //    edge.toInvertedEdgeHashLike.buildPut() ++ relEdges.flatMap { relEdge =>
  //      relEdge.edgesWithIndex.flatMap { e =>
  //        e.kvs.map { kv =>
  //          val put = new Put(kv.row)
  //          put.addColumn(kv.cf, kv.qualifier, kv.timestamp, kv.value)
  //        }
  //      }
  //    }
  //  }

}

case class EdgeMutate(edgesToDelete: List[IndexEdge] = List.empty[IndexEdge],
                      edgesToInsert: List[IndexEdge] = List.empty[IndexEdge],
                      newInvertedEdge: Option[SnapshotEdge] = None) {

  def toLogString: String = {
    val deletes = s"deletes: ${edgesToDelete.map(e => e.toString).mkString("\n")}"
    val inserts = s"inserts: ${edgesToInsert.map(e => e.toString).mkString("\n")}"
    val updates = s"snapshot: $newInvertedEdge"

    List(deletes, inserts, updates).mkString("\n\n")
  }
}

object Edge extends JSONParser {
  val incrementVersion = 1L
  val minTsVal = 0L
  // FIXME:

  /** now version information is required also **/
  type State = Map[Byte, InnerValLikeWithTs]
  type PropsPairWithTs = (State, State, Long, String)
  type MergeState = PropsPairWithTs => (State, Boolean)
  type UpdateFunc = (Option[Edge], Edge, MergeState)

  def allPropsDeleted(props: Map[Byte, InnerValLikeWithTs]): Boolean = {
    if (!props.containsKey(LabelMeta.lastDeletedAt)) false
    else {
      val lastDeletedAt = props.get(LabelMeta.lastDeletedAt).get.ts
      for {
        (k, v) <- props if k != LabelMeta.lastDeletedAt
      } {
        if (v.ts > lastDeletedAt) return false
      }
      true
    }
  }


  def buildUpsert(invertedEdge: Option[Edge], requestEdges: Edge): (Edge, EdgeMutate) = {
    //    assert(requestEdge.op == GraphUtil.operations("insert"))
    buildOperation(invertedEdge, Seq(requestEdges))
  }

  def buildUpdate(invertedEdge: Option[Edge], requestEdges: Edge): (Edge, EdgeMutate) = {
    //    assert(requestEdge.op == GraphUtil.operations("update"))
    buildOperation(invertedEdge, Seq(requestEdges))
  }

  def buildDelete(invertedEdge: Option[Edge], requestEdges: Edge): (Edge, EdgeMutate) = {
    //    assert(requestEdge.op == GraphUtil.operations("delete"))
    buildOperation(invertedEdge, Seq(requestEdges))
  }

  def buildIncrement(invertedEdge: Option[Edge], requestEdges: Edge): (Edge, EdgeMutate) = {
    //    assert(requestEdge.op == GraphUtil.operations("increment"))
    buildOperation(invertedEdge, Seq(requestEdges))
  }

  def buildInsertBulk(invertedEdge: Option[Edge], requestEdges: Edge): (Edge, EdgeMutate) = {
    //    assert(invertedEdge.isEmpty)
    //    assert(requestEdge.op == GraphUtil.operations("insertBulk") || requestEdge.op == GraphUtil.operations("insert"))
    buildOperation(None, Seq(requestEdges))
  }

  def buildDeleteBulk(invertedEdge: Option[Edge], requestEdge: Edge): (Edge, EdgeMutate) = {
    //    assert(invertedEdge.isEmpty)
    //    assert(requestEdge.op == GraphUtil.operations("delete"))
    //
    val edgesToDelete = requestEdge.relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
    val edgeInverted = Option(requestEdge.toSnapshotEdge)

    (requestEdge, EdgeMutate(edgesToDelete, edgesToInsert = Nil, edgeInverted))
  }

  def buildOperation(invertedEdge: Option[Edge], requestEdges: Seq[Edge]): (Edge, EdgeMutate) = {
    //            logger.debug(s"oldEdge: ${invertedEdge.map(_.toStringRaw)}")
    //            logger.debug(s"requestEdge: ${requestEdge.toStringRaw}")
    val oldPropsWithTs = if (invertedEdge.isEmpty) Map.empty[Byte, InnerValLikeWithTs] else invertedEdge.get.propsWithTs

    val funcs = requestEdges.map { edge =>
      if (edge.op == GraphUtil.operations("insert")) Edge.mergeUpsert _
      else if (edge.op == GraphUtil.operations("delete")) Edge.mergeDelete _
      else if (edge.op == GraphUtil.operations("update")) Edge.mergeUpdate _
      else if (edge.op == GraphUtil.operations("increment")) Edge.mergeIncrement _
      else throw new RuntimeException(s"not supported operation on edge: $edge")
    }
    val oldTs = invertedEdge.map(e => e.ts).getOrElse(minTsVal)
    val requestWithFuncs = requestEdges.zip(funcs).filter(oldTs != _._1.ts).sortBy(_._1.ts)

    if (requestWithFuncs.isEmpty) {
      logger.info(s"all requests have duplicated timestamp with snapshotEdge.")
      (requestEdges.head, EdgeMutate())
    } else {
      var shouldReplaceCnt = 0
      var prevPropsWithTs = oldPropsWithTs
      var lastOp = GraphUtil.operations("insert")
      var lastTs = 0L
      for {
        (requestEdge, func) <- requestWithFuncs
      } {
        val (_newPropsWithTs, _shouldReplace) = func(prevPropsWithTs, requestEdge.propsWithTs, requestEdge.ts, requestEdge.schemaVer)
        prevPropsWithTs = _newPropsWithTs

        if (_shouldReplace) shouldReplaceCnt += 1

        if (lastTs < requestEdge.ts) {
          lastTs = requestEdge.ts
          lastOp = requestEdge.op
        }

        // 1. EdgeRequests.maxTs
        // 2. invertedEdge.maxTs vs EdgeRequests.maxTs
        // 3. if request.maxTs > invertedEdge.maxTs => request.op
        // 4. else invertedEdge.op
      }

      if (shouldReplaceCnt <= 0) {
        logger.info(s"drop all requests because all request should replaces are false.")
        (requestEdges.head, EdgeMutate())
      } else {

        val maxTsInNewProps = prevPropsWithTs.map(kv => kv._2.ts).max
        val newOp = if (maxTsInNewProps > lastTs) {
          invertedEdge match {
            case None => lastOp
            case Some(old) => old.op
          }
        } else {
          lastOp
        }
        lastOp = newOp

        val newEdgeVersion = invertedEdge.map(e => e.version + incrementVersion).getOrElse(lastTs)

        val maxTs = if (oldTs > lastTs) oldTs else lastTs
        val (requestEdge, _) = requestWithFuncs.head
        val newEdge = Edge(requestEdge.srcVertex, requestEdge.tgtVertex, requestEdge.labelWithDir, lastOp, maxTs, newEdgeVersion, prevPropsWithTs)

        (newEdge, buildReplace(invertedEdge, newEdge, prevPropsWithTs))
      }
    }
  }

  /**
   * delete invertedEdge.edgesWithIndex
   * insert requestEdge.edgesWithIndex
   * update requestEdge.edgesWithIndexInverted
   */
  def buildReplace(invertedEdge: Option[Edge], requestEdge: Edge, newPropsWithTs: Map[Byte, InnerValLikeWithTs]): EdgeMutate = {

    val edgesToDelete = invertedEdge match {
      //      case Some(e) if e.op != GraphUtil.operations("delete") =>
      case Some(e) if !allPropsDeleted(e.propsWithTs) =>
        e.relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
      //      case Some(e) => e.edgesWithIndexValid
      case _ =>
        // nothing to remove on indexed.
        List.empty[IndexEdge]
    }

    val edgesToInsert = {
      if (newPropsWithTs.isEmpty) List.empty[IndexEdge]
      else {
        if (allPropsDeleted(newPropsWithTs)) {
          // all props is older than lastDeletedAt so nothing to insert on indexed.
          List.empty[IndexEdge]
        } else {
          /** force operation on edge as insert */
          requestEdge.relatedEdges.flatMap { relEdge =>
            relEdge.edgesWithIndexValid(GraphUtil.defaultOpByte)
          }
        }
      }
    }

    val edgeInverted = if (newPropsWithTs.isEmpty) None else Some(requestEdge.toSnapshotEdge)
    val update = EdgeMutate(edgesToDelete, edgesToInsert, edgeInverted)
    update
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
          if (oldValWithTs.ts >= requestTs || k < 0) Some(k -> oldValWithTs)
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
          if (k == LabelMeta.timeStampSeq) {
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
      if (k == LabelMeta.timeStampSeq) {
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

  def fromString(s: String): Option[Edge] = Graph.toEdge(s)
}
