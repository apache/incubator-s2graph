package com.daumkakao.s2graph.core
import HBaseElement._
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.{ Delete, Mutation, Put, Result }
import org.hbase.async.{HBaseRpc, DeleteRequest, PutRequest}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import play.api.libs.json.Json

case class EdgeWithIndexInverted(srcVertex: Vertex, tgtVertex: Vertex, labelWithDir: LabelWithDirection, op: Byte, version: Long, props: Map[Byte, InnerValWithTs]) {
  import GraphConstant._
  import Edge._

  //  Logger.error(s"EdgeWithIndexInverted${this.toString}")
  lazy val lastModifiedAt = props.map(_._2.ts).max

  lazy val rowKey = EdgeRowKey(srcVertex.id, labelWithDir, LabelIndex.defaultSeq, isInverted = true)

  lazy val qualifier = EdgeQualifierInverted(tgtVertex.id)

  // only for toString.
  lazy val label = Label.findById(labelWithDir.labelId)
  lazy val propsWithoutTs = props.map(kv => (kv._1 -> kv._2.innerVal))

  lazy val value = EdgeValueInverted(op, props.toList)

  def buildPut() = {
    val put = new Put(rowKey.bytes)
    put.add(edgeCf, qualifier.bytes, version, value.bytes)
  }
  def buildPutAsync() = {
    new PutRequest(label.hbaseTableName.getBytes, rowKey.bytes, edgeCf, qualifier.bytes, value.bytes, version)
  }

  def isSame(other: Any): Boolean = {
    val ret = this.toString == other.toString
    logger.debug(s"EdgeWithIndexInverted\n$this\n$other\n$ret")
    ret
  }

  override def toString(): String = {
    val ls = ListBuffer(lastModifiedAt, GraphUtil.fromOp(op), "e",
      srcVertex.innerId, tgtVertex.innerId, label.label)
    //    if (!propsWithName.isEmpty) ls += Json.toJson(propsWithName)
    if (!props.isEmpty) ls += props
    ls.mkString("\t")
  }
}
case class EdgeWithIndex(srcVertex: Vertex, tgtVertex: Vertex, labelWithDir: LabelWithDirection, op: Byte, version: Long, labelIndexSeq: Byte, props: Map[Byte, InnerVal]) {

  assert(props.get(LabelMeta.timeStampSeq).isDefined)

  lazy val ts = props(LabelMeta.timeStampSeq).longV.get

  import GraphConstant._
  import Edge._

  lazy val rowKey = EdgeRowKey(srcVertex.id, labelWithDir, labelIndexSeq, isInverted = false)
  lazy val labelIndex = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelIndexSeq).get
  lazy val defaultIndexMetas = labelIndex.sortKeyTypes.map(meta => meta.seq -> meta.defaultInnerVal).toMap
  lazy val labelIndexMetaSeqs = labelIndex.metaSeqs

  lazy val orders = for (k <- labelIndexMetaSeqs) yield {
    props.get(k) match {
      case None =>
        /**
         *  TODO: agly hack
         * 	now we double store target vertex.innerId/srcVertex.innerId for easy development. later fix this to only store id once
         */
        val v = k match {
          case LabelMeta.timeStampSeq => InnerVal.withLong(ts)
          case LabelMeta.toSeq => tgtVertex.innerId
          case LabelMeta.fromSeq => //srcVertex.innerId
            // for now, it does not make sense to build index on srcVertex.innerId since all edges have same data.
            throw new RuntimeException("_from on indexProps is not supported")
          case _ => defaultIndexMetas(k)
        }
        //        val v = if (k == LabelMeta.timeStampSeq) InnerVal.withLong(ts) else defaultIndexMetas(k)
        (k -> v)
      case Some(v) => (k -> v)
    }
  }
  lazy val metas = for ((k, v) <- props if !defaultIndexMetas.containsKey(k)) yield (k -> v)

  lazy val qualifier = EdgeQualifier(orders, tgtVertex.id, op)
  lazy val value = EdgeValue(metas.toList)

  lazy val hasAllPropsForIndex = orders.length == labelIndexMetaSeqs.length

  // only for toString.
  lazy val label = Label.findById(labelWithDir.labelId)

  def buildPuts(): List[Put] = {
    if (!hasAllPropsForIndex) {
      logger.error(s"$this dont have all props for index")
      List.empty[Put]
    } else {
      val put = new Put(rowKey.bytes)
      //    Logger.debug(s"$this")
      //      Logger.debug(s"EdgeWithIndex.buildPut: $rowKey, $qualifier, $value")
      put.add(edgeCf, qualifier.bytes, version, value.bytes)
      List(put)
    }
  }
  def buildPutsAsync(): List[PutRequest] = {
    if (!hasAllPropsForIndex) {
      logger.error(s"$this dont have all props for index")
      List.empty[PutRequest]
    } else {
      List(new PutRequest(label.hbaseTableName.getBytes, rowKey.bytes, edgeCf, qualifier.bytes, value.bytes, version))
    }
  }
  def buildDeletes(): List[Delete] = {
    if (!hasAllPropsForIndex) List.empty[Delete]
    else {
      val delete = new Delete(rowKey.bytes)
      delete.deleteColumns(edgeCf, qualifier.bytes, version)
      List(delete)
    }
  }
  def buildDeletesAsync(): List[DeleteRequest] = {
    if (!hasAllPropsForIndex) List.empty[DeleteRequest]
    else {
      List(new DeleteRequest(label.hbaseTableName.getBytes, rowKey.bytes, edgeCf, qualifier.bytes, version))
    }
  }
  override def toString(): String = {
    val ls = ListBuffer(ts, GraphUtil.fromOp(op), "e",
      srcVertex.innerId, tgtVertex.innerId, label.label)
    //    if (!propsWithName.isEmpty) ls += Json.toJson(propsWithName ++ Map("version" -> version.toString))
    if (!props.isEmpty) ls += props
    ls.mkString("\t")
  }
  def isSame(other: Any): Boolean = {
    val ret = this.toString == other.toString
    logger.debug(s"EdgeWithIndex\n$this\n$other\n$ret")
    ret
  }
}
/**
 *
 */
case class Edge(srcVertex: Vertex, tgtVertex: Vertex, labelWithDir: LabelWithDirection, op: Byte,
  ts: Long, version: Long, propsWithTs: Map[Byte, InnerValWithTs]) extends GraphElement with JSONParser {

  val logger = Edge.logger
  implicit val ex = Graph.executionContext

  lazy val props =
    if (op == GraphUtil.operations("delete")) Map(LabelMeta.timeStampSeq -> InnerVal.withLong(ts))
    else for ((k, v) <- propsWithTs) yield (k -> v.innerVal)

  lazy val relatedEdges = {
    labelWithDir.dir match {
      case 2 => //undirected
        val out = LabelWithDirection(labelWithDir.labelId, 0)
        val base = Edge(srcVertex, tgtVertex, out, op, ts, version, propsWithTs)
        List(base, base.duplicateEdge, base.reverseDirEdge, base.reverseSrcTgtEdge)
      case 0 | 1 =>
        List(this, duplicateEdge)
    }
  }

  lazy val srcForVertex = Vertex(srcVertex.id.updateIsEdge(false), srcVertex.ts, srcVertex.props)
  lazy val tgtForVertex = Vertex(tgtVertex.id.updateIsEdge(false), tgtVertex.ts, tgtVertex.props)

  lazy val duplicateEdge = Edge(tgtVertex, srcVertex, labelWithDir.dirToggled, op, ts, version, propsWithTs)
  lazy val reverseDirEdge = Edge(srcVertex, tgtVertex, labelWithDir.dirToggled, op, ts, version, propsWithTs)
  lazy val reverseSrcTgtEdge = Edge(tgtVertex, srcVertex, labelWithDir, op, ts, version, propsWithTs)

  lazy val label = Label.findById(labelWithDir.labelId)
  lazy val labelOrders = LabelIndex.findByLabelIdAll(labelWithDir.labelId)
  override lazy val serviceName = label.serviceName
  override lazy val queueKey = Seq(ts.toString, tgtVertex.serviceName).mkString("|")
  override lazy val queuePartitionKey = Seq(srcVertex.innerId, tgtVertex.innerId).mkString("|")

  lazy val propsPlusTs = propsWithTs.get(LabelMeta.timeStampSeq) match {
    case Some(_) => props
    case None => props ++ Map(LabelMeta.timeStampSeq -> InnerVal.withLong(ts))
  }

  lazy val propsPlusTsValid = propsPlusTs.filter(kv => kv._1 >= 0)

  lazy val edgesWithIndex = {
    for (labelOrder <- labelOrders) yield {
      EdgeWithIndex(srcVertex, tgtVertex, labelWithDir, op, version, labelOrder.seq, propsPlusTs)
    }
  }
  lazy val edgesWithIndexValid = {
    for (labelOrder <- labelOrders) yield {
      EdgeWithIndex(srcVertex, tgtVertex, labelWithDir, op, version, labelOrder.seq, propsPlusTsValid)
    }
  }
  def edgesWithIndex(newOp: Byte) = {
    for (labelOrder <- labelOrders) yield {
      EdgeWithIndex(srcVertex, tgtVertex, labelWithDir, newOp, version, labelOrder.seq, propsPlusTs)
    }
  }
  def edgesWithIndexValid(newOp: Byte) = {
    for (labelOrder <- labelOrders) yield {
      EdgeWithIndex(srcVertex, tgtVertex, labelWithDir, newOp, version, labelOrder.seq, propsPlusTsValid)
    }
  }

  lazy val edgesWithInvertedIndex = {
    EdgeWithIndexInverted(srcVertex, tgtVertex, labelWithDir, op, version, propsWithTs ++
      Map(LabelMeta.timeStampSeq -> InnerValWithTs(InnerVal.withLong(ts), ts)))
  }
  def edgesWithInvertedIndex(newOp: Byte) = {
    EdgeWithIndexInverted(srcVertex, tgtVertex, labelWithDir, newOp, version, propsWithTs ++
      Map(LabelMeta.timeStampSeq -> InnerValWithTs(InnerVal.withLong(ts), ts)))
  }
  lazy val propsWithName = for {
    (seq, v) <- props
    name <- label.metaPropNamesMap.get(seq) if seq > 0
  } yield (name -> innerValToJsValue(v))

  lazy val canBeBatched =
    op == GraphUtil.operations("insertBulk") ||
      (op == GraphUtil.operations("insert") && label.consistencyLevel != "strong")

  def updateTgtVertex(id: InnerVal) = {
    val newCompositeId = CompositeId(tgtVertex.id.colId, id, isEdge = tgtVertex.id.isEdge, useHash = tgtVertex.id.useHash)
    val newTgtVertex = Vertex(newCompositeId, tgtVertex.ts, tgtVertex.props)
    Edge(srcVertex, newTgtVertex, labelWithDir, op, ts, version, propsWithTs)
  }

  /**
   * methods for build mutations.
   */
  def buildVertexPuts(): List[Put] = srcForVertex.buildPuts ++ tgtForVertex.buildPuts
//
  def buildVertexPutsAsync(): List[PutRequest] = srcForVertex.buildPutsAsync() ++ tgtForVertex.buildPutsAsync()

  def buildPutsAll(): List[HBaseRpc] = {
    val edgePuts =
      relatedEdges.flatMap { edge =>
        if (edge.op == GraphUtil.operations("insert")) {
          if (label.consistencyLevel == "strong") {
            edge.upsert()
            List.empty[PutRequest]
          } else {
            edge.insert()
          }
        } else if (edge.op == GraphUtil.operations("delete")) {
          edge.delete(label.consistencyLevel == "strong")
          List.empty[PutRequest]
        } else if (edge.op == GraphUtil.operations("update")) {
          edge.update()
          List.empty[PutRequest]
        } else if (edge.op == GraphUtil.operations("increment")) {
          edge.increment()
          List.empty[PutRequest]
        } else if (edge.op == GraphUtil.operations("insertBulk")) {
          edge.insert()
        } else {
          throw new Exception(s"operation[${edge.op}] is not supported on edge.")
        }

      }
    edgePuts
  }
  def insertBulk() = {
    edgesWithInvertedIndex.buildPut :: edgesWithIndex.flatMap(e => e.buildPuts())
  }
  def insert() = {
    edgesWithInvertedIndex.buildPutAsync :: edgesWithIndex.flatMap(e => e.buildPutsAsync)
  }
  /**
   * write must be synchronous to keep consistencyLevel
   */
//  def fetchInverted() = {
//    Graph.getEdgeSync(srcVertex, tgtVertex, label, labelWithDir.dir)
//  }
  def fetchInvertedAsync() = {
    Graph.getEdge(srcVertex, tgtVertex, label, labelWithDir.dir)
  }
  /**
   *
   */

//  def mutate(f: (Option[Edge], Edge) => EdgeUpdate): Unit = {
//    val edgeUpdate = f(fetchInverted().headOption, this)
//    withWriteTable(label.hbaseZkAddr, label.hbaseTableName) { table =>
//      val size = edgeUpdate.mutations.size
//      val rets: Array[Object] = Array.fill(size)(null)
//      table.batch(edgeUpdate.mutations, rets)
//      for (ret <- rets if ret == null) {
//        Logger.error(s"mutation tryed but failed.")
//      }
//    } {
//      Logger.error(s"mutation failed.")
//    }
//  }

  def mutate(f: (Option[Edge], Edge) => EdgeUpdate): Unit = {
    try {
      val client = Graph.getClient(label.hbaseZkAddr)
      for {
        edges <- fetchInvertedAsync()
        invertedEdgeOpt = edges.headOption
        edgeUpdate = f(invertedEdgeOpt, this)
      } yield {
        /**
         * we can use CAS operation on inverted Edge checkAndSet() and if it return false
         * then re-read and build update and write recursively.
         */
        Graph.writeAsync(label.hbaseZkAddr, edgeUpdate.mutations)
      }
//      client.flush()
    } catch {
      case e: Throwable =>
        logger.error(s"mutate failed. $e", e)
    }
  }

  def upsert(): Unit = {
    mutate(Edge.buildUpsert)
  }

  def delete(mutateInPlace: Boolean = true) = {
    mutate(Edge.buildDelete)
  }

  def update() = {
    mutate(Edge.buildUpdate)
  }

  def increment() = {
    mutate(Edge.buildIncrement)
  }

  def toJson() = {}

  def rank(r: RankParam): Double = {

    if (r.keySeqAndWeights.size <= 0) 1.0f
    else {
      //      Logger.warn(s"orderMapping : ${orderByKeyValues} keyIdAndWeights : ${r.keyIdAndWeights}")

      var sum: Double = 0
      for {
        (seq, w) <- r.keySeqAndWeights
      } {
        seq match {
          case -1 => { //case key == "count"
            sum += 1
          }
          case _ => {
            props.get(seq) match {
              case None =>
              //                Logger.error(s"Not Found SortKeyType : ${seq} for rank in Label(${labelWithDir.labelId}})'s OrderByKeys(${orderByKey.typeIds}})")
              case Some(innerVal) => {
                sum += w * innerVal.longV.getOrElse(1L).toDouble
              }
            }
          }
        }
      }
      sum
    }
  }
  override def toString(): String = {
    val ls = ListBuffer(ts, GraphUtil.fromOp(op), "e",
      srcVertex.innerId, tgtVertex.innerId, label.label)
    if (!propsWithName.isEmpty) ls += Json.toJson(propsWithName)
    //    if (!propsPlusTs.isEmpty) ls += propsPlusTs
    ls.mkString("\t")
  }
  def toStringRaw(): String = {
    val ls = ListBuffer(ts, GraphUtil.fromOp(op), "e",
      srcVertex.innerId, tgtVertex.innerId, label.label)
    if (!propsPlusTs.isEmpty) ls += propsPlusTs
    ls.mkString("\t")
  }
}

case class EdgeUpdate(mutations: List[HBaseRpc] = List.empty[HBaseRpc],
                      edgesToDelete: List[EdgeWithIndex] = List.empty[EdgeWithIndex],
                      edgesToInsert: List[EdgeWithIndex] = List.empty[EdgeWithIndex],
                      newInvertedEdge: Option[EdgeWithIndexInverted] = None) {

  override def toString(): String = {
    val size = s"mutations: ${mutations.size}"
    val deletes = s"deletes: ${edgesToDelete.map(e => e.toString).mkString("\n")}"
    val inserts = s"inserts: ${edgesToInsert.map(e => e.toString).mkString("\n")}"
    val updates = s"snapshot: $newInvertedEdge"
    List(size, deletes, inserts, updates).mkString("\n")
  }
}

object Edge {
  val logger = LoggerFactory.getLogger(classOf[Edge])
  //  val initialVersion = 2L
  val incrementVersion = 1L
  val minOperationTs = 1L
  val minTsVal = 0L
  type PropsPairWithTs = (Map[Byte, InnerValWithTs], Map[Byte, InnerValWithTs], Long)

  def buildUpsert(invertedEdge: Option[Edge], requestEdge: Edge): EdgeUpdate = {
    assert(requestEdge.op == GraphUtil.operations("insert"))
    buildOperation(invertedEdge, requestEdge)(buildUpsert)
  }
  def buildUpdate(invertedEdge: Option[Edge], requestEdge: Edge): EdgeUpdate = {
    assert(requestEdge.op == GraphUtil.operations("update"))
    buildOperation(invertedEdge, requestEdge)(buildUpdate)
  }
  def buildDelete(invertedEdge: Option[Edge], requestEdge: Edge): EdgeUpdate = {
    assert(requestEdge.op == GraphUtil.operations("delete"))
    buildOperation(invertedEdge, requestEdge)(buildDelete)
  }
  def buildIncrement(invertedEdge: Option[Edge], requestEdge: Edge): EdgeUpdate = {
    assert(requestEdge.op == GraphUtil.operations("increment"))
    buildOperation(invertedEdge, requestEdge)(buildIncrement)
  }
  def buildOperation(invertedEdge: Option[Edge], requestEdge: Edge)(f: PropsPairWithTs => (Map[Byte, InnerValWithTs], Boolean)) = {
//        Logger.debug(s"oldEdge: ${invertedEdge.map(_.toStringRaw)}")
//        Logger.debug(s"requestEdge: ${requestEdge.toStringRaw}")

    val oldPropsWithTs = if (invertedEdge.isEmpty) Map.empty[Byte, InnerValWithTs] else invertedEdge.get.propsWithTs
    val minTsInOldProps = if (invertedEdge.isEmpty) minTsVal else invertedEdge.get.propsWithTs.map(kv => kv._2.ts).min

    val oldTs = invertedEdge.map(e => e.ts).getOrElse(minTsVal)

    if (oldTs == requestEdge.ts) {
      logger.error(s"duplicate timestamp on same edge. $requestEdge")
      EdgeUpdate()
    } else {
      val (newPropsWithTs, shouldReplace) = f(oldPropsWithTs, requestEdge.propsWithTs, requestEdge.ts)

      if (!shouldReplace) {
        logger.error(s"drop request $requestEdge becaseu shouldReplace is $shouldReplace")
        EdgeUpdate()
      } else {

        val maxTsInNewProps = newPropsWithTs.map(kv => kv._2.ts).max
        val newOp = if (maxTsInNewProps > requestEdge.ts) {
          invertedEdge match {
            case None => requestEdge.op
            case Some(old) => old.op
          }
        } else {
          requestEdge.op
        }
        val newEdgeVersion = invertedEdge.map(e => e.version + incrementVersion).getOrElse(requestEdge.ts)

        val maxTs = if (oldTs > requestEdge.ts) oldTs else requestEdge.ts
        val newEdge = Edge(requestEdge.srcVertex, requestEdge.tgtVertex, requestEdge.labelWithDir,
          newOp, maxTs, newEdgeVersion, newPropsWithTs)

        buildReplace(invertedEdge, newEdge, newPropsWithTs)
      }
    }

  }
  def buildUpsert(propsPairWithTs: PropsPairWithTs) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt).map(v => v.ts).getOrElse(minTsVal)
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      propsWithTs.get(k) match {
        case Some(newValWithTs) =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          val v = if (oldValWithTs.ts >= newValWithTs.ts) oldValWithTs else {
            shouldReplace = true
            newValWithTs
          }
          Some(k -> v)
        //          if (v.ts > lastDeletedAt) Some(k -> v)
        //          else {
        //            shouldReplace = true
        //            None
        //          }

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
  def buildUpdate(propsPairWithTs: PropsPairWithTs) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt).map(v => v.ts).getOrElse(minTsVal)
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      propsWithTs.get(k) match {
        case Some(newValWithTs) =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          val v = if (oldValWithTs.ts >= newValWithTs.ts) oldValWithTs else {
            shouldReplace = true
            newValWithTs
          }
          Some(k -> v)
        //          if (v.ts > lastDeletedAt) Some(k -> v)
        //          else None

        case None =>
          // important: update need to merge previous valid values.
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
  def buildIncrement(propsPairWithTs: PropsPairWithTs) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt).map(v => v.ts).getOrElse(minTsVal)
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      propsWithTs.get(k) match {
        case Some(newValWithTs) =>
          if (k == LabelMeta.timeStampSeq) {
            val v = if (oldValWithTs.ts >= newValWithTs.ts) oldValWithTs else {
              shouldReplace = true
              newValWithTs
            }
            Some(k -> v)
          } else {
            //            assert(oldValWithTs.ts >= lastDeletedAt)
            //            if (oldValWithTs.ts < newValWithTs.ts) {
            //              shouldReplace = true
            //              Some(k -> InnerValWithTs(oldValWithTs.innerVal + newValWithTs.innerVal, oldValWithTs.ts))
            //            } else if (newValWithTs.ts >= lastDeletedAt){
            //              shouldReplace = true
            //              Some(k -> InnerValWithTs(oldValWithTs.innerVal + newValWithTs.innerVal, newValWithTs.ts))
            //            } else {
            //              Some(k -> oldValWithTs)
            //            }
            if (oldValWithTs.ts >= newValWithTs.ts) {
              Some(k -> oldValWithTs)
              //              if (newValWithTs.ts > lastDeletedAt) {
              //                shouldReplace = true
              //                Some(k -> InnerValWithTs(oldValWithTs.innerVal + newValWithTs.innerVal, newValWithTs.ts))
              //              } else {
              //                Some(k -> oldValWithTs)
              //              }

              //              if (oldValWithTs.ts > lastDeletedAt) Some(k -> oldValWithTs)
              //              else None
            } else {
              assert(oldValWithTs.ts < newValWithTs.ts && oldValWithTs.ts >= lastDeletedAt)
              shouldReplace = true
              // incr(t0), incr(t2), d(t1) => deleted
              Some(k -> InnerValWithTs(oldValWithTs.innerVal + newValWithTs.innerVal, oldValWithTs.ts))

              //              if (oldValWithTs.ts > lastDeletedAt) {
              //                
              //                Some(k -> InnerValWithTs(oldValWithTs.innerVal + newValWithTs.innerVal, newValWithTs.ts))
              //              } else if (oldValWithTs.ts <= lastDeletedAt && lastDeletedAt < newValWithTs.ts) {
              //                Some(k -> newValWithTs)
              //              } else None
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
  def buildDelete(propsPairWithTs: PropsPairWithTs) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs) = propsPairWithTs
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
          Some(k -> InnerValWithTs.withLong(requestTs, requestTs))
        }
      } else {
        if (oldValWithTs.ts >= lastDeletedAt) Some(k -> oldValWithTs)
        else {
          shouldReplace = true
          None
        }
      }
    }
    val mustExistInNew = Map(LabelMeta.lastDeletedAt -> InnerValWithTs.withLong(lastDeletedAt, lastDeletedAt))
    ((existInOld.flatten ++ mustExistInNew).toMap, shouldReplace)
  }

  private def allPropsDeleted(props: Map[Byte, InnerValWithTs]) = {
    val maxTsProp = props.toList.sortBy(kv => (kv._2.ts, -1 * kv._1)).reverse.head
    maxTsProp._1 == LabelMeta.lastDeletedAt
  }
  /**
   * delete invertedEdge.edgesWithIndex
   * insert requestEdge.edgesWithIndex
   * update requestEdge.edgesWithIndexInverted
   */
  def buildReplace(invertedEdge: Option[Edge], requestEdge: Edge, newPropsWithTs: Map[Byte, InnerValWithTs]): EdgeUpdate = {

    val edgesToDelete = invertedEdge match {
      //      case Some(e) if e.op != GraphUtil.operations("delete") => e.edgesWithIndexValid
      case Some(e) => e.edgesWithIndexValid
      case _ =>
        // nothing to remove on indexed.
        List.empty[EdgeWithIndex]
    }

    val edgesToInsert = {
      if (newPropsWithTs.isEmpty) List.empty[EdgeWithIndex]
      else {
        //        val maxTsProp = newPropsWithTs.toList.sortBy(kv => (kv._2.ts, -1 * kv._1)).reverse.head
        //        if (maxTsProp._1 == LabelMeta.lastDeletedAt) {
        if (allPropsDeleted(newPropsWithTs)) {
          // all props is older than lastDeletedAt so nothing to insert on indexed.
          List.empty[EdgeWithIndex]
        } else {
          requestEdge.edgesWithIndexValid
        }
      }
    }
    val edgeInverted = if (newPropsWithTs.isEmpty) None else Some(requestEdge.edgesWithInvertedIndex)

    val deleteMutations = edgesToDelete.flatMap(edge => edge.buildDeletesAsync).toList
    val insertMutations = edgesToInsert.flatMap(edge => edge.buildPutsAsync).toList
    val invertMutations = edgeInverted.map(e => List(e.buildPutAsync)).getOrElse(List.empty[PutRequest])
    val mutations = deleteMutations ++ insertMutations ++ invertMutations

    val update = EdgeUpdate(mutations, edgesToDelete, edgesToInsert, edgeInverted)

//        Logger.debug(s"UpdatedProps: ${newPropsWithTs}\n")
//        Logger.debug(s"EdgeUpdate: $update\n")
    update
  }
  def fromString(s: String): Option[Edge] = Graph.toEdge(s)

  def toEdge(kv: org.hbase.async.KeyValue, param: QueryParam): Option[Edge] = {
    val version = kv.timestamp()

    val rowKey = EdgeRowKey(kv.key(), 0)
    val srcVertexId = rowKey.srcVertexId
    val (tgtVertexId, props, op, ts) = rowKey.isInverted match {
      case true =>
        val qualifier = EdgeQualifierInverted(kv.qualifier(), 0)
        val value = EdgeValueInverted(kv.value(), 0)
        val kvsMap = value.props.toMap
        val ts = kvsMap.get(LabelMeta.timeStampSeq) match {
          case None => version
          case Some(v) => v.innerVal.longV.get
        }
        (qualifier.tgtVertexId, kvsMap, value.op, ts)
      case false =>
        val kvQual = kv.qualifier()
        val qualifier = EdgeQualifier(kvQual, 0, kvQual.length)
        val value = EdgeValue(kv.value(), 0)
        val kvs = qualifier.propsKVs(rowKey.labelWithDir.labelId, rowKey.labelOrderSeq) ::: value.props.toList
        val kvsMap = kvs.toMap

        val ts = kvsMap.get(LabelMeta.timeStampSeq) match {
          case None => version
          case Some(v) => v.longV.get
        }
        val kvsMapWithoutTs = kvsMap.map(kv => (kv._1 -> InnerValWithTs(kv._2, ts)))
        (qualifier.tgtVertexId, kvsMapWithoutTs, qualifier.op, ts)
    }
    //    Logger.error(s"toEdge: $rowKey, $tgtVertexId")
    val labelMetas = LabelMeta.findAllByLabelId(rowKey.labelWithDir.labelId, useCache = true)
    val propsWithDefault = (for (meta <- labelMetas) yield {
      props.get(meta.seq) match {
        case Some(v) => (meta.seq -> v)
        case None => (meta.seq -> InnerValWithTs(meta.defaultInnerVal, minTsVal))
      }
    }).toMap
    //    play.api.Logger.debug(s"$rowKey, $tgtVertexId, $props, $op, $ts => ${param.hasFilters}")
    /**
     * TODO: backward compatability only. deprecate has field
     */
    val edge = Edge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), rowKey.labelWithDir, op, ts, version, props)
    //    if (param.propsFilters.filter(edge)) Some(edge) else None
    val matches =
      for {
        (k, v) <- param.hasFilters
        edgeVal <- propsWithDefault.get(k) if edgeVal.innerVal.value == v.value
      } yield (k -> v)

    val ret = if (matches.size == param.hasFilters.size && param.where.map(_.filter(edge)).getOrElse(true)) {
      //      val edge = Edge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), rowKey.labelWithDir, op, ts, version, props)
      //      Logger.debug(s"fetchedEdge: $edge")
      Some(edge)
    } else {
      None
    }
    //    Logger.debug(s"${cell.getQualifier().toList}, ${ret.map(x => x.toStringRaw)}")
    ret
  }
  def toEdge(kv: org.hbase.async.KeyValue): Edge = {
    val version = kv.timestamp()

    val rowKey = EdgeRowKey(kv.key(), 0)
    val srcVertexId = rowKey.srcVertexId
    val (tgtVertexId, props, op, ts) = rowKey.isInverted match {
      case true =>
        val qualifier = EdgeQualifierInverted(kv.qualifier(), 0)
        val value = EdgeValueInverted(kv.value(), 0)
        val kvsMap = value.props.toMap
        val ts = kvsMap.get(LabelMeta.timeStampSeq) match {
          case None => version
          case Some(v) => v.innerVal.longV.get
        }
        (qualifier.tgtVertexId, kvsMap, value.op, ts)
      case false =>
        val kvQual = kv.qualifier()
        val qualifier = EdgeQualifier(kvQual, 0, kvQual.length)
        val value = EdgeValue(kv.value(), 0)
        val kvs = qualifier.propsKVs(rowKey.labelWithDir.labelId, rowKey.labelOrderSeq) ::: value.props.toList
        val kvsMap = kvs.toMap

        val ts = kvsMap.get(LabelMeta.timeStampSeq) match {
          case None => version
          case Some(v) => v.longV.get
        }
        val kvsMapWithoutTs = kvsMap.map(kv => (kv._1 -> InnerValWithTs(kv._2, ts)))
        (qualifier.tgtVertexId, kvsMapWithoutTs, qualifier.op, ts)
    }
    //    Logger.error(s"toEdge: $rowKey, $tgtVertexId")
    val labelMetas = LabelMeta.findAllByLabelId(rowKey.labelWithDir.labelId, useCache = true)
    val propsWithDefault = (for (meta <- labelMetas) yield {
      props.get(meta.seq) match {
        case Some(v) => (meta.seq -> v)
        case None => (meta.seq -> InnerValWithTs(meta.defaultInnerVal, minTsVal))
      }
    }).toMap
    //    play.api.Logger.debug(s"$rowKey, $tgtVertexId, $props, $op, $ts => ${param.hasFilters}")
    /**
     * TODO: backward compatability only. deprecate has field
     */
    Edge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), rowKey.labelWithDir, op, ts, version, props)
  }

}
