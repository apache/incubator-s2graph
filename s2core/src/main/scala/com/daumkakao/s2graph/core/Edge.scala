package com.daumkakao.s2graph.core

//import com.daumkakao.s2graph.core.mysqls.{Label, LabelIndex, LabelMeta}
//import HBaseElement._

//import com.daumkakao.s2graph.core.models.{Label, LabelIndex, LabelMeta}
import com.daumkakao.s2graph.core.mysqls._
import com.daumkakao.s2graph.core.types2._

//import com.daumkakao.s2graph.core.types.EdgeType._
//import com.daumkakao.s2graph.core.types._

import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{AtomicIncrementRequest, HBaseRpc, DeleteRequest, PutRequest}
import org.slf4j.LoggerFactory
import play.api.Logger
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import play.api.libs.json.Json

case class EdgeWithIndexInverted(srcVertex: Vertex,
                                 tgtVertex: Vertex,
                                 labelWithDir: LabelWithDirection,
                                 op: Byte,
                                 version: Long,
                                 props: Map[Byte, InnerValLikeWithTs]) {

  import GraphConstant._
  import Edge._

  //  Logger.error(s"EdgeWithIndexInverted${this.toString}")
  lazy val lastModifiedAt = props.map(_._2.ts).max
  lazy val schemaVer = label.schemaVersion
  lazy val rowKey = EdgeRowKey(srcVertex.id, labelWithDir, LabelIndex.defaultSeq, isInverted = true)(version = schemaVer)

  lazy val qualifier = EdgeQualifierInverted(tgtVertex.id)(version = schemaVer)
  lazy val value = EdgeValueInverted(op, props.toList)(version = schemaVer)

  // only for toString.
  lazy val label = Label.findById(labelWithDir.labelId)
  lazy val propsWithoutTs = props.map(kv => (kv._1 -> kv._2.innerVal))



  def buildPut() = {
    val put = new Put(rowKey.bytes)
    put.addColumn(edgeCf, qualifier.bytes, version, value.bytes)
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

case class EdgeWithIndex(srcVertex: Vertex,
                         tgtVertex: Vertex,
                         labelWithDir: LabelWithDirection,
                         op: Byte,
                         ts: Long,
                         labelIndexSeq: Byte,
                         props: Map[Byte, InnerValLike]) extends JSONParser {

  //  assert(props.get(LabelMeta.timeStampSeq).isDefined)

  //  lazy val ts = props(LabelMeta.timeStampSeq).value.asInstanceOf[BigDecimal].toLong

  import GraphConstant._
  import Edge._

  lazy val label = Label.findById(labelWithDir.labelId)
  lazy val schemaVer = label.schemaVersion
  lazy val labelIndex = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelIndexSeq).get
  lazy val defaultIndexMetas = (labelIndex.sortKeyTypes.map { meta =>
    val innerVal = toInnerVal(meta.defaultValue, meta.dataType, schemaVer)
    meta.seq -> innerVal
  }).toMap
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
        //        val v = if (k == LabelMeta.timeStampSeq) InnerVal.withLong(ts) else defaultIndexMetas(k)
        (k -> v)
      case Some(v) => (k -> v)
    }
  }
  lazy val ordersKeyMap = orders.map(_._1).toSet
  lazy val metas = for ((k, v) <- props if !ordersKeyMap.contains(k)) yield (k -> v)

  lazy val rowKey = EdgeRowKey(srcVertex.id, labelWithDir, labelIndexSeq, isInverted = false)(schemaVer)
  lazy val qualifier = EdgeQualifier(orders, tgtVertex.id, op)(label.schemaVersion)
  lazy val value = EdgeValue(metas.toList)(label.schemaVersion)

  lazy val hasAllPropsForIndex = orders.length == labelIndexMetaSeqs.length

  def buildPuts(): List[Put] = {
    if (!hasAllPropsForIndex) {
      Logger.error(s"$this dont have all props for index")
      List.empty[Put]
    } else {
      val put = new Put(rowKey.bytes)
      //    Logger.debug(s"$this")
      //      Logger.debug(s"EdgeWithIndex.buildPut: $rowKey, $qualifier, $value")
      put.addColumn(edgeCf, qualifier.bytes, ts, value.bytes)
      List(put)
    }
  }

  def buildPutsAsync(): List[HBaseRpc] = {
    if (!hasAllPropsForIndex) {
      Logger.error(s"$this dont have all props for index")
      List.empty[PutRequest]
    } else {
      val put = new PutRequest(label.hbaseTableName.getBytes, rowKey.bytes, edgeCf, qualifier.bytes, value.bytes, ts)
      //      Logger.debug(s"$put")
      List(put)
    }
  }

  def buildIncrementsAsync(amount: Long = 1L): List[HBaseRpc] = {
    if (!hasAllPropsForIndex) {
      Logger.error(s"$this dont have all props for index")
      List.empty[AtomicIncrementRequest]
    } else {
      val incr = new AtomicIncrementRequest(label.hbaseTableName.getBytes, rowKey.bytes, edgeCf, Array.empty[Byte], amount)
      List(incr)
    }
  }

  def buildDeletes(): List[Delete] = {
    if (!hasAllPropsForIndex) List.empty[Delete]
    else {
      val delete = new Delete(rowKey.bytes)
      delete.addColumns(edgeCf, qualifier.bytes, ts)
      List(delete)
    }
  }

  def buildDeletesAsync(): List[HBaseRpc] = {
    if (!hasAllPropsForIndex) List.empty[DeleteRequest]
    else {
      List(new DeleteRequest(label.hbaseTableName.getBytes, rowKey.bytes, edgeCf, qualifier.bytes, ts))
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
case class Edge(srcVertex: Vertex,
                tgtVertex: Vertex,
                labelWithDir: LabelWithDirection,
                op: Byte,
                ts: Long,
                version: Long,
                propsWithTs: Map[Byte, InnerValLikeWithTs]) extends GraphElement with JSONParser {

  val logger = Edge.logger
  implicit val ex = Graph.executionContext
  lazy val schemaVer = label.schemaVersion
  lazy val props =
      if (op == GraphUtil.operations("delete")) Map(LabelMeta.timeStampSeq -> InnerVal.withLong(ts, schemaVer))
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
//  assert(srcVertex.isInstanceOf[SourceVertexId] && tgtVertex.isInstanceOf[TargetVertexId])

  lazy val srcForVertex = Vertex(SourceVertexId.toVertexId(srcVertex.id), srcVertex.ts, srcVertex.props)
  lazy val tgtForVertex = Vertex(TargetVertexId.toVertexId(tgtVertex.id), tgtVertex.ts, tgtVertex.props)

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
    case None => props ++ Map(LabelMeta.timeStampSeq -> InnerVal.withLong(ts, schemaVer))
  }

  lazy val propsPlusTsValid = propsPlusTs.filter(kv => kv._1 >= 0)

  lazy val edgesWithIndex = {
    for (labelOrder <- labelOrders) yield {
      EdgeWithIndex(srcVertex, tgtVertex, labelWithDir, op, version, labelOrder.seq, propsPlusTs)
    }
  }

  def edgesWithIndex(newOp: Byte) = {
    for (labelOrder <- labelOrders) yield {
      EdgeWithIndex(srcVertex, tgtVertex, labelWithDir, newOp, version, labelOrder.seq, propsPlusTs)
    }
  }

  lazy val edgesWithIndexValid = {
    for (labelOrder <- labelOrders) yield {
      EdgeWithIndex(srcVertex, tgtVertex, labelWithDir, op, version, labelOrder.seq, propsPlusTsValid)
    }
  }

  def edgesWithIndexValid(newOp: Byte) = {
    for (labelOrder <- labelOrders) yield {
      EdgeWithIndex(srcVertex, tgtVertex, labelWithDir, newOp, version, labelOrder.seq, propsPlusTsValid)
    }
  }

  lazy val edgesWithInvertedIndex = {
    EdgeWithIndexInverted(srcVertex, tgtVertex, labelWithDir, op, version, propsWithTs ++
      Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(ts, schemaVer), ts)))
  }

  def edgesWithInvertedIndex(newOp: Byte) = {
    EdgeWithIndexInverted(srcVertex, tgtVertex, labelWithDir, newOp, version, propsWithTs ++
      Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(ts, schemaVer), ts)))
  }

  lazy val propsWithName = for {
    (seq, v) <- props
    meta <- label.metaPropsMap.get(seq) if seq > 0
    jsValue <- innerValToJsValue(v, meta.dataType)
  } yield (meta.name -> jsValue)

  lazy val canBeBatched =
    op == GraphUtil.operations("insertBulk") ||
      (op == GraphUtil.operations("insert") && label.consistencyLevel != "strong")

  def updateTgtVertex(id: InnerValLike) = {
    val newId = TargetVertexId(tgtVertex.id.colId, id)
    val newTgtVertex = Vertex(newId, tgtVertex.ts, tgtVertex.props)
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
    edgePuts ++ buildVertexPutsAsync
  }

  def insertBulk() = {
    edgesWithInvertedIndex.buildPut() :: edgesWithIndex.flatMap(e => e.buildPuts())
  }

  def insert() = {
    val puts = edgesWithInvertedIndex.buildPutAsync :: edgesWithIndex.flatMap(e => e.buildPutsAsync)
    val incrs = edgesWithIndex.flatMap(e => e.buildIncrementsAsync())
    puts ++ incrs
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
        //        Logger.debug(s"$edgeUpdate")
        /**
         * we can use CAS operation on inverted Edge checkAndSet() and if it return false
         * then re-read and build update and write recursively.
         */
        Graph.writeAsync(label.hbaseZkAddr, edgeUpdate.mutations)
        /** degree */
        val incrs =
          (edgeUpdate.edgesToDelete.isEmpty, edgeUpdate.edgesToInsert.isEmpty) match {
            case (true, true) =>

              /** when there is no need to update. shouldUpdate == false */
              List.empty[AtomicIncrementRequest]
            case (true, false) =>

              /** no edges to delete but there is new edges to insert so increase degree by 1 */
              this.edgesWithIndexValid.flatMap(e => e.buildIncrementsAsync())
            case (false, true) =>

              /** no edges to insert but there is old edges to delete so decrease degree by 1 */
              this.edgesWithIndexValid.flatMap(e => e.buildIncrementsAsync(-1L))
            case (false, false) =>

              /** update on existing edges so no change on degree */
              //              if (edgeUpdate.edgesToDelete.find(e => e.op == GraphUtil.operations("delete")).isDefined) {
              //                this.edgesWithIndexValid.flatMap(e => e.buildIncrementsAsync())
              //              } else {
              List.empty[AtomicIncrementRequest]
            //              }
          }
        //        Logger.debug(s"Increment: $incrs")
        Graph.writeAsync(label.hbaseZkAddr, incrs)
      }
      //      client.flush()
    } catch {
      case e: Throwable =>
        Logger.error(s"mutate failed. $e", e)
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
          case -1 => {
            //case key == "count"
            sum += 1
          }
          case _ => {
            props.get(seq) match {
              case None =>
              //                Logger.error(s"Not Found SortKeyType : ${seq} for rank in Label(${labelWithDir.labelId}})'s OrderByKeys(${orderByKey.typeIds}})")
              case Some(innerVal) => {
                val cost = if (innerVal.value.isInstanceOf[BigDecimal]) innerVal.value.asInstanceOf[BigDecimal].toDouble else 1.0
                sum += w * cost
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
//    if (!propsWithName.isEmpty) ls += Json.toJson(propsWithName)
        if (!propsPlusTs.isEmpty) ls += propsPlusTs
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

object Edge extends JSONParser {
  val logger = LoggerFactory.getLogger(classOf[Edge])
  //  val initialVersion = 2L
  val incrementVersion = 1L
  val minOperationTs = 1L
  val minTsVal = 0L
  /** now version information is required also **/
  type PropsPairWithTs = (Map[Byte, InnerValLikeWithTs], Map[Byte, InnerValLikeWithTs], Long, String)

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

  def buildOperation(invertedEdge: Option[Edge], requestEdge: Edge)(f: PropsPairWithTs => (Map[Byte, InnerValLikeWithTs], Boolean)) = {
    //        Logger.debug(s"oldEdge: ${invertedEdge.map(_.toStringRaw)}")
    //        Logger.debug(s"requestEdge: ${requestEdge.toStringRaw}")

    val oldPropsWithTs = if (invertedEdge.isEmpty) Map.empty[Byte, InnerValLikeWithTs] else invertedEdge.get.propsWithTs
    val minTsInOldProps = if (invertedEdge.isEmpty) minTsVal else invertedEdge.get.propsWithTs.map(kv => kv._2.ts).min

    val oldTs = invertedEdge.map(e => e.ts).getOrElse(minTsVal)

    if (oldTs == requestEdge.ts) {
      logger.error(s"duplicate timestamp on same edge. $requestEdge")
      EdgeUpdate()
    } else {
      val (newPropsWithTs, shouldReplace) =
        f(oldPropsWithTs, requestEdge.propsWithTs, requestEdge.ts, requestEdge.schemaVer)

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

  def buildUpdate(propsPairWithTs: PropsPairWithTs) = {
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
              Some(k -> InnerValLikeWithTs(oldValWithTs.innerVal + newValWithTs.innerVal, oldValWithTs.ts))

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

  private def allPropsDeleted(props: Map[Byte, InnerValLikeWithTs]) = {
    val maxTsProp = props.toList.sortBy(kv => (kv._2.ts, -1 * kv._1)).reverse.head
    maxTsProp._1 == LabelMeta.lastDeletedAt
  }

  /**
   * delete invertedEdge.edgesWithIndex
   * insert requestEdge.edgesWithIndex
   * update requestEdge.edgesWithIndexInverted
   */
  def buildReplace(invertedEdge: Option[Edge], requestEdge: Edge, newPropsWithTs: Map[Byte, InnerValLikeWithTs]): EdgeUpdate = {

    val edgesToDelete = invertedEdge match {
      case Some(e) if e.op != GraphUtil.operations("delete") => e.edgesWithIndexValid
      //      case Some(e) => e.edgesWithIndexValid
      case _ =>
        // nothing to remove on indexed.
        List.empty[EdgeWithIndex]
    }

    val edgesToInsert = {
      if (newPropsWithTs.isEmpty) List.empty[EdgeWithIndex]
      else {
        if (allPropsDeleted(newPropsWithTs)) {
          // all props is older than lastDeletedAt so nothing to insert on indexed.
          List.empty[EdgeWithIndex]
        } else {
          /** force operation on edge as insert */
          requestEdge.edgesWithIndexValid(GraphUtil.defaultOpByte)
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
    //    Logger.debug(s"$update")
    update
  }

  def fromString(s: String): Option[Edge] = Graph.toEdge(s)

  def toEdge(kv: org.hbase.async.KeyValue, param: QueryParam): Option[Edge] = {
    val version = kv.timestamp()
    val keyBytes = kv.key()
    val rowKey = EdgeRowKey.fromBytes(keyBytes, 0, keyBytes.length, param.label.schemaVersion)
    val srcVertexId = rowKey.srcVertexId
    var isDegree = false
    val (tgtVertexId, props, op, ts) = rowKey.isInverted match {
      case true =>
        val qBytes = kv.qualifier()
        val vBytes = kv.value()
        val qualifier = EdgeQualifierInverted.fromBytes(qBytes, 0, qBytes.length, param.label.schemaVersion)
        val value = EdgeValueInverted.fromBytes(vBytes, 0, vBytes.length, param.label.schemaVersion)
        val kvsMap = value.props.toMap
        val ts = kvsMap.get(LabelMeta.timeStampSeq) match {
          case None => version
          case Some(v) => BigDecimal(v.innerVal.toString).toLong
        }
        (qualifier.tgtVertexId, kvsMap, value.op, ts)
      case false =>
        val kvQual = kv.qualifier()
        val vBytes = kv.value()
        if (kvQual.length == 0) {
          /** degree */
          isDegree = true
          val degree = Bytes.toLong(kv.value())
          // dirty hack
          val ts = kv.timestamp()
          val dummyProps = Map(LabelMeta.degreeSeq -> InnerValLikeWithTs.withLong(degree, ts, param.label.schemaVersion))
          (rowKey.srcVertexId, dummyProps, GraphUtil.operations("insert"), ts)
        } else {
          /** edge */
          val qualifier = EdgeQualifier.fromBytes(kvQual, 0, kvQual.length, param.label.schemaVersion)
          println(qualifier)
          val value = EdgeValue.fromBytes(vBytes, 0, vBytes.length, param.label.schemaVersion)

          val index = param.label.indicesMap.get(rowKey.labelOrderSeq).getOrElse(
            throw new RuntimeException(s"can`t find index sequence for $rowKey ${param.label}"))

          val kvs = qualifier.propsKVs(index.metaSeqs) ::: value.props.toList
          val kvsMap = kvs.toMap
          val tgtVertexId = if (qualifier.tgtVertexId == null) {
            kvsMap.get(LabelMeta.toSeq) match {
              case None => qualifier.tgtVertexId
              case Some(vId) => TargetVertexId(VertexId.DEFAULT_COL_ID, vId)
            }
          } else {
            qualifier.tgtVertexId
          }

          val ts = kvsMap.get(LabelMeta.timeStampSeq).map { v => BigDecimal(v.value.toString).toLong }.getOrElse(version)
          val mergedProps = kvsMap.map { case (k, innerVal) => k -> InnerValLikeWithTs(innerVal, ts) }
          (tgtVertexId, mergedProps, qualifier.op, ts)
        }
    }
    if (!param.includeDegree && isDegree) {
      None
    } else {
      val edge = Edge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), rowKey.labelWithDir, op, ts, version, props)

          Logger.debug(s"toEdge: $srcVertexId, $tgtVertexId, $props, $op, $ts")
      val labelMetas = LabelMeta.findAllByLabelId(rowKey.labelWithDir.labelId)
      val propsWithDefault = (for (meta <- labelMetas) yield {
        props.get(meta.seq) match {
          case Some(v) => (meta.seq -> v)
          case None =>
            val defaultInnerVal = toInnerVal(meta.defaultValue, meta.dataType, param.label.schemaVersion)
            (meta.seq -> InnerValLikeWithTs(defaultInnerVal, minTsVal))
        }
      }).toMap
      /**
       * TODO: backward compatability only. deprecate has field
       */
      val matches =
        for {
          (k, v) <- param.hasFilters
          edgeVal <- propsWithDefault.get(k) if edgeVal.innerVal == v
        } yield (k -> v)
      val ret = if (matches.size == param.hasFilters.size && param.where.map(_.filter(edge)).getOrElse(true)) {
        //      val edge = Edge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), rowKey.labelWithDir, op, ts, version, props)
//              Logger.debug(s"fetchedEdge: $edge")
        Some(edge)
      } else {
        None
      }
      //    Logger.debug(s"$edge")
      //    Logger.debug(s"${cell.getQualifier().toList}, ${ret.map(x => x.toStringRaw)}")
      ret
    }
  }

}
