package com.daumkakao.s2graph.core

//import com.daumkakao.s2graph.core.mysqls._

import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.types2._
import play.api.libs.json.Json
import scala.concurrent.Future
import org.apache.hadoop.hbase.client.{Increment, Delete, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async._
import play.api.Logger
import scala.collection.mutable.ListBuffer


case class EdgeWithIndexInverted(srcVertex: Vertex,
                                 tgtVertex: Vertex,
                                 labelWithDir: LabelWithDirection,
                                 op: Byte,
                                 version: Long,
                                 props: Map[Byte, InnerValLikeWithTs]) {


  import GraphConstant._
  import Edge._

  //  Logger.error(s"EdgeWithIndexInverted${this.toString}")
  lazy val lastModifiedAt = if (props.isEmpty) 0L else props.map(_._2.ts).max
  lazy val schemaVer = label.schemaVersion
  lazy val rowKey = EdgeRowKey(VertexId.toSourceVertexId(srcVertex.id),
    labelWithDir, LabelIndex.defaultSeq, isInverted = true)(version = schemaVer)

  lazy val qualifier = EdgeQualifierInverted(VertexId.toTargetVertexId(tgtVertex.id))(version = schemaVer)
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

  def buildDeleteAsync() = {
    val ret = new DeleteRequest(label.hbaseTableName.getBytes, rowKey.bytes, edgeCf, qualifier.bytes, version)
    Logger.debug(s"$ret, $version")
    ret
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

  lazy val rowKey = EdgeRowKey(VertexId.toSourceVertexId(srcVertex.id), labelWithDir, labelIndexSeq, isInverted = false)(schemaVer)
  lazy val qualifier = EdgeQualifier(orders, VertexId.toTargetVertexId(tgtVertex.id), op)(label.schemaVersion)
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

  def buildIncrements(amount: Long = 1L): List[Increment] = {
    //    if (!hasAllPropsForIndex) {
    //      Logger.error(s"$this dont have all props for index")
    //      List.empty[Increment]
    //    } else {

    val increment = new Increment(rowKey.bytes)
    increment.addColumn(edgeCf, Array.empty[Byte], amount)
    List(increment)
    //    }
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

  def buildDegreeDeletesAsync(): List[HBaseRpc] = {
    if (!hasAllPropsForIndex) List.empty[DeleteRequest]
    else {
      List(new DeleteRequest(label.hbaseTableName.getBytes, rowKey.bytes, edgeCf, Array.empty[Byte], ts))
    }
  }

}

/**
 *
 */
case class Edge(srcVertex: Vertex,
                tgtVertex: Vertex,
                labelWithDir: LabelWithDirection,
                op: Byte = GraphUtil.operations("insert"),
                ts: Long = System.currentTimeMillis(),
                version: Long = System.currentTimeMillis(),
                propsWithTs: Map[Byte, InnerValLikeWithTs] = Map.empty[Byte, InnerValLikeWithTs])
  extends GraphElement with JSONParser {


  import Edge._

  implicit val ex = Graph.executionContext
  lazy val schemaVer = label.schemaVersion
  lazy val props =
    if (op == GraphUtil.operations("delete")) Map(LabelMeta.timeStampSeq -> InnerVal.withLong(ts, schemaVer))
    else for ((k, v) <- propsWithTs) yield (k -> v.innerVal)

  lazy val relatedEdges = {
    labelWithDir.dir match {
      case 2 => //undirected
        val out = LabelWithDirection(labelWithDir.labelId, GraphUtil.directions("out"))
        val base = Edge(srcVertex, tgtVertex, out, op, ts, version, propsWithTs)
        List(base, base.reverseSrcTgtEdge)
      case 0 | 1 =>
        List(this, duplicateEdge)
    }
  }
  //  assert(srcVertex.isInstanceOf[SourceVertexId] && tgtVertex.isInstanceOf[TargetVertexId])

  lazy val srcForVertex = {
    if (labelWithDir.dir == GraphUtil.directions("in")) {
      Vertex(VertexId(label.tgtColumn.id.get, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props)
    } else {
      Vertex(VertexId(label.srcColumn.id.get, srcVertex.innerId), srcVertex.ts, srcVertex.props)
    }
  }
  lazy val tgtForVertex = {
    if (labelWithDir.dir == GraphUtil.directions("in")) {
      Vertex(VertexId(label.srcColumn.id.get, srcVertex.innerId), srcVertex.ts, srcVertex.props)
    } else {
      Vertex(VertexId(label.tgtColumn.id.get, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props)
    }
  }


  lazy val duplicateEdge = Edge(tgtVertex, srcVertex, labelWithDir.dirToggled, op, ts, version, propsWithTs)
  lazy val reverseDirEdge = Edge(srcVertex, tgtVertex, labelWithDir.dirToggled, op, ts, version, propsWithTs)
  lazy val reverseSrcTgtEdge = Edge(tgtVertex, srcVertex, labelWithDir, op, ts, version, propsWithTs)

  lazy val label = Label.findById(labelWithDir.labelId)
  lazy val labelOrders = LabelIndex.findByLabelIdAll(labelWithDir.labelId)

  override lazy val serviceName = label.serviceName
  override lazy val queueKey = Seq(ts.toString, tgtVertex.serviceName).mkString("|")
  override lazy val queuePartitionKey = Seq(srcVertex.innerId, tgtVertex.innerId).mkString("|")
  override lazy val isAsync = label.isAsync

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

  def toInvertedEdgeHashLike(): EdgeWithIndexInverted = {
    //    val (smaller, larger) =
    //      if (srcForVertex.innerId == tgtForVertex.innerId) {
    //        if (srcForVertex.id.colId <= tgtForVertex.id.colId) {
    //          (srcForVertex, tgtForVertex)
    //        } else {
    //          (tgtForVertex, srcForVertex)
    //        }
    //      } else if (srcForVertex.innerId < tgtForVertex.innerId) {
    //        (srcForVertex, tgtForVertex)
    //      } else {
    //        (tgtForVertex, srcForVertex)
    //      }
    val (smaller, larger) = (srcForVertex, tgtForVertex)
    //      if (srcVertex.id.colId == VertexId.DEFAULT_COL_ID) {
    //        (srcForVertex, tgtForVertex)
    //      } else {
    //        if (srcVertex.id.colId == label.srcColumn.id.get) {
    //          (srcVertex, tgtVertex)
    //        } else {
    //          (tgtVertex, srcVertex)
    //        }
    //      }

    Logger.debug(s"$smaller, ${smaller.innerId.bytes.toList}, $larger, ${larger.innerId.bytes.toList}")
    /** force direction as out on invertedEdge */
    val newLabelWithDir = LabelWithDirection(labelWithDir.labelId, GraphUtil.directions("out"))

    val ret = EdgeWithIndexInverted(smaller, larger, newLabelWithDir, op, version, propsWithTs ++
      Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(ts, schemaVer), ts)))
    ret
  }

  lazy val edgesWithInvertedIndex = {
    this.toInvertedEdgeHashLike()
    //    EdgeWithIndexInverted(srcVertex, tgtVertex, labelWithDir, op, version, propsWithTs ++
    //      Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(ts, schemaVer), ts)))
  }

  def edgesWithInvertedIndex(newOp: Byte) = {
    this.toInvertedEdgeHashLike()
    //    EdgeWithIndexInverted(srcVertex, tgtVertex, labelWithDir, newOp, version, propsWithTs ++
    //      Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(ts, schemaVer), ts)))
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
    val edgePuts = {
      if (op == GraphUtil.operations("insert")) {
        if (label.consistencyLevel == "strong") {
          upsert()
          List.empty[PutRequest]
        } else {
          insert()
        }
      } else if (op == GraphUtil.operations("delete")) {
        delete()
        List.empty[PutRequest]
      } else if (op == GraphUtil.operations("update")) {
        update()
        List.empty[PutRequest]
      } else if (op == GraphUtil.operations("increment")) {
        increment()
        List.empty[PutRequest]
      } else if (op == GraphUtil.operations("insertBulk")) {
        insert()
      } else {
        throw new Exception(s"operation[${op}] is not supported on edge.")
      }

    }
    val ret = edgePuts ++ buildVertexPutsAsync
    //    Logger.debug(s"$this, $ret")
    ret
  }

  def insertBulk() = {
    val puts = edgesWithInvertedIndex.buildPut() :: relatedEdges.flatMap { relEdge =>
      relEdge.edgesWithIndex.flatMap(e => e.buildPuts())
    }
    puts
  }

  def insert() = {
    val puts = edgesWithInvertedIndex.buildPutAsync() :: relatedEdges.flatMap { relEdge =>
      relEdge.edgesWithIndex.flatMap(e => e.buildPutsAsync)
    }
    val incrs = relatedEdges.flatMap { relEdge =>
      relEdge.edgesWithIndex.flatMap(e => e.buildIncrementsAsync())
    }

    val rets = puts ++ incrs
    Logger.debug(s"$rets")
    rets
  }

  def buildDeleteBulk() = {
    toInvertedEdgeHashLike().buildDeleteAsync() :: edgesWithIndex.flatMap { e => e.buildDeletesAsync() ++
      e.buildDegreeDeletesAsync()
    }
  }

  /**
   * write must be synchronous to keep consistencyLevel
   */
  //  def fetchInverted() = {
  //    Graph.getEdgeSync(srcVertex, tgtVertex, label, labelWithDir.dir)
  //  }
  def fetchInvertedAsync(): Future[(QueryParam, Option[Edge])] = {
    val queryParam = QueryParam(labelWithDir)
    Graph.getEdge(srcVertex, tgtVertex, queryParam).map { case queryResult =>
      (queryParam, queryResult.edgeWithScoreLs.headOption.map { edgeWithScore => edgeWithScore._1 })
    }
  }

  /**
   *
   */

  def compareAndSet(client: HBaseClient)(invertedEdgeOpt: Option[Edge], edgeUpdate: EdgeUpdate): Future[Boolean] = {
    val expected = invertedEdgeOpt.map { e =>
      e.edgesWithInvertedIndex.value.bytes
    }.getOrElse(Array.empty[Byte])
    val futures = edgeUpdate.invertedEdgeMutations.map { newPut =>
      Graph.defferedToFuture(client.compareAndSet(newPut, expected))(false)
    }

    for {
      rets <- Future.sequence(futures)
    } yield {
      if (rets.forall(x => x)) {
        Graph.writeAsync(label.hbaseZkAddr, Seq(edgeUpdate.indexedEdgeMutations))
        /** degree */
        val incrs =
          (edgeUpdate.edgesToDelete.isEmpty, edgeUpdate.edgesToInsert.isEmpty) match {
            case (true, true) =>

              /** when there is no need to update. shouldUpdate == false */
              List.empty[AtomicIncrementRequest]
            case (true, false) =>

              /** no edges to delete but there is new edges to insert so increase degree by 1 */
              this.relatedEdges.flatMap { relEdge =>
                relEdge.edgesWithIndexValid.flatMap(e => e.buildIncrementsAsync())
              }
            //              this.edgesWithIndexValid.flatMap(e => e.buildIncrementsAsync())
            case (false, true) =>

              /** no edges to insert but there is old edges to delete so decrease degree by 1 */
              this.relatedEdges.flatMap { relEdge =>
                relEdge.edgesWithIndexValid.flatMap(e => e.buildIncrementsAsync(-1L))
              }
            //              this.edgesWithIndexValid.flatMap(e => e.buildIncrementsAsync(-1L))
            case (false, false) =>

              /** update on existing edges so no change on degree */
              List.empty[AtomicIncrementRequest]
          }
        //        Logger.debug(s"Increment: $incrs")
        Graph.writeAsync(label.hbaseZkAddr, Seq(incrs))
        true
      } else {
        false
      }
    }
    //    }
  }


  def mutate(f: (Option[Edge], Edge) => EdgeUpdate, tryNum: Int = 0): Unit = {
    if (tryNum >= maxTryNum) {
      Logger.error(s"mutate failed after $tryNum retry")
      throw new RuntimeException(s"mutate failed after $tryNum")
    } else {
      try {
        val client = Graph.getClient(label.hbaseZkAddr)
        for {
          (queryParam, edges) <- fetchInvertedAsync()
          invertedEdgeOpt = edges.headOption
          edgeUpdate = f(invertedEdgeOpt, this)
          ret <- compareAndSet(client)(invertedEdgeOpt, edgeUpdate)
        } {
          /**
           * we can use CAS operation on inverted Edge checkAndSet() and if it return false
           * then re-read and build update and write recursively.
           */
          if (ret) {
            Logger.debug(s"mutate successed. ${this.toLogString()}, ${edgeUpdate.toLogString()}")
          } else {
            Logger.info(s"mutate failed. retry")
            mutate(f, tryNum + 1)
          }

        }
        //      client.flush()
      } catch {
        case e: Throwable =>
          Logger.error(s"mutate failed. $e", e)
      }
    }
  }

  def upsert(): Unit = {
    mutate(Edge.buildUpsert)
  }

  def delete() = {
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
                val cost = try {
                  BigDecimal(innerVal.toString).toDouble
                } catch {
                  case e: Throwable => 1.0
                }
                sum += w * cost
              }
            }
          }
        }
      }
      sum
    }
  }

  def toLogString(): String = {
    val ls = ListBuffer(ts, GraphUtil.fromOp(op), "e",
      srcVertex.innerId, tgtVertex.innerId, label.label)
    if (!propsWithName.isEmpty) ls += Json.toJson(propsWithName)
    ls.mkString("\t")
  }

}

case class EdgeUpdate(indexedEdgeMutations: List[HBaseRpc] = List.empty[HBaseRpc],
                      invertedEdgeMutations: List[PutRequest] = List.empty[PutRequest],
                      edgesToDelete: List[EdgeWithIndex] = List.empty[EdgeWithIndex],
                      edgesToInsert: List[EdgeWithIndex] = List.empty[EdgeWithIndex],
                      newInvertedEdge: Option[EdgeWithIndexInverted] = None) {

  def toLogString(): String = {
    val indexedEdgeSize = s"indexedEdgeMutationSize: ${indexedEdgeMutations.size}"
    val invertedEdgeSize = s"invertedEdgeMutationSize: ${invertedEdgeMutations.size}"
    val deletes = s"deletes: ${edgesToDelete.map(e => e.toString).mkString("\n")}"
    val inserts = s"inserts: ${edgesToInsert.map(e => e.toString).mkString("\n")}"
    val updates = s"snapshot: $newInvertedEdge"
    List(indexedEdgeSize, invertedEdgeSize, deletes, inserts, updates).mkString("\n")
  }
}

object Edge extends JSONParser {

  //  val initialVersion = 2L
  val incrementVersion = 1L
  val minOperationTs = 1L
  val minTsVal = 0L
  // FIXME:
  val maxTryNum = 10
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
    //            Logger.debug(s"oldEdge: ${invertedEdge.map(_.toStringRaw)}")
    //            Logger.debug(s"requestEdge: ${requestEdge.toStringRaw}")

    val oldPropsWithTs = if (invertedEdge.isEmpty) Map.empty[Byte, InnerValLikeWithTs] else invertedEdge.get.propsWithTs
    val minTsInOldProps = if (invertedEdge.isEmpty) minTsVal else invertedEdge.get.propsWithTs.map(kv => kv._2.ts).min

    val oldTs = invertedEdge.map(e => e.ts).getOrElse(minTsVal)

    if (oldTs == requestEdge.ts) {
      Logger.info(s"duplicate timestamp on same edge. $requestEdge")
      EdgeUpdate()
    } else {
      val (newPropsWithTs, shouldReplace) =
        f(oldPropsWithTs, requestEdge.propsWithTs, requestEdge.ts, requestEdge.schemaVer)

      if (!shouldReplace) {
        Logger.info(s"drop request $requestEdge becaseu shouldReplace is $shouldReplace")
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

  /**
   * delete invertedEdge.edgesWithIndex
   * insert requestEdge.edgesWithIndex
   * update requestEdge.edgesWithIndexInverted
   */
  def buildReplace(invertedEdge: Option[Edge], requestEdge: Edge, newPropsWithTs: Map[Byte, InnerValLikeWithTs]): EdgeUpdate = {

    val edgesToDelete = invertedEdge match {
      case Some(e) if e.op != GraphUtil.operations("delete") =>
        e.relatedEdges.flatMap { relEdge => relEdge.edgesWithIndexValid }
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
          requestEdge.relatedEdges.flatMap { relEdge =>
            relEdge.edgesWithIndexValid(GraphUtil.defaultOpByte)
          }
        }
      }
    }
    val edgeInverted = if (newPropsWithTs.isEmpty) None else Some(requestEdge.edgesWithInvertedIndex)

    val deleteMutations = edgesToDelete.flatMap(edge => edge.buildDeletesAsync)
    val insertMutations = edgesToInsert.flatMap(edge => edge.buildPutsAsync)
    val invertMutations = edgeInverted.map(e => List(e.buildPutAsync)).getOrElse(List.empty[PutRequest])
    val indexedEdgeMutations = deleteMutations ++ insertMutations
    val invertedEdgeMutations = invertMutations

    val update = EdgeUpdate(indexedEdgeMutations, invertedEdgeMutations, edgesToDelete, edgesToInsert, edgeInverted)

    //        Logger.debug(s"UpdatedProps: ${newPropsWithTs}\n")
    //        Logger.debug(s"EdgeUpdate: $update\n")
    //    Logger.debug(s"$update")
    update
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


  def fromString(s: String): Option[Edge] = Graph.toEdge(s)

  def toEdge(kv: org.hbase.async.KeyValue, param: QueryParam): Option[Edge] = {
    Logger.debug(s"$param -> $kv")

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
          //FIXME: dirty hack. dummy target vertexId
          val ts = kv.timestamp()
          val dummyProps = Map(LabelMeta.degreeSeq -> InnerValLikeWithTs.withLong(degree, ts, param.label.schemaVersion))
          val tgtVertexId = VertexId(VertexId.DEFAULT_COL_ID, InnerVal.withStr("0", param.label.schemaVersion))
          (tgtVertexId, dummyProps, GraphUtil.operations("insert"), ts)
        } else {
          /** edge */
          val qualifier = EdgeQualifier.fromBytes(kvQual, 0, kvQual.length, param.label.schemaVersion)
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
      val edge =
//        if (!param.label.isDirected && param.labelWithDir.dir == GraphUtil.directions("in")) {
//          Edge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), rowKey.labelWithDir.updateDir(0), op, ts, version, props)
//        } else {
          Edge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), rowKey.labelWithDir, op, ts, version, props)
//        }

      //          Logger.debug(s"toEdge: $srcVertexId, $tgtVertexId, $props, $op, $ts")
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

  //FIXME
  def buildIncrementDegreeBulk(srcVertexId: String, labelName: String, direction: String,
                               degreeVal: Long) = {
    for {
      label <- Label.findByName(labelName)
      dir <- GraphUtil.toDir(direction)
      labelWithDir = LabelWithDirection(label.id.get, dir)
      jsValue = Json.toJson(srcVertexId)
      innerVal <- jsValueToInnerVal(jsValue, label.srcColumnWithDir(dir).columnType, label.schemaVersion)
      vertexId = SourceVertexId(label.srcColumn.id.get, innerVal)
      vertex = Vertex(vertexId)
      labelWithDir = LabelWithDirection(label.id.get, GraphUtil.toDirection(direction))
      edge = Edge(vertex, vertex, labelWithDir)
    } yield {
      for {
        edgeWithIndex <- edge.edgesWithIndex
        incr <- edgeWithIndex.buildIncrements(degreeVal)
      } yield {
        incr
      }
    }
  }

}
