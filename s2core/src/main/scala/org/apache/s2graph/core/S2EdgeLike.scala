package org.apache.s2graph.core
import java.util
import java.util.function.BiConsumer

import org.apache.s2graph.core.JSONParser.innerValToJsValue
import org.apache.s2graph.core.S2Edge.{Props, State}
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, LabelMeta, ServiceColumn}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.structure
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, Graph, Property, T, Vertex}
import play.api.libs.json.Json

import scala.concurrent.Await
import scala.collection.JavaConverters._

trait S2EdgeLike extends Edge {
  this: S2Edge =>

  val innerGraph: S2Graph
  val srcVertex: S2VertexLike
  var tgtVertex: S2VertexLike
  val innerLabel: Label
  val dir: Int

//  var op: Byte = GraphUtil.defaultOpByte
//  var version: Long = System.currentTimeMillis()
  val propsWithTs: Props = S2Edge.EmptyProps
  val parentEdges: Seq[EdgeWithScore] = Nil
  val originalEdgeOpt: Option[S2Edge] = None
  val pendingEdgeOpt: Option[S2Edge] = None
  val statusCode: Byte = 0
  val lockTs: Option[Long] = None
//  var tsInnerValOpt: Option[InnerValLike] = None

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

  def propertyValueInner(labelMeta: LabelMeta): InnerValLikeWithTs = {
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

  lazy val properties = toProps()

  def props = propsWithTs.asScala.mapValues(_.innerVal)

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

  def srcForVertex = {
    val belongLabelIds = Seq(labelWithDir.labelId)
    if (labelWithDir.dir == GraphUtil.directions("in")) {
      val tgtColumn = getServiceColumn(tgtVertex, innerLabel.tgtColumn)
      innerGraph.newVertex(VertexId(tgtColumn, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props, belongLabelIds = belongLabelIds)
    } else {
      val srcColumn = getServiceColumn(srcVertex, innerLabel.srcColumn)
      innerGraph.newVertex(VertexId(srcColumn, srcVertex.innerId), srcVertex.ts, srcVertex.props, belongLabelIds = belongLabelIds)
    }
  }

  def tgtForVertex = {
    val belongLabelIds = Seq(labelWithDir.labelId)
    if (labelWithDir.dir == GraphUtil.directions("in")) {
      val srcColumn = getServiceColumn(srcVertex, innerLabel.srcColumn)
      innerGraph.newVertex(VertexId(srcColumn, srcVertex.innerId), srcVertex.ts, srcVertex.props, belongLabelIds = belongLabelIds)
    } else {
      val tgtColumn = getServiceColumn(tgtVertex, innerLabel.tgtColumn)
      innerGraph.newVertex(VertexId(tgtColumn, tgtVertex.innerId), tgtVertex.ts, tgtVertex.props, belongLabelIds = belongLabelIds)
    }
  }

  def duplicateEdge = reverseSrcTgtEdge.reverseDirEdge

  //  def reverseDirEdge = copy(labelWithDir = labelWithDir.dirToggled)
  def reverseDirEdge = copy(dir = GraphUtil.toggleDir(dir))

  def reverseSrcTgtEdge = copy(srcVertex = tgtVertex, tgtVertex = srcVertex)

  def labelOrders = LabelIndex.findByLabelIdAll(labelWithDir.labelId)

  def isDegree = propsWithTs.containsKey(LabelMeta.degree.name)

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

    propertyInner(LabelMeta.timestamp.name, ts, ts)
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

  def checkProperty(key: String): Boolean = propsWithTs.containsKey(key)

  def copyEdge(srcVertex: S2VertexLike = srcVertex,
               tgtVertex: S2VertexLike = tgtVertex,
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
    edge.propertyInner(LabelMeta.timestamp.name, ts, ts)
    edge
  }

  def copyEdgeWithState(state: State, ts: Long): S2Edge = {
    val newEdge = copy(propsWithTs = S2Edge.EmptyProps)
    S2Edge.fillPropsWithTs(newEdge, state)
    newEdge.propertyInner(LabelMeta.timestamp.name, ts, ts)
    newEdge
  }

  def copyEdgeWithState(state: State): S2Edge = {
    val newEdge = copy(propsWithTs = S2Edge.EmptyProps)
    S2Edge.fillPropsWithTs(newEdge, state)
    newEdge
  }

  def vertices(direction: Direction): util.Iterator[structure.Vertex] = {
    val arr = new util.ArrayList[Vertex]()

    direction match {
      case Direction.OUT =>
        //        val newVertexId = this.direction match {
        //          case "out" => VertexId(innerLabel.srcColumn, srcVertex.innerId)
        //          case "in" => VertexId(innerLabel.tgtColumn, tgtVertex.innerId)
        //          case _ => throw new IllegalArgumentException("direction can only be out/in.")
        //        }
        val newVertexId = edgeId.srcVertexId
        innerGraph.getVertex(newVertexId).foreach(arr.add)
      case Direction.IN =>
        //        val newVertexId = this.direction match {
        //          case "in" => VertexId(innerLabel.srcColumn, srcVertex.innerId)
        //          case "out" => VertexId(innerLabel.tgtColumn, tgtVertex.innerId)
        //          case _ => throw new IllegalArgumentException("direction can only be out/in.")
        //        }
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
    val labelMeta = innerLabel.metaPropsInvMap.getOrElse(key, throw new java.lang.IllegalStateException(s"$key is not configured on Edge."))
    if (propsWithTs.containsKey(key)) propsWithTs.get(key).asInstanceOf[Property[V]]
    else {
      Property.empty()
      //      val default = innerLabel.metaPropsDefaultMapInner(labelMeta)
      //      propertyInner(key, default.innerVal.value, default.ts).asInstanceOf[Property[V]]
    }
  }

  // just for tinkerpop: save to storage, do not use for internal
  def property[V](key: String, value: V): Property[V] = {
    S2Property.assertValidProp(key, value)

    val v = propertyInner(key, value, System.currentTimeMillis())
    val newTs = props.get(LabelMeta.timestamp.name).map(_.toString.toLong + 1).getOrElse(System.currentTimeMillis())
    val newEdge = this.copyEdge(ts = newTs)

    Await.result(innerGraph.mutateEdges(Seq(newEdge), withWait = true), innerGraph.WaitTimeout)

    v
  }

  def propertyInner[V](key: String, value: V, ts: Long): Property[V] = {
    val labelMeta = innerLabel.metaPropsInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on Edge."))
    val newProp = new S2Property[V](this, labelMeta, key, value, ts)
    propsWithTs.put(key, newProp)
    newProp
  }

  def remove(): Unit = {
    if (graph.features().edge().supportsRemoveEdges()) {
      val requestTs = System.currentTimeMillis()
      val edgeToDelete = this.copyEdge(op = GraphUtil.operations("delete"),
        version = version + S2Edge.incrementVersion, propsWithTs = S2Edge.propsToState(updatePropsWithTs()), ts = requestTs)
      // should we delete related edges also?
      val future = innerGraph.mutateEdges(Seq(edgeToDelete), withWait = true)
      val mutateSuccess = Await.result(future, innerGraph.WaitTimeout)
      if (!mutateSuccess.forall(_.isSuccess)) throw new RuntimeException("edge remove failed.")
    } else {
      throw Edge.Exceptions.edgeRemovalNotSupported()
    }
  }

  def graph(): Graph = innerGraph

  lazy val edgeId: EdgeId = {
    // NOTE: xxxForVertex makes direction to be "out"
    val timestamp = if (this.innerLabel.consistencyLevel == "strong") 0l else ts
    //    EdgeId(srcVertex.innerId, tgtVertex.innerId, label(), "out", timestamp)
    val (srcColumn, tgtColumn) = innerLabel.srcTgtColumn(dir)
    if (direction == "out")
      EdgeId(VertexId(srcColumn, srcVertex.id.innerId), VertexId(tgtColumn, tgtVertex.id.innerId), label(), "out", timestamp)
    else
      EdgeId(VertexId(tgtColumn, tgtVertex.id.innerId), VertexId(srcColumn, srcVertex.id.innerId), label(), "out", timestamp)
  }

  def id(): AnyRef = edgeId

  def label(): String = innerLabel.label

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

  private def getServiceColumn(vertex: S2VertexLike, defaultServiceColumn: ServiceColumn) =
    if (vertex.id.column == ServiceColumn.Default) defaultServiceColumn else vertex.id.column

}
