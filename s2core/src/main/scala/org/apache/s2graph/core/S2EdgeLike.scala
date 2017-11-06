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

trait S2EdgeLike extends Edge with GraphElement {
  this: S2Edge =>

  val builder: S2EdgeBuilder = new S2EdgeBuilder(this)

  val innerGraph: S2Graph
  val srcVertex: S2VertexLike
  var tgtVertex: S2VertexLike
  val innerLabel: Label
  val dir: Int

//  var op: Byte = GraphUtil.defaultOpByte
//  var version: Long = System.currentTimeMillis()
  val propsWithTs: Props = S2Edge.EmptyProps
  val parentEdges: Seq[EdgeWithScore] = Nil
  val originalEdgeOpt: Option[S2EdgeLike] = None
  val pendingEdgeOpt: Option[S2EdgeLike] = None
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

  def serializePropsWithTs(): Array[Byte] = HBaseSerializable.propsToKeyValuesWithTs(propsWithTs.asScala.map(kv => kv._2.labelMeta.seq -> kv._2.innerValWithTs).toSeq)

  def updatePropsWithTs(others: Props = S2Edge.EmptyProps): Props =
    S2Edge.updatePropsWithTs(this, others)

  def propertyValue(key: String): Option[InnerValLikeWithTs] = S2Edge.propertyValue(this, key)

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

  def relatedEdges = builder.relatedEdges

  def srcForVertex = builder.srcForVertex

  def tgtForVertex = builder.tgtForVertex

  def duplicateEdge = builder.duplicateEdge

  //  def reverseDirEdge = copy(labelWithDir = labelWithDir.dirToggled)
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

  def propertyInner[V](key: String, value: V, ts: Long): Property[V] = builder.propertyInner(key, value, ts)

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

  def graph(): Graph = innerGraph

  lazy val edgeId: EdgeId = builder.edgeId

  def id(): AnyRef = edgeId

  def label(): String = innerLabel.label

  def toLogString: String = {
    //    val allPropsWithName = defaultPropsWithName ++ Json.toJson(propsWithName).asOpt[JsObject].getOrElse(Json.obj())
    List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, innerLabel.label, propsWithTs).mkString("\t")
  }
}
