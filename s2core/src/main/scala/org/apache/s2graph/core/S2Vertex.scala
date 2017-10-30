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
import java.util.function.{BiConsumer, Consumer}

import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.S2Vertex.Props
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.structure.Edge.Exceptions
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, Graph, Property, T, Vertex, VertexProperty}
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}

case class S2Vertex(graph: S2Graph,
                  id: VertexId,
                  ts: Long = System.currentTimeMillis(),
                  props: Props = S2Vertex.EmptyProps,
                  op: Byte = 0,
                  belongLabelIds: Seq[Int] = Seq.empty) extends GraphElement with Vertex {

  val innerId = id.innerId

  val innerIdVal = innerId.value

  lazy val properties = for {
    (k, v) <- props.asScala
  } yield v.columnMeta.name -> v.value

  def schemaVer = serviceColumn.schemaVersion

  def serviceColumn = ServiceColumn.findById(id.colId)

  def columnName = serviceColumn.columnName

  lazy val service = Service.findById(serviceColumn.serviceId)

  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, service.hTableName)

  def defaultProps = {
    val default = S2Vertex.EmptyProps
    val newProps = new S2VertexProperty(this, ColumnMeta.lastModifiedAtColumn, ColumnMeta.lastModifiedAtColumn.name, ts)
    default.put(ColumnMeta.lastModifiedAtColumn.name, newProps)
    default
  }

  //  lazy val kvs = Graph.client.vertexSerializer(this).toKeyValues

  /** TODO: make this as configurable */
  override def serviceName = service.serviceName

  override def isAsync = false

  override def queueKey = Seq(ts.toString, serviceName).mkString("|")

  override def queuePartitionKey = id.innerId.toString

  def propsWithName = for {
    (k, v) <- props.asScala
  } yield (v.columnMeta.name -> v.value.toString)

  override def hashCode() = {
    val hash = id.hashCode()
    //    logger.debug(s"Vertex.hashCode: $this -> $hash")
    hash
  }

  override def equals(obj: Any) = {
    obj match {
      case otherVertex: Vertex =>
        val ret = id == otherVertex.id
        //        logger.debug(s"Vertex.equals: $this, $obj => $ret")
        ret
      case _ => false
    }
  }

  override def toString(): String = {
    // V + L_BRACKET + vertex.id() + R_BRACKET;
    // v[VertexId(1, 1481694411514)]
    s"v[${id}]"
  }

  def toLogString(): String = {
    val (serviceName, columnName) =
      if (!id.storeColId) ("", "")
      else (serviceColumn.service.serviceName, serviceColumn.columnName)

    if (propsWithName.nonEmpty)
      Seq(ts, GraphUtil.fromOp(op), "v", id.innerId, serviceName, columnName, Json.toJson(propsWithName)).mkString("\t")
    else
      Seq(ts, GraphUtil.fromOp(op), "v", id.innerId, serviceName, columnName).mkString("\t")
  }

  def copyVertexWithState(props: Props): S2Vertex = {
    val newVertex = copy(props = S2Vertex.EmptyProps)
    S2Vertex.fillPropsWithTs(newVertex, props)
    newVertex
  }

  override def vertices(direction: Direction, edgeLabels: String*): util.Iterator[Vertex] = {
    val arr = new util.ArrayList[Vertex]()
    edges(direction, edgeLabels: _*).forEachRemaining(new Consumer[Edge] {
      override def accept(edge: Edge): Unit = {
        val s2Edge = edge.asInstanceOf[S2Edge]

        s2Edge.direction match {
          case "out" => arr.add(edge.inVertex())
          case "in" => arr.add(edge.outVertex())
          case _ => throw new IllegalStateException("only out/in direction can be found in S2Edge")
        }
//        direction match {
//          case Direction.OUT => arr.add(edge.inVertex())
//          case Direction.IN => arr.add(edge.outVertex())
//          case _ =>
//            val inV = edge.inVertex()
//            val outV = edge.outVertex()
//            if (id != inV.id()) arr.add(inV)
//            if (id != outV.id()) arr.add(outV)
//        }
      }
    })
    arr.iterator()
  }

  override def edges(direction: Direction, labelNames: String*): util.Iterator[Edge] = {
    val labelNameWithDirs =
      if (labelNames.isEmpty) {
        // TODO: Let's clarify direction
        if (direction == Direction.BOTH) {
          Label.findBySrcColumnId(id.colId).map(l => l.label -> Direction.OUT.name) ++
            Label.findByTgtColumnId(id.colId).map(l => l.label -> Direction.IN.name)
        } else if (direction == Direction.IN) {
          Label.findByTgtColumnId(id.colId).map(l => l.label -> direction.name)
        } else {
          Label.findBySrcColumnId(id.colId).map(l => l.label -> direction.name)
        }
      } else {
        direction match {
          case Direction.BOTH =>
            Seq(Direction.OUT, Direction.IN).flatMap { dir => labelNames.map(_ -> dir.name()) }
          case _ => labelNames.map(_ -> direction.name())
        }
      }

    graph.fetchEdges(this, labelNameWithDirs.distinct)
  }

  private def edgesAsync(direction: Direction, labelNames: String*): Future[util.Iterator[Edge]] = {
    val labelNameWithDirs =
      if (labelNames.isEmpty) {
        // TODO: Let's clarify direction
        if (direction == Direction.BOTH) {
          Label.findBySrcColumnId(id.colId).map(l => l.label -> Direction.OUT.name) ++
            Label.findByTgtColumnId(id.colId).map(l => l.label -> Direction.IN.name)
        } else if (direction == Direction.IN) {
          Label.findByTgtColumnId(id.colId).map(l => l.label -> direction.name)
        } else {
          Label.findBySrcColumnId(id.colId).map(l => l.label -> direction.name)
        }
      } else {
        direction match {
          case Direction.BOTH =>
            Seq(Direction.OUT, Direction.IN).flatMap { dir => labelNames.map(_ -> dir.name()) }
          case _ => labelNames.map(_ -> direction.name())
        }
      }

    graph.fetchEdgesAsync(this, labelNameWithDirs.distinct)
  }

  // do no save to storage
  def propertyInner[V](cardinality: Cardinality, key: String, value: V, objects: AnyRef*): VertexProperty[V] = {
    S2Property.assertValidProp(key, value)

    cardinality match {
      case Cardinality.single =>
        val columnMeta = serviceColumn.metasInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on Vertex."))
        val newProps = new S2VertexProperty[V](this, columnMeta, key, value)
        props.put(key, newProps)
        newProps
      case _ => throw new RuntimeException("only single cardinality is supported.")
    }
  }

  override def property[V](cardinality: Cardinality, key: String, value: V, objects: AnyRef*): VertexProperty[V] = {
    S2Property.assertValidProp(key, value)

    cardinality match {
      case Cardinality.single =>
        val columnMeta = serviceColumn.metasInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on Vertex."))
        val newProps = new S2VertexProperty[V](this, columnMeta, key, value)
        props.put(key, newProps)

        // FIXME: save to persistent for tp test
        graph.addVertexInner(this)
        newProps
      case _ => throw new RuntimeException("only single cardinality is supported.")
    }
  }

  override def addEdge(labelName: String, vertex: Vertex, kvs: AnyRef*): Edge = {
    graph.addEdge(this, labelName, vertex, kvs: _*)
  }

  override def property[V](key: String): VertexProperty[V] = {
    if (props.containsKey(key)) {
      props.get(key).asInstanceOf[S2VertexProperty[V]]
    } else {
      VertexProperty.empty()
    }
  }

  override def properties[V](keys: String*): util.Iterator[VertexProperty[V]] = {
    val ls = new util.ArrayList[VertexProperty[V]]()
    if (keys.isEmpty) {
      props.forEach(new BiConsumer[String, VertexProperty[_]] {
        override def accept(key: String, property: VertexProperty[_]): Unit = {
          if (!ColumnMeta.reservedMetaNamesSet(key) && property.isPresent && key != T.id.name)
            ls.add(property.asInstanceOf[VertexProperty[V]])
        }
      })
    } else {
      keys.foreach { key =>
        val prop = property[V](key)
        if (prop.isPresent) ls.add(prop)
      }
    }
    ls.iterator
  }

  override def label(): String = {
    serviceColumn.columnName
//    if (serviceColumn.columnName == Vertex.DEFAULT_LABEL) Vertex.DEFAULT_LABEL // TP3 default vertex label name
//    else {
//      service.serviceName + S2Vertex.VertexLabelDelimiter + serviceColumn.columnName
//    }
  }

  override def remove(): Unit = {
    if (graph.features().vertex().supportsRemoveVertices()) {
      // remove edge
      // TODO: remove related edges also.
      implicit val ec = graph.ec

      val verticesToDelete = Seq(this.copy(op = GraphUtil.operations("delete")))

      val vertexFuture = graph.mutateVertices(verticesToDelete, withWait = true)

      val future = for {
        vertexSuccess <- vertexFuture
        edges <- edgesAsync(Direction.BOTH)
      } yield {
        edges.asScala.toSeq.foreach { edge => edge.remove() }
        if (!vertexSuccess.forall(_.isSuccess)) throw new RuntimeException("Vertex.remove vertex delete failed.")

        true
      }
      Await.result(future, graph.WaitTimeout)

    } else {
      throw Vertex.Exceptions.vertexRemovalNotSupported()
    }
  }
}

object S2Vertex {

  val VertexLabelDelimiter = "::"

  type Props = java.util.Map[String, S2VertexProperty[_]]
  type State = Map[ColumnMeta, InnerValLike]
  def EmptyProps = new java.util.HashMap[String, S2VertexProperty[_]]()
  def EmptyState = Map.empty[ColumnMeta, InnerValLike]

  def toPropKey(labelId: Int): Int = Byte.MaxValue + labelId

  def toLabelId(propKey: Int): Int = propKey - Byte.MaxValue

  def isLabelId(propKey: Int): Boolean = propKey > Byte.MaxValue

  def fillPropsWithTs(vertex: S2Vertex, props: Props): Unit = {
    props.forEach(new BiConsumer[String, S2VertexProperty[_]] {
      override def accept(key: String, p: S2VertexProperty[_]): Unit = {
//        vertex.property(Cardinality.single, key, p.value)
        vertex.propertyInner(Cardinality.single, key, p.value)
      }
    })
  }

  def fillPropsWithTs(vertex: S2Vertex, state: State): Unit = {
    state.foreach { case (k, v) => vertex.propertyInner(Cardinality.single, k.name, v.value) }
  }

  def propsToState(props: Props): State = {
    props.asScala.map { case (k, v) =>
      v.columnMeta -> v.innerVal
    }.toMap
  }

  def stateToProps(vertex: S2Vertex, state: State): Props = {
    state.foreach { case (k, v) =>
      vertex.property(k.name, v.value)
    }
    vertex.props
  }

}
