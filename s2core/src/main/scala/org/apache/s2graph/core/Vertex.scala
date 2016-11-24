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

import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.mysqls.{ColumnMeta, Service, ServiceColumn}
import org.apache.s2graph.core.types.{InnerVal, InnerValLike, SourceVertexId, VertexId}
import org.apache.tinkerpop.gremlin.structure
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure.{Vertex => TpVertex, Direction, Edge, VertexProperty, Graph}
import play.api.libs.json.Json

case class Vertex(id: VertexId,
                  ts: Long = System.currentTimeMillis(),
                  props: Map[Int, InnerValLike] = Map.empty[Int, InnerValLike],
                  op: Byte = 0,
                  belongLabelIds: Seq[Int] = Seq.empty) extends GraphElement with TpVertex {

  val innerId = id.innerId

  val innerIdVal = innerId.value

  lazy val properties = for {
    (k, v) <- props
    meta <- serviceColumn.metasMap.get(k)
  } yield meta.name -> v.value

  def schemaVer = serviceColumn.schemaVersion

  def serviceColumn = ServiceColumn.findById(id.colId)

  def columnName = serviceColumn.columnName

  def service = Service.findById(serviceColumn.serviceId)

  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, service.hTableName)

  def defaultProps = Map(ColumnMeta.lastModifiedAtColumnSeq.toInt -> InnerVal.withLong(ts, schemaVer))

  //  lazy val kvs = Graph.client.vertexSerializer(this).toKeyValues

  /** TODO: make this as configurable */
  override def serviceName = service.serviceName

  override def isAsync = false

  override def queueKey = Seq(ts.toString, serviceName).mkString("|")

  override def queuePartitionKey = id.innerId.toString

  def propsWithName = for {
    (seq, v) <- props
    meta <- ColumnMeta.findByIdAndSeq(id.colId, seq.toByte)
  } yield (meta.name -> v.toString)

  def toEdgeVertex() = Vertex(SourceVertexId(id.colId, innerId), ts, props, op)


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

  def withProps(newProps: Map[Int, InnerValLike]) = Vertex(id, ts, newProps, op)

  def toLogString(): String = {
    val (serviceName, columnName) =
      if (!id.storeColId) ("", "")
      else (serviceColumn.service.serviceName, serviceColumn.columnName)

    if (propsWithName.nonEmpty)
      Seq(ts, GraphUtil.fromOp(op), "v", id.innerId, serviceName, columnName, Json.toJson(propsWithName)).mkString("\t")
    else
      Seq(ts, GraphUtil.fromOp(op), "v", id.innerId, serviceName, columnName).mkString("\t")
  }

  override def vertices(direction: Direction, strings: String*): util.Iterator[TpVertex] = ???

  override def edges(direction: Direction, strings: String*): util.Iterator[structure.Edge] = ???

  override def property[V](cardinality: Cardinality, s: String, v: V, objects: AnyRef*): VertexProperty[V] = ???

  override def addEdge(s: String, vertex: TpVertex, objects: AnyRef*): Edge = ???

  override def properties[V](strings: String*): util.Iterator[VertexProperty[V]] = ???

  override def remove(): Unit = ???

  override def graph(): Graph = ???

  override def label(): String = ???
}

object Vertex {

  def toPropKey(labelId: Int): Int = Byte.MaxValue + labelId

  def toLabelId(propKey: Int): Int = propKey - Byte.MaxValue

  def isLabelId(propKey: Int): Boolean = propKey > Byte.MaxValue

  def toVertex(serviceName: String,
            columnName: String,
            id: Any,
            props: Map[String, Any] = Map.empty,
            ts: Long = System.currentTimeMillis(),
            operation: String = "insert"): Vertex = {

    val service = Service.findByName(serviceName).getOrElse(throw new RuntimeException(s"$serviceName is not found."))
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse(throw new RuntimeException(s"$columnName is not found."))
    val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

    val srcVertexId = VertexId(column.id.get, toInnerVal(id.toString, column.columnType, column.schemaVersion))
    val propsInner = column.propsToInnerVals(props) ++
      Map(ColumnMeta.timeStampSeq.toInt -> InnerVal.withLong(ts, column.schemaVersion))

    new Vertex(srcVertexId, ts, propsInner, op)
  }
}
