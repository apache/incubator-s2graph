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

import org.apache.s2graph.core.mysqls.{ColumnMeta, Service, ServiceColumn}
import org.apache.s2graph.core.types.{InnerVal, InnerValLike, SourceVertexId, VertexId}
import play.api.libs.json.Json

case class Vertex(id: VertexId,
                  ts: Long = System.currentTimeMillis(),
                  props: Map[Int, InnerValLike] = Map.empty[Int, InnerValLike],
                  op: Byte = 0,
                  belongLabelIds: Seq[Int] = Seq.empty) extends GraphElement {

  val innerId = id.innerId

  def schemaVer = serviceColumn.schemaVersion

  def serviceColumn = ServiceColumn.findById(id.colId)

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

  //  /** only used by bulk loader */
  //  def buildPuts(): List[Put] = {
  //    //    logger.error(s"put: $this => $rowKey")
  ////    val put = new Put(rowKey.bytes)
  ////    for ((q, v) <- qualifiersWithValues) {
  ////      put.addColumn(vertexCf, q, ts, v)
  ////    }
  ////    List(put)
  //    val kv = kvs.head
  //    val put = new Put(kv.row)
  //    kvs.map { kv =>
  //      put.addColumn(kv.cf, kv.qualifier, kv.timestamp, kv.value)
  //    }
  //    List(put)
  //  }

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
}

object Vertex {

  def toPropKey(labelId: Int): Int = Byte.MaxValue + labelId

  def toLabelId(propKey: Int): Int = propKey - Byte.MaxValue

  def isLabelId(propKey: Int): Boolean = propKey > Byte.MaxValue

  //  val emptyVertex = Vertex(new CompositeId(CompositeId.defaultColId, CompositeId.defaultInnerId, false, true),
  //    System.currentTimeMillis())
  def fromString(s: String): Option[Vertex] = Graph.toVertex(s)
}
