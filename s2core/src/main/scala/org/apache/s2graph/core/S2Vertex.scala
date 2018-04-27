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

import java.util.function.BiConsumer

import org.apache.s2graph.core.S2Vertex.Props
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.types._
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

import scala.collection.JavaConverters._

case class S2Vertex(graph: S2GraphLike,
                    id: VertexId,
                    ts: Long = System.currentTimeMillis(),
                    props: Props = S2Vertex.EmptyProps,
                    op: Byte = 0,
                    belongLabelIds: Seq[Int] = Seq.empty) extends S2VertexLike {

  //  lazy val kvs = Graph.client.vertexSerializer(this).toKeyValues

  /** TODO: make this as configurable */
  override def serviceName = service.serviceName

  override def isAsync = false

  override def queueKey = Seq(ts.toString, serviceName).mkString("|")

  override def queuePartitionKey = id.innerId.toString

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

  def fillPropsWithTs(vertex: S2VertexLike, props: Props): Unit = {
    props.forEach(new BiConsumer[String, S2VertexProperty[_]] {
      override def accept(key: String, p: S2VertexProperty[_]): Unit = {
//        vertex.property(Cardinality.single, key, p.value)
        vertex.propertyInner(Cardinality.single, key, p.value)
      }
    })
  }

  def fillPropsWithTs(vertex: S2VertexLike, state: State): Unit = {
    state.foreach { case (k, v) => vertex.propertyInner(Cardinality.single, k.name, v.value) }
  }

  def propsToState(props: Props): State = {
    props.asScala.map { case (k, v) =>
      v.columnMeta -> v.innerVal
    }.toMap
  }

  def stateToProps(vertex: S2VertexLike, state: State): Props = {
    state.foreach { case (k, v) =>
      vertex.property(k.name, v.value)
    }
    vertex.props
  }

}
