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

import org.apache.s2graph.core.mysqls.ColumnMeta
import org.apache.s2graph.core.types.CanInnerValLike
import org.apache.tinkerpop.gremlin.structure.{Property, VertexProperty, Vertex => TpVertex}

import scala.util.hashing.MurmurHash3

case class S2VertexPropertyId[V](columnMeta: ColumnMeta, value: V)

case class S2VertexProperty[V](element: S2Vertex,
                               columnMeta: ColumnMeta,
                               key: String,
                               v: V) extends VertexProperty[V] {
  import CanInnerValLike._
  implicit lazy val encodingVer = element.serviceColumn.schemaVersion
  lazy val innerVal = CanInnerValLike.anyToInnerValLike.toInnerVal(value)
  def toBytes: Array[Byte] = {
    innerVal.bytes
  }

  val value = castValue(v, columnMeta.dataType).asInstanceOf[V]

  override def properties[U](strings: String*): util.Iterator[Property[U]] = ???

  override def property[A](key: String, value: A): Property[A] = ???

  override def remove(): Unit = {
    if (!element.graph.features.vertex.properties.supportsRemoveProperty) {
      throw Property.Exceptions.propertyRemovalNotSupported
    }
    isRemoved = true
  }

  override def id(): AnyRef = S2VertexPropertyId(columnMeta, v)

  @volatile var isRemoved = false

  override def isPresent: Boolean = !isRemoved

  override def hashCode(): Int = {
    (element, id()).hashCode()
  }

  override def equals(other: Any): Boolean = other match {
    case p: VertexProperty[_] => element == p.element && id() == p.id()
    case _ => false
  }

  override def toString(): String = {
//    Map("columnMeta" -> columnMeta.toString, "key" -> key, "value" -> value).toString
    s"vp[${key}->${value}]"
  }
}
