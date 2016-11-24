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

case class S2VertexProperty[V](element: TpVertex,
                               columnMeta: ColumnMeta,
                               key: String,
                               value: V) extends VertexProperty[V] {
  implicit val encodingVer = columnMeta.serviceColumn.schemaVersion
  val innerVal = CanInnerValLike.anyToInnerValLike.toInnerVal(value)
  def toBytes: Array[Byte] = {
    innerVal.bytes
  }

  override def properties[U](strings: String*): util.Iterator[Property[U]] = ???

  override def property[V](s: String, v: V): Property[V] = ???

  override def remove(): Unit = ???

  override def id(): AnyRef = ???

  override def isPresent: Boolean = ???
}
