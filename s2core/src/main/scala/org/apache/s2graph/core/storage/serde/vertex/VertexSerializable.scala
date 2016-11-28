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

package org.apache.s2graph.core.storage.serde.vertex

import org.apache.s2graph.core.S2Vertex
import org.apache.s2graph.core.storage.StorageSerializable._
import org.apache.s2graph.core.storage.{SKeyValue, Serializable}
import scala.collection.JavaConverters._

case class VertexSerializable(vertex: S2Vertex, intToBytes: Int => Array[Byte] = intToBytes) extends Serializable[S2Vertex] {

  override val table = vertex.hbaseTableName.getBytes
  override val ts = vertex.ts
  override val cf = Serializable.vertexCf

  override def toRowKey: Array[Byte] = vertex.id.bytes

  override def toQualifier: Array[Byte] = Array.empty[Byte]
  override def toValue: Array[Byte] = Array.empty[Byte]

  /** vertex override toKeyValues since vertex expect to produce multiple sKeyValues */
  override def toKeyValues: Seq[SKeyValue] = {
    val row = toRowKey
    val base = for ((k, v) <- vertex.props.asScala ++ vertex.defaultProps.asScala) yield {
      val columnMeta = v.columnMeta
      intToBytes(columnMeta.seq) -> v.innerVal.bytes
    }
    val belongsTo = vertex.belongLabelIds.map { labelId => intToBytes(S2Vertex.toPropKey(labelId)) -> Array.empty[Byte] }
    (base ++ belongsTo).map { case (qualifier, value) =>
      SKeyValue(vertex.hbaseTableName.getBytes, row, cf, qualifier, value, vertex.ts)
    } toSeq
  }
}