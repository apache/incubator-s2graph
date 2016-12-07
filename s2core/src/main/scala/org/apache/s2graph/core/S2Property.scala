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

import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.{CanInnerValLike, InnerValLikeWithTs}
import org.apache.tinkerpop.gremlin.structure.{Property}

import scala.util.hashing.MurmurHash3

case class S2Property[V](element: S2Edge, labelMeta: LabelMeta, key: String, v: V, ts: Long)
    extends Property[V] {

  import CanInnerValLike._
  lazy val innerVal =
    anyToInnerValLike.toInnerVal(value)(element.innerLabel.schemaVersion)
  lazy val innerValWithTs = InnerValLikeWithTs(innerVal, ts)

  val value = castValue(v, labelMeta.dataType).asInstanceOf[V]

  def bytes: Array[Byte] =
    innerVal.bytes

  def bytesWithTs: Array[Byte] =
    innerValWithTs.bytes

  override def isPresent: Boolean = ???

  override def remove(): Unit = ???

  override def hashCode(): Int =
    MurmurHash3.stringHash(
      labelMeta.labelId + "," + labelMeta.id.get + "," + key + "," + value + "," + ts
    )

  override def equals(other: Any): Boolean = other match {
    case p: S2Property[_] =>
      labelMeta.labelId == p.labelMeta.labelId &&
        labelMeta.seq == p.labelMeta.seq &&
        key == p.key && value == p.value && ts == p.ts
    case _ => false
  }

  override def toString(): String =
    Map("labelMeta" -> labelMeta.toString, "key" -> key, "value" -> value, "ts" -> ts).toString

}
