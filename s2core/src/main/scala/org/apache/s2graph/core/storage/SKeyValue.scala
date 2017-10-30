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

package org.apache.s2graph.core.storage

import java.nio.charset.StandardCharsets
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue


object SKeyValue {
  val EdgeCf = "e".getBytes("UTF-8")
  val VertexCf = "v".getBytes("UTF-8")
  val Put = 1
  val Delete = 2
  val Increment = 3
  val Default = Put
}

case class SKeyValue(table: Array[Byte],
                     row: Array[Byte],
                     cf: Array[Byte],
                     qualifier: Array[Byte],
                     value: Array[Byte],
                     timestamp: Long,
                     operation: Int = SKeyValue.Default,
                     durability: Boolean = true) {
  def toLogString = {
    Map("table" -> Bytes.toString(table), "row" -> row.toList, "cf" -> Bytes.toString(cf),
      "qualifier" -> qualifier.toList, "value" -> value.toList, "timestamp" -> timestamp,
      "operation" -> operation, "durability" -> durability).toString()
  }
  override def toString(): String = toLogString

  def toKeyValue: KeyValue = new KeyValue(row, cf, qualifier, timestamp, value)
}

trait CanSKeyValue[T] {
  def toSKeyValue(from: T): SKeyValue
}

object CanSKeyValue {
  def instance[T](f: T => SKeyValue): CanSKeyValue[T] = new CanSKeyValue[T] {
    override def toSKeyValue(from: T): SKeyValue = f.apply(from)
  }

  // For asyncbase KeyValues
  implicit val asyncKeyValue = instance[KeyValue] { kv =>
    SKeyValue(Array.empty[Byte], kv.key(), kv.family(), kv.qualifier(), kv.value(), kv.timestamp())
  }

  // For asyncbase KeyValues
  implicit val sKeyValue = instance[SKeyValue](identity)

  // For hbase KeyValues
}

