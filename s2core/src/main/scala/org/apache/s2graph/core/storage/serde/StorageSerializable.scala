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

package org.apache.s2graph.core.storage.serde

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.schema.{ColumnMeta, LabelMeta}
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.core.types.{InnerValLike, InnerValLikeWithTs}

object StorageSerializable {
  /** serializer */
  def propsToBytes(props: Seq[(LabelMeta, InnerValLike)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((_, v) <- props) bytes = Bytes.add(bytes, v.bytes)
    bytes
  }

  def vertexPropsToBytes(props: Seq[(ColumnMeta, Array[Byte])]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Bytes.toBytes(k.seq.toInt), v)
    bytes
  }

  def propsToKeyValues(props: Seq[(LabelMeta, InnerValLike)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k.seq), v.bytes)
    bytes
  }

  def propsToKeyValuesWithTs(props: Seq[(LabelMeta, InnerValLikeWithTs)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k.seq), v.bytes)
    bytes
  }

  def labelOrderSeqWithIsInverted(labelOrderSeq: Byte, isInverted: Boolean): Array[Byte] = {
    assert(labelOrderSeq < (1 << 6))
    val byte = labelOrderSeq << 1 | (if (isInverted) 1 else 0)
    Array.fill(1)(byte.toByte)
  }

  def intToBytes(value: Int): Array[Byte] = Bytes.toBytes(value)

  def longToBytes(value: Long): Array[Byte] = Bytes.toBytes(value)
}

trait StorageSerializable[E] {
  val cf = Serializable.edgeCf

  def table: Array[Byte]
  def ts: Long

  def toRowKey: Array[Byte]
  def toQualifier: Array[Byte]
  def toValue: Array[Byte]

  def toKeyValues: Seq[SKeyValue] = {
    val row = toRowKey
    val qualifier = toQualifier
    val value = toValue
    val kv = SKeyValue(table, row, cf, qualifier, value, ts)
//    logger.debug(s"[SER]: ${kv.toLogString}}")
    Seq(kv)
  }
}
