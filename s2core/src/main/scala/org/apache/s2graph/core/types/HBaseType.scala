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

package org.apache.s2graph.core.types

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.schema.LabelMeta

object HBaseType {
  val VERSION4 = "v4"
  val VERSION3 = "v3"
  val VERSION2 = "v2"
  val ValidVersions = List(VERSION4, VERSION3, VERSION2)
//  val VERSION1 = "v1"
//  val DEFAULT_VERSION = VERSION2
  val DEFAULT_VERSION = VERSION3
//  val EMPTY_SEQ_BYTE = Byte.MaxValue
  val DEFAULT_COL_ID = 0
  val bitsForDir = 2
  val maxBytes = Bytes.toBytes(Int.MaxValue)
  val toSeqByte = -5.toByte
  val defaultTgtVertexId = null
}

object HBaseDeserializable {

  import HBaseType._

  // 6 bits is used for index sequence so total index per label is limited to 2^6
  def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }

  def bytesToKeyValues(bytes: Array[Byte],
                       offset: Int,
                       length: Int,
                       version: String = DEFAULT_VERSION): (Array[(Byte, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = bytes(pos)
      pos += 1
      val (v, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, 0, version)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    val ret = (kvs, pos)
    //    logger.debug(s"bytesToProps: $ret")
    ret
  }

  def bytesToKeyValuesWithTs(bytes: Array[Byte],
                             offset: Int,
                             version: String = DEFAULT_VERSION): (Array[(Byte, InnerValLikeWithTs)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLikeWithTs)](len)
    var i = 0
    while (i < len) {
      val k = bytes(pos)
      pos += 1
      val (v, numOfBytesUsed) = InnerValLikeWithTs.fromBytes(bytes, pos, 0, version)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    val ret = (kvs, pos)
    //    logger.debug(s"bytesToProps: $ret")
    ret
  }

  def bytesToProps(bytes: Array[Byte],
                   offset: Int,
                   version: String = DEFAULT_VERSION): (Array[(Byte, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = LabelMeta.emptySeq
      val (v, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, 0, version)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    //    logger.error(s"bytesToProps: $kvs")
    val ret = (kvs, pos)

    ret
  }
}

object HBaseSerializable {
  def propsToBytes(props: Seq[(Byte, InnerValLike)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, v.bytes)
    bytes
  }

  def propsToKeyValues(props: Seq[(Byte, InnerValLike)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
    bytes
  }

  def propsToKeyValuesWithTs(props: Seq[(Byte, InnerValLikeWithTs)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
    bytes
  }

  def labelOrderSeqWithIsInverted(labelOrderSeq: Byte, isInverted: Boolean): Array[Byte] = {
    assert(labelOrderSeq < (1 << 6))
    val byte = labelOrderSeq << 1 | (if (isInverted) 1 else 0)
    Array.fill(1)(byte.toByte)
  }
}

trait HBaseSerializable {
  def bytes: Array[Byte]
}

trait HBaseDeserializable {

  import HBaseType._

  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (HBaseSerializable, Int)

  //  def fromBytesWithIndex(bytes: Array[Byte],
  //                offset: Int,
  //                len: Int,
  //                version: String = DEFAULT_VERSION): (HBaseSerializable, Int)
  def notSupportedEx(version: String) = new RuntimeException(s"not supported version, $version")
}

trait HBaseDeserializableWithIsVertexId {

  import HBaseType._

  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION,
                isVertexId: Boolean = false): (HBaseSerializable, Int)

  def notSupportedEx(version: String) = new RuntimeException(s"not supported version, $version")
}
