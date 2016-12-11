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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.QueryParam
import org.apache.s2graph.core.mysqls.{LabelMeta, Label}
import org.apache.s2graph.core.types.{HBaseType, InnerVal, InnerValLike, InnerValLikeWithTs}
import org.apache.s2graph.core.utils.logger

object StorageDeserializable {
  /** Deserializer */
  def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }

  def bytesToKeyValues(bytes: Array[Byte],
                       offset: Int,
                       length: Int,
                       schemaVer: String,
                       label: Label): (Array[(LabelMeta, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(LabelMeta, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = label.labelMetaMap(bytes(pos))
      pos += 1
      val (v, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, 0, schemaVer)
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
                             schemaVer: String,
                             label: Label): (Array[(LabelMeta, InnerValLikeWithTs)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(LabelMeta, InnerValLikeWithTs)](len)
    var i = 0
    while (i < len) {
      val k = label.labelMetaMap(bytes(pos))
      pos += 1
      val (v, numOfBytesUsed) = InnerValLikeWithTs.fromBytes(bytes, pos, 0, schemaVer)
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
                   schemaVer: String): (Array[(LabelMeta, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(LabelMeta, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = LabelMeta.empty
      val (v, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, 0, schemaVer)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    //    logger.error(s"bytesToProps: $kvs")
    val ret = (kvs, pos)

    ret
  }

  def bytesToLong(bytes: Array[Byte], offset: Int): Long = Bytes.toLong(bytes, offset)

  def bytesToInt(bytes: Array[Byte], offset: Int): Int = Bytes.toInt(bytes, offset)
}

trait StorageDeserializable[E] {
  def fromKeyValues[T: CanSKeyValue](kvs: Seq[T], cacheElementOpt: Option[E]): Option[E]
//  = {
//    try {
//      Option(fromKeyValuesInner(kvs, cacheElementOpt))
//    } catch {
//      case e: Exception =>
//        logger.error(s"${this.getClass.getName} fromKeyValues failed.", e)
//        None
//    }
//  }
//  def fromKeyValuesInner[T: CanSKeyValue](kvs: Seq[T], cacheElementOpt: Option[E]): E
}
