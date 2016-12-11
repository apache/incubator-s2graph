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
import org.apache.s2graph.core.types.{HBaseType, LabelWithDirection, SourceVertexId, VertexId}


trait Deserializable[E] extends StorageDeserializable[E] {
  import StorageDeserializable._

  type RowKeyRaw = (VertexId, LabelWithDirection, Byte, Boolean, Int)

//  /** version 1 and version 2 share same code for parsing row key part */
//  def parseRow(kv: SKeyValue, version: String = HBaseType.DEFAULT_VERSION): RowKeyRaw = {
//    var pos = 0
//    val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(kv.row, pos, kv.row.length, version)
//    pos += srcIdLen
//    val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
//    pos += 4
//    val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(kv.row, pos)
//
//    val rowLen = srcIdLen + 4 + 1
//    (srcVertexId, labelWithDir, labelIdxSeq, isInverted, rowLen)
//  }
}
