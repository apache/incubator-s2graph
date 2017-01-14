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
import org.apache.s2graph.core.{GraphUtil, S2Vertex}
import org.apache.s2graph.core.mysqls.ServiceColumn
import org.apache.s2graph.core.types.HBaseType._

object VertexId extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (VertexId, Int) = {
    /* since murmur hash is prepended, skip numOfBytes for murmur hash */
    var pos = offset + GraphUtil.bytesForMurMurHash

    val (innerId, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, len, version, isVertexId = true)
    pos += numOfBytesUsed
    val colId = Bytes.toInt(bytes, pos, 4)
    val column = ServiceColumn.findById(colId)
    (VertexId(column, innerId), GraphUtil.bytesForMurMurHash + numOfBytesUsed + 4)
  }

  def apply(column: ServiceColumn, innerId: InnerValLike): VertexId = new VertexId(column, innerId)

  def toSourceVertexId(vid: VertexId) = {
    SourceVertexId(vid.column, vid.innerId)
  }

  def toTargetVertexId(vid: VertexId) = {
    TargetVertexId(vid.column, vid.innerId)
  }
}

class VertexId (val column: ServiceColumn, val innerId: InnerValLike) extends HBaseSerializable {
  val storeHash: Boolean = true
  val storeColId: Boolean = true
  val colId = column.id.get
  lazy val hashBytes =
//    if (storeHash) Bytes.toBytes(GraphUtil.murmur3(innerId.toString))
    if (storeHash) Bytes.toBytes(GraphUtil.murmur3(innerId.toIdString()))
    else Array.empty[Byte]

  lazy val colIdBytes: Array[Byte] =
    if (storeColId) Bytes.toBytes(column.id.get)
    else Array.empty[Byte]

  def bytes: Array[Byte] = Bytes.add(hashBytes, innerId.bytes, colIdBytes)

  override def toString(): String = {
    //    column.id.get.toString() + "," + innerId.toString()
    val del = S2Vertex.VertexLabelDelimiter
    s"${column.service.serviceName}${del}${column.columnName}${del}${innerId}"
  }

  override def hashCode(): Int = {
    val ret = if (storeColId) {
      column.id.get * 31 + innerId.hashCode()
    } else {
      innerId.hashCode()
    }
//    logger.debug(s"VertexId.hashCode: $ret")
    ret
  }
  override def equals(obj: Any): Boolean = {
    val ret = obj match {
      case other: VertexId => colId == other.colId && innerId.toIdString() == other.innerId.toIdString()
      case _ => false
    }
//    logger.debug(s"VertexId.equals: $this, $obj => $ret")
    ret
  }

  def compareTo(other: VertexId): Int = {
    Bytes.compareTo(bytes, other.bytes)
  }
  def <(other: VertexId): Boolean = compareTo(other) < 0
  def <=(other: VertexId): Boolean = compareTo(other) <= 0
  def >(other: VertexId): Boolean = compareTo(other) > 0
  def >=(other: VertexId): Boolean = compareTo(other) >= 0

}

object SourceVertexId extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (VertexId, Int) = {
    /* since murmur hash is prepended, skip numOfBytes for murmur hash */
    val pos = offset + GraphUtil.bytesForMurMurHash
    val (innerId, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, len, version, isVertexId = true)

    (SourceVertexId(ServiceColumn.Default, innerId), GraphUtil.bytesForMurMurHash + numOfBytesUsed)
  }

}


case class SourceVertexId(override val column: ServiceColumn,
                          override val innerId: InnerValLike) extends VertexId(column, innerId) {
  override val storeColId: Boolean = false
}

object TargetVertexId extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (VertexId, Int) = {
    /* murmur has is not prepended so start from offset */
    val (innerId, numOfBytesUsed) = InnerVal.fromBytes(bytes, offset, len, version, isVertexId = true)
    (TargetVertexId(ServiceColumn.Default, innerId), numOfBytesUsed)
  }
}

case class TargetVertexId(override val column: ServiceColumn,
                          override val innerId: InnerValLike)
  extends  VertexId(column, innerId) {
  override val storeColId: Boolean = false
  override val storeHash: Boolean = false

}

object SourceAndTargetVertexIdPair extends HBaseDeserializable {
  val delimiter = ":"
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = DEFAULT_VERSION): (SourceAndTargetVertexIdPair, Int) = {
    val pos = offset + GraphUtil.bytesForMurMurHash
    val (srcId, srcBytesLen) = InnerVal.fromBytes(bytes, pos, len, version, isVertexId = true)
    val (tgtId, tgtBytesLen) = InnerVal.fromBytes(bytes, pos + srcBytesLen, len, version, isVertexId = true)
    (SourceAndTargetVertexIdPair(srcId, tgtId), GraphUtil.bytesForMurMurHash + srcBytesLen + tgtBytesLen)
  }
}

case class SourceAndTargetVertexIdPair(val srcInnerId: InnerValLike, val tgtInnerId: InnerValLike) extends HBaseSerializable {
  val colId = DEFAULT_COL_ID
  import SourceAndTargetVertexIdPair._
  override def bytes: Array[Byte] = {
    val hashBytes = Bytes.toBytes(GraphUtil.murmur3(srcInnerId + delimiter + tgtInnerId))
    Bytes.add(hashBytes, srcInnerId.bytes, tgtInnerId.bytes)
  }
}

