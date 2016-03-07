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

package s2.counter.core.v2

import org.apache.hadoop.hbase.util._
import s2.counter.core.TimedQualifier.IntervalUnit
import s2.counter.core._
import s2.models.Counter.ItemType
import s2.util.Hashes

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 11..
 */
object BytesUtilV2 extends BytesUtil {
  // ExactKey: [hash(1b)][version(1b)][policy(4b)][item(variable)]
  val BUCKET_BYTE_SIZE = Bytes.SIZEOF_BYTE
  val VERSION_BYTE_SIZE = Bytes.SIZEOF_BYTE
  val POLICY_ID_SIZE = Bytes.SIZEOF_INT

  val INTERVAL_SIZE = Bytes.SIZEOF_BYTE
  val TIMESTAMP_SIZE = Bytes.SIZEOF_LONG
  val TIMED_QUALIFIER_SIZE = INTERVAL_SIZE + TIMESTAMP_SIZE

  override def getRowKeyPrefix(id: Int): Array[Byte] = {
    Array(s2.counter.VERSION_2) ++ Bytes.toBytes(id)
  }

  override def toBytes(key: ExactKeyTrait): Array[Byte] = {
    val buff = new ArrayBuffer[Byte]
    // hash byte
    buff ++= Bytes.toBytes(Hashes.murmur3(key.itemKey)).take(BUCKET_BYTE_SIZE)

    // row key prefix
    // version + policy id
    buff ++= getRowKeyPrefix(key.policyId)

    buff ++= {
      key.itemType match {
        case ItemType.INT => Bytes.toBytes(key.itemKey.toInt)
        case ItemType.LONG => Bytes.toBytes(key.itemKey.toLong)
        case ItemType.STRING | ItemType.BLOB => Bytes.toBytes(key.itemKey)
      }
    }
    buff.toArray
  }

  override def toBytes(eq: ExactQualifier): Array[Byte] = {
    val len = eq.dimKeyValues.map { case (k, v) => k.length + 2 + v.length + 2 }.sum
    val pbr = new SimplePositionedMutableByteRange(len)
    for {
      v <- ExactQualifier.makeSortedDimension(eq.dimKeyValues)
    } {
      OrderedBytes.encodeString(pbr, v, Order.ASCENDING)
    }
    toBytes(eq.tq) ++ pbr.getBytes
  }

  override def toBytes(tq: TimedQualifier): Array[Byte] = {
    val pbr = new SimplePositionedMutableByteRange(INTERVAL_SIZE + 2 + TIMESTAMP_SIZE + 1)
    OrderedBytes.encodeString(pbr, tq.q.toString, Order.ASCENDING)
    OrderedBytes.encodeInt64(pbr, tq.ts, Order.DESCENDING)
    pbr.getBytes
  }

  private def decodeString(pbr: PositionedByteRange): Stream[String] = {
    if (pbr.getRemaining > 0) {
      Stream.cons(OrderedBytes.decodeString(pbr), decodeString(pbr))
    }
    else {
      Stream.empty
    }
  }

  override def toExactQualifier(bytes: Array[Byte]): ExactQualifier = {
    val pbr = new SimplePositionedByteRange(bytes)
    ExactQualifier(toTimedQualifier(pbr), {
      val seqStr = decodeString(pbr).toSeq
      val (keys, values) = seqStr.splitAt(seqStr.length / 2)
      keys.zip(values).toMap
    })
  }

  override def toTimedQualifier(bytes: Array[Byte]): TimedQualifier = {
    val pbr = new SimplePositionedByteRange(bytes)
    toTimedQualifier(pbr)
  }

  def toTimedQualifier(pbr: PositionedByteRange): TimedQualifier = {
    TimedQualifier(IntervalUnit.withName(OrderedBytes.decodeString(pbr)), OrderedBytes.decodeInt64(pbr))
  }
}
