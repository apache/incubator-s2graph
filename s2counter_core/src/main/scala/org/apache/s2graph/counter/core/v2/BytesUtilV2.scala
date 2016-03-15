package org.apache.s2graph.counter.core.v2

import org.apache.hadoop.hbase.util._
import org.apache.s2graph.counter
import org.apache.s2graph.counter.core.TimedQualifier.IntervalUnit
import org.apache.s2graph.counter.core.{TimedQualifier, ExactQualifier, ExactKeyTrait, BytesUtil}
import org.apache.s2graph.counter.models.Counter.ItemType
import org.apache.s2graph.counter.util.Hashes
import scala.collection.mutable.ArrayBuffer

object BytesUtilV2 extends BytesUtil {
  // ExactKey: [hash(1b)][version(1b)][policy(4b)][item(variable)]
  val BUCKET_BYTE_SIZE = Bytes.SIZEOF_BYTE
  val VERSION_BYTE_SIZE = Bytes.SIZEOF_BYTE
  val POLICY_ID_SIZE = Bytes.SIZEOF_INT

  val INTERVAL_SIZE = Bytes.SIZEOF_BYTE
  val TIMESTAMP_SIZE = Bytes.SIZEOF_LONG
  val TIMED_QUALIFIER_SIZE = INTERVAL_SIZE + TIMESTAMP_SIZE

  override def getRowKeyPrefix(id: Int): Array[Byte] = {
    Array(counter.VERSION_2) ++ Bytes.toBytes(id)
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
