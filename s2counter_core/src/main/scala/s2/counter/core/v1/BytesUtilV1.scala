package s2.counter.core.v1

import org.apache.hadoop.hbase.util.Bytes
import s2.counter.core.TimedQualifier.IntervalUnit
import s2.counter.core._
import s2.models.Counter.ItemType
import s2.util.Hashes

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 11..
 */
object BytesUtilV1 extends BytesUtil {
  // ExactKey: [hash(2b)][policy(4b)][item(variable)]
  val BUCKET_BYTE_SIZE = Bytes.SIZEOF_SHORT
  val POLICY_ID_SIZE = Bytes.SIZEOF_INT
  val INTERVAL_SIZE = Bytes.SIZEOF_BYTE
  val TIMESTAMP_SIZE = Bytes.SIZEOF_LONG
  val TIMED_QUALIFIER_SIZE = INTERVAL_SIZE + TIMESTAMP_SIZE

  override def getRowKeyPrefix(id: Int): Array[Byte] = {
    Bytes.toBytes(id)
  }

  override def toBytes(key: ExactKeyTrait): Array[Byte] = {
    val buff = new ArrayBuffer[Byte]
    // hash key (2 byte)
    buff ++= Bytes.toBytes(Hashes.murmur3(key.itemKey)).take(BUCKET_BYTE_SIZE)

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
    toBytes(eq.tq) ++ eq.dimension.getBytes
  }

  override def toBytes(tq: TimedQualifier): Array[Byte] = {
    Bytes.toBytes(tq.q.toString) ++ Bytes.toBytes(tq.ts)
  }

  override def toExactQualifier(bytes: Array[Byte]): ExactQualifier = {
    // qualifier: interval, ts, dimension 순서
    val tq = toTimedQualifier(bytes)

    val dimension = Bytes.toString(bytes, TIMED_QUALIFIER_SIZE, bytes.length - TIMED_QUALIFIER_SIZE)
    ExactQualifier(tq, dimension)
  }

  override def toTimedQualifier(bytes: Array[Byte]): TimedQualifier = {
    val interval = Bytes.toString(bytes, 0, INTERVAL_SIZE)
    val ts = Bytes.toLong(bytes, INTERVAL_SIZE)

    TimedQualifier(IntervalUnit.withName(interval), ts)
  }
}
