package s2.helper

import java.util
import java.util.Comparator

import com.google.common.primitives.SignedBytes
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 21..
 */

object DistributedScanner {
  val BUCKET_BYTE_SIZE = Bytes.SIZEOF_BYTE

  def getRealRowKey(result: Result): Array[Byte] = {
    result.getRow.drop(BUCKET_BYTE_SIZE)
  }
}

class DistributedScanner(table: Table, scan: Scan) extends AbstractClientScanner {
  import DistributedScanner._

  private val BYTE_MAX = BigInt(256)

  private[helper] val scanners = {
    for {
      i <- 0 until BYTE_MAX.pow(BUCKET_BYTE_SIZE).toInt
    } yield {
      val bucketBytes: Array[Byte] = Bytes.toBytes(i).takeRight(BUCKET_BYTE_SIZE)
      val newScan = new Scan(scan).setStartRow(bucketBytes ++ scan.getStartRow).setStopRow(bucketBytes ++ scan.getStopRow)
      table.getScanner(newScan)
    }
  }

  val resultCache = new util.TreeMap[Result, java.util.Iterator[Result]](new Comparator[Result] {
    val comparator = SignedBytes.lexicographicalComparator()
    override def compare(o1: Result, o2: Result): Int = {
      comparator.compare(getRealRowKey(o1), getRealRowKey(o2))
    }
  })

  lazy val initialized = {
    val iterators = scanners.map(_.iterator()).filter(_.hasNext)
    iterators.foreach { it =>
      resultCache.put(it.next(), it)
    }
    iterators.nonEmpty
  }

  override def next(): Result = {
    if (initialized) {
      Option(resultCache.pollFirstEntry()).map { entry =>
        val it = entry.getValue
        if (it.hasNext) {
          // fill cache
          resultCache.put(it.next(), it)
        }
        entry.getKey
      }.orNull
    } else {
      null
    }
  }

  override def close(): Unit = {
    for {
      scanner <- scanners
    } {
      scanner.close()
    }
  }
}
