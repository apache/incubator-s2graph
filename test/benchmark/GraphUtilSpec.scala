package benchmark

import com.kakao.s2graph.core.{Management, GraphUtil}
import com.kakao.s2graph.core.types.{SourceVertexId, HBaseType, InnerVal, VertexId}
import org.apache.hadoop.hbase.util.Bytes
import play.api.test.{FakeApplication, PlaySpecification}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class GraphUtilSpec extends BenchmarkCommon with PlaySpecification {


  "GraphUtil" should {
    "test murmur3 hash function distribution" in {
      val testNum = 1000000
      val bucketSize = Short.MaxValue / 40
      val countsNew = new mutable.HashMap[Int, Int]()
      val counts = new mutable.HashMap[Int, Int]()
      for {
        i <- (0 until testNum)
      } {
        val h = GraphUtil.murmur3(i.toString) / bucketSize
        val hNew = GraphUtil.murmur3Int(i.toString) / bucketSize
        counts += (h -> (counts.getOrElse(h, 0) + 1))
        countsNew += (hNew -> (countsNew.getOrElse(hNew, 0) + 1))
      }
      val all = counts.toList.sortBy { case (bucket, count) => count }.reverse
      val allNew = countsNew.toList.sortBy { case (bucket, count) => count }.reverse
      val top = all.take(10)
      val bottom = all.takeRight(10)
      val topNew = allNew.take(10)
      val bottomNew = allNew.takeRight(10)
      println(s"Top: $top")
      println(s"Bottom: $bottom")
      println("-" * 50)
      println(s"TopNew: $topNew")
      println(s"Bottom: $bottomNew")
      true
    }

    "test murmur hash skew2" in {
      running(FakeApplication()) {
        import HBaseType._
        val testNum = 1000000L
        val regionCount = 10
        val window = Int.MaxValue / regionCount
        val rangeBytes = new ListBuffer[(List[Byte], List[Byte])]()
        for {
          i <- (0 until regionCount)
        } yield {
          val startKey =  Bytes.toBytes(i * window)
          val endKey = Bytes.toBytes((i + 1) * window)
          rangeBytes += (startKey.toList -> endKey.toList)
        }

        def betweenShort(value: Short, start: Short, end: Short): Boolean =
          start <= value && value <= end

        def between(bytes: Array[Byte], startKey: Array[Byte], endKey: Array[Byte]): Boolean =
          (Bytes.compareTo(startKey, bytes) >= 0 && Bytes.compareTo(bytes, endKey) <= 0)

        val stats = new collection.mutable.HashMap[Int, ((List[Byte], List[Byte]), Long)]()
        val counts = new collection.mutable.HashMap[Short, Long]()
        stats += (0 -> (rangeBytes.head -> 0L))

        for (i <- (0L until testNum)) {
          val vertexId = SourceVertexId(DEFAULT_COL_ID, InnerVal.withLong(i, HBaseType.DEFAULT_VERSION))
          val bytes = vertexId.bytes
          val shortKey = Bytes.toShort(bytes.take(2))
          val shortVal = counts.getOrElse(shortKey, 0L) + 1L
          counts += (shortKey -> shortVal)

          var j = 0
          var found = false
          while (j < rangeBytes.size && !found) {
            val (start, end) = rangeBytes(j)
            if (between(bytes, start.toArray, end.toArray)) {
              found = true
            }
            j += 1
          }
          val head = rangeBytes(j - 1)
          val key = j - 1
          val value = stats.get(key) match {
            case None => 0L
            case Some(v) => v._2 + 1
          }
          stats += (key -> (head, value))
        }
        stats.toList.sortBy(kv => kv._2._2).reverse.foreach { case (idx, ((start, end), cnt)) =>
          val startShort = Bytes.toShort(start.take(2).toArray)
          val endShort = Bytes.toShort(end.take(2).toArray)
          val count = counts.count(t => startShort <= t._1 && t._1 < endShort)
          println(s"$idx: $start ~ $end\t$startShort ~ $endShort\t$cnt\t$count")

        }
      }
      true
    }
  }

}
