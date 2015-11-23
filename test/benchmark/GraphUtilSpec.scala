package benchmark

import com.kakao.s2graph.core.{Management, GraphUtil}
import com.kakao.s2graph.core.types.{SourceVertexId, HBaseType, InnerVal, VertexId}
import org.apache.hadoop.hbase.util.Bytes
import play.api.test.{FakeApplication, PlaySpecification}

import scala.collection.mutable.ListBuffer

class GraphUtilSpec extends BenchmarkCommon with PlaySpecification {


  "GraphUtil" should {
    "test murmur hash skew" in {
      running(FakeApplication()) {
        import HBaseType._
        val testNum = 1000000L
        val regionCount = 40
        val window = Int.MaxValue / regionCount
        val rangeShort = new ListBuffer[(Short, Short)]()

        for {
          i <- (0 until regionCount)
        } yield {
            val startKey =  Bytes.toBytes(i * window)
            val endKey = Bytes.toBytes((i + 1) * window)
            val x = Bytes.toShort(startKey.take(2))
            val y = Bytes.toShort(endKey.take(2))
            rangeShort += (x -> y)
          }

        def betweenShort(value: Short, start: Short, end: Short): Boolean =
          start <= value && value <= end

        val stats = new collection.mutable.HashMap[Int, Long]()

        for (i <- (0L until testNum)) {
          val hash = GraphUtil.murmur3(i.toString)
          val headOpt = rangeShort.zipWithIndex.dropWhile { case ((start, end), idx) =>
            !betweenShort(hash, start, end)
          }.headOption
          val head = headOpt match {
            case None => rangeShort.zipWithIndex.last
            case Some(s) => s
          }
          val key = head._2
          val value = stats.getOrElse(key, 0L) + 1L
          stats += (key -> value)
        }
        val result = stats.toList.sortBy(kv => kv._2).reverse
        println(s"$result\n")
      }
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
          val x = Bytes.toShort(startKey.take(2))
          val y = Bytes.toShort(endKey.take(2))
          rangeBytes += (startKey.toList -> endKey.toList)
        }

        def betweenShort(value: Short, start: Short, end: Short): Boolean =
          start <= value && value <= end

        def between(bytes: Array[Byte], startKey: Array[Byte], endKey: Array[Byte]): Boolean =
          (Bytes.compareTo(startKey, bytes) >= 0 && Bytes.compareTo(bytes, endKey) <= 0)

        val stats = new collection.mutable.HashMap[Int, Long]()

        for (i <- (0L until testNum)) {
          val vertexId = SourceVertexId(DEFAULT_COL_ID, InnerVal.withLong(i, HBaseType.DEFAULT_VERSION))
          val bytes = vertexId.bytes
          val headOpt = rangeBytes.zipWithIndex.dropWhile { case ((start, end), idx) =>
            !between(bytes, start.toArray, end.toArray)
          }.headOption
          val head = headOpt match {
            case None => rangeBytes.zipWithIndex.last
            case Some(s) => s
          }
          val key = head._2
          val value = stats.getOrElse(key, 0L) + 1L
          stats += (key -> value)
        }
        val result = stats.toList.sortBy(kv => kv._2).reverse
        //        val result2 = stats2.toList.sortBy(kv => kv._2).reverse.take(100)
        //        val result3 = stats3.toList.sortBy(kv => kv._2).reverse.take(100)
        println(s"$result\n")
        //        println(s"$result2\n")
        //        println(s"$result3\n")
      }
      true
    }
  }

}
