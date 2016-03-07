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

package benchmark

import com.kakao.s2graph.core.{Management, GraphUtil}
import com.kakao.s2graph.core.types.{SourceVertexId, HBaseType, InnerVal, VertexId}
import org.apache.hadoop.hbase.util.Bytes
import play.api.test.{FakeApplication, PlaySpecification}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class GraphUtilSpec extends BenchmarkCommon with PlaySpecification {

  def between(bytes: Array[Byte], startKey: Array[Byte], endKey: Array[Byte]): Boolean =
    Bytes.compareTo(startKey, bytes) <= 0 && Bytes.compareTo(endKey, bytes) >= 0

  def betweenShort(value: Short, start: Short, end: Short): Boolean =
    start <= value && value <= end


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
        val regionCount = 40
        val window = Int.MaxValue / regionCount
        val rangeBytes = new ListBuffer[(List[Byte], List[Byte])]()
        for {
          i <- (0 until regionCount)
        } yield {
          val startKey =  Bytes.toBytes(i * window)
          val endKey = Bytes.toBytes((i + 1) * window)
          rangeBytes += (startKey.toList -> endKey.toList)
        }



        val stats = new collection.mutable.HashMap[Int, ((List[Byte], List[Byte]), Long)]()
        val counts = new collection.mutable.HashMap[Short, Long]()
        stats += (0 -> (rangeBytes.head -> 0L))

        for (i <- (0L until testNum)) {
          val vertexId = SourceVertexId(DEFAULT_COL_ID, InnerVal.withLong(i, HBaseType.DEFAULT_VERSION))
          val bytes = vertexId.bytes
          val shortKey = GraphUtil.murmur3(vertexId.innerId.toIdString())
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
        val sorted = stats.toList.sortBy(kv => kv._2._2).reverse
        println(s"Index: StartBytes ~ EndBytes\tStartShortBytes ~ EndShortBytes\tStartShort ~ EndShort\tCount\tShortCount")
        sorted.foreach { case (idx, ((start, end), cnt)) =>
          val startShort = Bytes.toShort(start.take(2).toArray)
          val endShort = Bytes.toShort(end.take(2).toArray)
          val count = counts.count(t => startShort <= t._1 && t._1 < endShort)
          println(s"$idx: $start ~ $end\t${start.take(2)} ~ ${end.take(2)}\t$startShort ~ $endShort\t$cnt\t$count")

        }
        println("\n" * 10)
        println(s"Index: StartBytes ~ EndBytes\tStartShortBytes ~ EndShortBytes\tStartShort ~ EndShort\tCount\tShortCount")
        stats.toList.sortBy(kv => kv._1).reverse.foreach { case (idx, ((start, end), cnt)) =>
          val startShort = Bytes.toShort(start.take(2).toArray)
          val endShort = Bytes.toShort(end.take(2).toArray)
          val count = counts.count(t => startShort <= t._1 && t._1 < endShort)
          println(s"$idx: $start ~ $end\t${start.take(2)} ~ ${end.take(2)}\t$startShort ~ $endShort\t$cnt\t$count")

        }
      }
      true
    }

    "Bytes compareTo" in {
      val x = Array[Byte](11, -12, -26, -14, -23)
      val startKey = Array[Byte](0, 0, 0, 0)
      val endKey = Array[Byte](12, -52, -52, -52)
      println(Bytes.compareTo(startKey, x))
      println(Bytes.compareTo(endKey, x))
      true
    }
  }

}
