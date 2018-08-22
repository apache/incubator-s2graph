package org.apache.s2graph.s2jobs.wal.udafs

import org.apache.s2graph.s2jobs.wal.utils.BoundedPriorityQueue
import org.scalatest._

import scala.collection.mutable
import scala.util.Random

class WalLogUDAFTest extends FunSuite with Matchers {

  test("mergeTwoSeq") {
    val prev: Array[Int] = Array(3, 2, 1)
    val cur: Array[Int] = Array(4, 2, 2)

    val ls = WalLogUDAF.mergeTwoSeq(prev, cur, 10)
    println(ls.size)

    ls.foreach { x =>
      println(x)
    }
  }

  test("addToTopK test.") {
    import WalLogUDAF._
    val numOfTest = 100
    val numOfNums = 100
    val maxNum = 10

    (0 until numOfTest).foreach { testNum =>
      val maxSize = 1 + Random.nextInt(numOfNums)
      val pq = new BoundedPriorityQueue[Int](maxSize)
      val arr = (0 until numOfNums).map(x => Random.nextInt(maxNum))
      var result: mutable.Seq[Int] = mutable.ArrayBuffer.empty[Int]

      arr.foreach { i =>
        pq += i
        result = addToTopK(result, maxSize, i)
      }
      result.sorted shouldBe pq.toSeq.sorted
    }
  }
}
