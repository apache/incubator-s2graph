package org.apache.s2graph.s2jobs.wal.process

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.WalLog
import org.apache.s2graph.s2jobs.wal.udafs._
import org.apache.s2graph.s2jobs.wal.utils.BoundedPriorityQueue
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.mutable
import scala.util.Random

class WalLogAggregateProcessTest extends FunSuite with Matchers with BeforeAndAfterAll with DataFrameSuiteBase {
  val walLogsLs = Seq(
    WalLog(1L, "insert", "edge", "a", "b", "s2graph", "friends", """{"name": 1}"""),
    WalLog(2L, "insert", "edge", "a", "c", "s2graph", "friends", """{"name": 2}"""),
    WalLog(3L, "insert", "edge", "a", "d", "s2graph", "friends", """{"name": 3}"""),
    WalLog(4L, "insert", "edge", "a", "b", "s2graph", "friends", """{"name": 4}""")
  )
  val walLogsLs2 = Seq(
    WalLog(5L, "insert", "edge", "a", "b", "s2graph", "friends", """{"name": 1}"""),
    WalLog(6L, "insert", "edge", "a", "c", "s2graph", "friends", """{"name": 2}"""),
    WalLog(7L, "insert", "edge", "a", "d", "s2graph", "friends", """{"name": 3}"""),
    WalLog(8L, "insert", "edge", "a", "b", "s2graph", "friends", """{"name": 4}""")
  )


  test("test S2EdgeDataAggregateProcess") {
    import spark.sqlContext.implicits._

    val edges = spark.createDataset(walLogsLs).toDF()
    val inputMap = Map("edges" -> edges)
    val taskConf = new TaskConf(name = "test", `type` = "agg", inputs = Seq("edges"),
      options = Map("maxNumOfEdges" -> "10")
    )

    val job = new WalLogAggregateProcess(taskConf = taskConf)
    val processed = job.execute(spark, inputMap)

    processed.printSchema()
    processed.collect().foreach { row =>
      println(row)
    }
  }

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