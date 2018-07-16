package org.apache.s2graph.s2jobs.wal.process

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.{BoundedPriorityQueue, WalLog}
import org.apache.s2graph.s2jobs.wal.udafs._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.mutable
import scala.util.Random

class S2EdgeDataAggregateProcessTest extends FunSuite with Matchers with BeforeAndAfterAll with DataFrameSuiteBase {
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
      options = Map("maxNumOfEdges" -> "10",
        "runOrderBy" -> "false",
        "groupByAggClassName" -> "GroupByAggOptimized"))

    val job = new S2EdgeDataAggregateProcess(taskConf = taskConf)
    val processed = job.execute(spark, inputMap)

    processed.printSchema()
    processed.collect().foreach { row =>
      println(row)
    }
  }

  test("test S2EdgeDataArrayAggregateProcess") {
    import spark.sqlContext.implicits._

    val edges = spark.createDataset(walLogsLs).toDF()
    val edges2 = spark.createDataset(walLogsLs2).toDF()

    val firstConf = new TaskConf(name = "test", `type` = "agg", inputs = Seq("edges"),
      options = Map("maxNumOfEdges" -> "10"))

    val firstJob = new S2EdgeDataAggregateProcess(firstConf)
    val firstJob2 = new S2EdgeDataAggregateProcess(firstConf)

    val first = firstJob.execute(spark, Map("edges" -> edges))
    val first2 = firstJob2.execute(spark, Map("edges" -> edges2))

    val secondInputMap = Map(
      "aggregated" -> first.union(first2)
    )

    val secondConf = new TaskConf(name = "testarray", `type` = "agg",
      inputs = Seq("aggregated"),
      options = Map("maxNumOfEdges" -> "10"))

    val secondJob = new S2EdgeDataArrayAggregateProcess(secondConf)


    val processed = secondJob.execute(spark, secondInputMap)

    processed.printSchema()
    processed.collect().foreach { row =>
      println(row)
    }
  }

  test("mergeTwoSeq") {
    val prev: Array[Int] = Array(3, 2, 1)
    val cur: Array[Int] = Array(4, 2, 2)

    val ls = S2EdgeDataAggregate.mergeTwoSeq(prev, cur, 10)
    println(ls.size)

    ls.foreach { x =>
      println(x)
    }
  }

  test("addToTopK test.") {
    import S2EdgeDataAggregate._
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
      result.toSeq.sorted shouldBe pq.toSeq.sorted
    }

//    val maxSize = 1 + Random.nextInt(numOfNums)
//    val pq = new BoundedPriorityQueue[Int](maxSize)
//    val arr = (0 until numOfNums).map(x => Random.nextInt(maxNum))
//    val result = mutable.ArrayBuffer.empty[Int]
//    var lastPos = 0
//    arr.foreach { i =>
//      pq += i
//      addToTopK(result, lastPos, maxSize, i)
//      lastPos = lastPos + 1
//    }
//    result.toSeq.sorted shouldBe pq.toSeq.sorted
  }
}