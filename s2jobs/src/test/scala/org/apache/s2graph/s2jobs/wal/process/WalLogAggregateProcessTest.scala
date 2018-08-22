package org.apache.s2graph.s2jobs.wal.process

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class WalLogAggregateProcessTest extends FunSuite with Matchers with BeforeAndAfterAll with DataFrameSuiteBase {
  import org.apache.s2graph.s2jobs.wal.TestData._

  test("test entire process") {
    import spark.sqlContext.implicits._

    val edges = spark.createDataset(walLogsLs).toDF()
    val processKey = "agg"
    val inputMap = Map(processKey -> edges)

    val taskConf = new TaskConf(name = "test", `type` = "agg", inputs = Seq(processKey),
      options = Map("maxNumOfEdges" -> "10")
    )

    val job = new WalLogAggregateProcess(taskConf = taskConf)
    val processed = job.execute(spark, inputMap)

    processed.printSchema()
    processed.orderBy("from").as[WalLogAgg].collect().zip(aggExpected).foreach { case (real, expected) =>
      real shouldBe expected
    }
  }

}