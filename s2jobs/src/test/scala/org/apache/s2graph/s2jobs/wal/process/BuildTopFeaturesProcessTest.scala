package org.apache.s2graph.s2jobs.wal.process

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.DimValCountRank
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class BuildTopFeaturesProcessTest extends FunSuite with Matchers with BeforeAndAfterAll with DataFrameSuiteBase {

  import org.apache.s2graph.s2jobs.wal.TestData._

  test("test entire process.") {
    import spark.implicits._
    val df = spark.createDataset(aggExpected).toDF()

    val taskConf = new TaskConf(name = "test", `type` = "test", inputs = Seq("input"),
      options = Map("minUserCount" -> "0")
    )
    val job = new BuildTopFeaturesProcess(taskConf)


    val inputMap = Map("input" -> df)
    val featureDicts = job.execute(spark, inputMap)
      .orderBy("dim", "rank")
      .map(DimValCountRank.fromRow)
      .collect()

    featureDicts shouldBe featureDictExpected

  }
}
