package org.apache.s2graph.s2jobs.wal.process

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.transformer.DefaultTransformer
import org.apache.s2graph.s2jobs.wal.{DimValCountRank, WalLogAgg}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class FilterTopFeaturesProcessTest extends FunSuite with Matchers with BeforeAndAfterAll with DataFrameSuiteBase {
  import org.apache.s2graph.s2jobs.wal.TestData._

  test("test filterTopKsPerDim.") {
    import spark.implicits._
    val featureDf = spark.createDataset(featureDictExpected).map { x =>
      (x.dimVal.dim, x.dimVal.value, x.count, x.rank)
    }.toDF("dim", "value", "count", "rank")

    val maxRankPerDim = spark.sparkContext.broadcast(Map.empty[String, Int])

    // filter nothing because all feature has rank < 10
    val filtered = FilterTopFeaturesProcess.filterTopKsPerDim(featureDf, maxRankPerDim, 10)

    val real = filtered.orderBy("dim", "rank").map(DimValCountRank.fromRow).collect()
    real.zip(featureDictExpected).foreach { case (real, expected) =>
        real shouldBe expected
    }
    // filter rank >= 2
    val filtered2 = FilterTopFeaturesProcess.filterTopKsPerDim(featureDf, maxRankPerDim, 2)
    val real2 = filtered2.orderBy("dim", "rank").map(DimValCountRank.fromRow).collect()
    real2 shouldBe featureDictExpected.filter(_.rank < 2)
  }


  test("test filterWalLogAgg.") {
    import spark.implicits._
    val walLogAgg = spark.createDataset(aggExpected)
    val featureDf = spark.createDataset(featureDictExpected).map { x =>
      (x.dimVal.dim, x.dimVal.value, x.count, x.rank)
    }.toDF("dim", "value", "count", "rank")
    val maxRankPerDim = spark.sparkContext.broadcast(Map.empty[String, Int])

    val transformers = Seq(DefaultTransformer(TaskConf.Empty))
    // filter nothing. so input, output should be same.
    val featureFiltered = FilterTopFeaturesProcess.filterTopKsPerDim(featureDf, maxRankPerDim, 10)
    val validFeatureHashKeys = FilterTopFeaturesProcess.collectDistinctFeatureHashes(spark, featureFiltered)
    val validFeatureHashKeysBCast = spark.sparkContext.broadcast(validFeatureHashKeys)
    val real = FilterTopFeaturesProcess.filterWalLogAgg(spark, walLogAgg, transformers, validFeatureHashKeysBCast)
      .collect().sortBy(_.from)

    real.zip(aggExpected).foreach { case (real, expected) =>
      real shouldBe expected
    }
  }

  test("test entire process. filter nothing.") {
    import spark.implicits._
    val df = spark.createDataset(aggExpected).toDF()
    val featureDf = spark.createDataset(featureDictExpected).map { x =>
      (x.dimVal.dim, x.dimVal.value, x.count, x.rank)
    }.toDF("dim", "value", "count", "rank")

    val inputKey = "input"
    val featureDictKey = "feature"
    // filter nothing since we did not specified maxRankPerDim and defaultMaxRank.
    val taskConf = new TaskConf(name = "test", `type` = "test",
      inputs = Seq(inputKey, featureDictKey),
      options = Map(
        "featureDict" -> featureDictKey,
        "walLogAgg" -> inputKey
      )
    )
    val inputMap = Map(inputKey -> df, featureDictKey -> featureDf)
    val job = new FilterTopFeaturesProcess(taskConf)
    val filtered = job.execute(spark, inputMap)
      .orderBy("from")
      .as[WalLogAgg]
      .collect()

    filtered.zip(aggExpected).foreach { case (real, expected) =>
      real shouldBe expected
    }

  }
}
