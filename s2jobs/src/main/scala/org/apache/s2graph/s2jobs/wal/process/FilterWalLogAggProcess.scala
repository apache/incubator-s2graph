package org.apache.s2graph.s2jobs.wal.process

import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.WalLogAgg
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import play.api.libs.json.{JsObject, Json}

object FilterWalLogAggProcess {

  def collectDistinctFeatureHashes(ss: SparkSession,
                                   filteredDict: DataFrame): Array[Long] = {
    import ss.implicits._

    val featureHashUDF = udf((dim: String, value: String) => WalLogAgg.toFeatureHash(dim, value))

    filteredDict.withColumn("featureHash", featureHashUDF(col("dim"), col("value")))
      .select("featureHash")
      .distinct().as[Long].collect()
  }

  def filterTopKsPerDim(dict: DataFrame,
                        maxRankPerDim: Broadcast[Map[String, Int]],
                        defaultMaxRank: Int): DataFrame = {
    val filterUDF = udf((dim: String, rank: Long) => {
      rank < maxRankPerDim.value.getOrElse(dim, defaultMaxRank)
    })

    dict.filter(filterUDF(col("dim"), col("rank")))
  }

  def filterWalLogAgg(walLogAgg: Dataset[WalLogAgg],
                      validFeatureHashKeysBCast: Broadcast[Array[Long]]) = {
    walLogAgg.mapPartitions { iter =>
      val validFeatureHashKeys = validFeatureHashKeysBCast.value.toSet

      iter.map { walLogAgg =>
        WalLogAgg.filter(walLogAgg, validFeatureHashKeys)
      }
    }
  }
}

class FilterWalLogAggProcess(taskConf: TaskConf) extends org.apache.s2graph.s2jobs.task.Process(taskConf) {

  import FilterWalLogAggProcess._

  /*
    filter topKs per dim, then build valid dimValLs.
    then broadcast valid dimValLs to original dataframe, and filter out not valid dimVal.
   */
  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {
    val maxRankPerDim = taskConf.options.get("maxRankPerDim").map { s =>
      Json.parse(s).as[JsObject].fields.map { case (k, jsValue) =>
        k -> jsValue.as[Int]
      }.toMap
    }
    val maxRankPerDimBCast = ss.sparkContext.broadcast(maxRankPerDim.getOrElse(Map.empty))

    val defaultMaxRank = taskConf.options.get("defaultMaxRank").map(_.toInt)

    val featureDict = inputMap(taskConf.options("featureDict"))
    val walLogAgg = inputMap(taskConf.options("walLogAgg")).as[WalLogAgg]


    val filteredDict = filterTopKsPerDim(featureDict, maxRankPerDimBCast, defaultMaxRank.getOrElse(Int.MaxValue))
    val validFeatureHashKeys = collectDistinctFeatureHashes(ss, filteredDict)
    val validFeatureHashKeysBCast = ss.sparkContext.broadcast(validFeatureHashKeys)

    filterWalLogAgg(walLogAgg, validFeatureHashKeysBCast).toDF()
  }

  override def mandatoryOptions: Set[String] = Set("featureDict", "walLogAgg")
}
