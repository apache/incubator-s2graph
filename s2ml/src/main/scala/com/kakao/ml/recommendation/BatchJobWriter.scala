package com.kakao.ml.recommendation

import com.kakao.ml.{BaseDataProcessor, EmptyParams}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class BatchJobWriter(params: EmptyParams) extends BaseDataProcessor[BatchJobData, BatchJobData](params){

  def saveTo(df: DataFrame, suffix: String): Unit =
    df.write.mode(SaveMode.Ignore).parquet(batchDir + "/" + suffix)

  def saveTo(rdd: RDD[_], suffix: String): Unit =
    rdd.saveAsTextFile(batchDir + "/" + suffix)

  override protected def processBlock(sqlContext: SQLContext, input: BatchJobData): BatchJobData = {

    val sc = sqlContext.sparkContext

    val metadata = compact(render(("jobId" -> jobId) ~ ("batchId" -> batchId)
        ~ ("tsFrom" -> input.tsFrom) ~ ("tsTo" -> input.tsTo)))
    saveTo(sc.parallelize(Seq(metadata), 1), "metadata")

    input.indexedUserDF.foreach(saveTo(_, "userDF"))
    input.indexedItemDF.foreach(saveTo(_, "itemDF"))
    input.model.foreach { model =>
      saveTo(model.userFactors, "als/userFactorDF")
      saveTo(model.itemFactors, "als/itemFactors")
      val metadata = compact(render("rank" -> model.rank))
      saveTo(sc.parallelize(Seq(metadata), 1), "als/metadata")
    }
    input.similarUserDF.foreach(saveTo(_, "similarUserDF"))
    input.similarItemDF.foreach(saveTo(_, "similarItemDF"))

    input
  }
}
