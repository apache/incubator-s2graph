package com.kakao.ml.stat

import com.kakao.ml.io.{SourceData, _}
import com.kakao.ml.{BaseDataProcessor, Params}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

case class RevenueParams(scale: Int, adjust: Option[Double], ntile: Option[Int]) extends Params

class Revenue(params: RevenueParams) extends BaseDataProcessor[SourceData, SourceData](params) {

  override protected def processBlock(sqlContext: SQLContext, input: SourceData): SourceData = {

    import sqlContext.implicits._

    val scale = params.scale
    val adjust = params.adjust.getOrElse(0.0)
    val sourceDF = input.sourceDF.select(tsCol, userCol, itemCol)

    params.ntile.foreach { n =>
      val userRevenue = sourceDF
          .groupBy(userCol)
          .agg(sum(itemCol) as "revenue")

      sourceDF
          .groupBy(userCol)
          .count()
          .withColumn("ntile", ntile(n) over Window.orderBy(countColString))
          .join(userRevenue, userColString)
          .groupBy("ntile")
          .agg(
            min(countCol) as "min",
            max(countCol) as "max",
            count(countCol) as "count",
            round(mean(countCol), 2) as "mean",
            sum("revenue") as "revenue")
          .groupBy("min", "max")
          .agg(
            min("ntile") as "sort_key",
            concat_ws(" ~ ", min("ntile"), max("ntile")) as "ntile",
            sum($"count") as "count",
            first($"mean") as "mean",
            sum($"revenue") as "revenue")
          .orderBy("sort_key")
          .select("ntile", "count", "mean", "min", "max", "revenue")
          .show(10000, false)
    }

    val amountCol = itemCol.cast(DoubleType) + adjust

    sourceDF
        .withColumn("bin", round(amountCol, scale))
        .groupBy("bin")
        .agg(count(itemCol) as "count", sum(itemCol) as "revenue")
        .orderBy("bin")
        .show(10000, false)

    input
  }

}

