package com.kakao.ml.recommendation

import com.kakao.ml.Params
import com.kakao.ml.io._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

case class ALSParams(rank: Option[Int], maxIter: Option[Int], halfLifeTimeInMinute: Option[Int]) extends Params

class ALS(params: ALSParams) extends MatrixFactorization(params) {

  override protected def processBlock(sqlContext: SQLContext, input: IndexedData): MatrixFactorizationData = {

    val rank = params.rank.getOrElse(10)
    val maxIter = params.maxIter.getOrElse(10)

    /** TODO: implement */
    val decay = udf((ts: Long, confidence: Double) => confidence)

    val indexedDF = input.indexedDF
        .groupBy(indexedUserCol, indexedItemCol)
        .agg(indexedUserCol, indexedItemCol, sum(decay(tsCol, confidenceCol)) as confidenceColString)

    val alsModel = new org.apache.spark.ml.recommendation.ALS()
        .setRank(rank)
        .setMaxIter(maxIter)
        .setUserCol(indexedUserColString)
        .setItemCol(indexedItemColString)
        .setRatingCol(confidenceColString)
        .setImplicitPrefs(true)
        .fit(indexedDF)

    MatrixFactorizationData(alsModel, Some(indexedDF))
  }
}
