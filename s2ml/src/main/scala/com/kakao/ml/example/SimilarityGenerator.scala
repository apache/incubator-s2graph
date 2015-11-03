package com.kakao.ml.example

import com.kakao.ml.recommendation.BatchJobData
import com.kakao.ml.{BaseDataProcessor, EmptyData}
import org.apache.spark.sql.SQLContext

class SimilarityGenerator extends BaseDataProcessor[EmptyData, BatchJobData] {

  import com.kakao.ml.recommendation._

  override protected def processBlock(sqlContext: SQLContext, input: EmptyData): BatchJobData = {

    import sqlContext.implicits._

    val numItems = 1000
    val numSimilarItems = 100

    val generatedSimilarUsers = sqlContext.sparkContext.parallelize(0 until numItems).flatMap { itemId =>
      (0 until numSimilarItems).map { targetId =>
        val score = itemId / (targetId + numItems.toDouble)
        (itemId.toString, targetId.toString, score)
      }
    }.toDF(fromColString, toColString, scoreColString)

    BatchJobData(0L, 0L, None, None, None, None, Some(generatedSimilarUsers))

  }
}
