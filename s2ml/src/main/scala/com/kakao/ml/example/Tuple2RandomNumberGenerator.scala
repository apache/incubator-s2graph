package com.kakao.ml.example

import com.kakao.ml.{BaseDataProcessor, Data, EmptyData, Params}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

case class Tuple2RandomNumberParams(num: Int) extends Params

case class Tuple2RandomNumberData(rdd: RDD[(Double, Double)], df: DataFrame, num: Int) extends Data

class Tuple2RandomNumberGenerator(params: Tuple2RandomNumberParams)
    extends BaseDataProcessor[EmptyData, Tuple2RandomNumberData]{

  override protected def processBlock(sqlContext: SQLContext, input: EmptyData): Tuple2RandomNumberData = {

    import sqlContext.implicits._

    val rdd = sqlContext.sparkContext.parallelize(1 to params.num).map{i =>
      (Math.random(), Math.random())
    }

    val df = rdd.toDF("x", "y")

    Tuple2RandomNumberData(rdd, df, params.num)
  }
}
