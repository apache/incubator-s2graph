package org.apache.s2graph.lambda.example

import org.apache.s2graph.lambda._
import org.apache.s2graph.lambda.source.Source
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

case class Tuple2RandomNumberParams(num: Int) extends Params

case class Tuple2RandomNumberData(rdd: RDD[(Double, Double)], df: DataFrame, num: Int) extends Data

class Tuple2RandomNumberGenerator(params: Tuple2RandomNumberParams)
    extends Source[Tuple2RandomNumberData](params) {

  override protected def processBlock(input: EmptyData): Tuple2RandomNumberData = {
    val sqlContext = context.sqlContext
    import sqlContext.implicits._

    val rdd = sqlContext.sparkContext.parallelize(1 to params.num).map{i =>
      (Math.random(), Math.random())
    }

    Tuple2RandomNumberData(rdd, rdd.toDF("x", "y"), params.num)
  }
}
