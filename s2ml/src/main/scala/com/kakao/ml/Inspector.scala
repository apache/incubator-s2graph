package com.kakao.ml

import com.kakao.ml.util.PrivateMethodAccessor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

case class InspectorParams(num: Option[Int]) extends Params

class Inspector(params: InspectorParams) extends BaseDataProcessor[PredecessorData, PredecessorData](params) {

  val defaultNum = 20

  def isDataFrame[T](df: T): Boolean = df.isInstanceOf[DataFrame]

  def isRDD[T](rdd: T): Boolean = rdd.isInstanceOf[RDD[_]]

  override protected def processBlock(sqlContext: SQLContext, input: PredecessorData): PredecessorData = {

    val num = params.num.getOrElse(defaultNum)

    predecessorData.asMap.foreach {
      case (key, df: DataFrame) =>
        val samples = PrivateMethodAccessor(df, "showString")[String](num, false)
        logInfo(s"$key:DataFrame => $samples")
        logInfo(df.schema.treeString)
      case (key, df: Some[_]) if isDataFrame(df.get) =>
        val samples = PrivateMethodAccessor(df.get.asInstanceOf[DataFrame], "showString")[String](num, false)
        logInfo(s"$key:DataFrame => $samples")
        logInfo(df.get.asInstanceOf[DataFrame].schema.treeString)
      case (key, rdd: RDD[_]) =>
        val samples = rdd.take(num)
        logInfo(s"$key: RDD[${samples.head.getClass.getName}] => ${samples.mkString("\n")}")
      case (key, rdd: Some[_]) if isRDD(rdd.get) =>
        val samples = rdd.get.asInstanceOf[RDD[_]].take(num)
        logInfo(s"$key: RDD[${samples.head.getClass.getName}] => ${samples.mkString("\n")}")
      case (key, any) =>
        val t = any.getClass.getName
        logInfo(s"$key: $t => $any")
    }

    predecessorData
  }
}
