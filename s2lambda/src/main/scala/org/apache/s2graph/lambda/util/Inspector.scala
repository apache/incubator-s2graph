package org.apache.s2graph.lambda.util

import org.apache.s2graph.lambda.{BaseDataProcessor, Params, PredecessorData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

case class InspectorParams(num: Option[Int]) extends Params

class Inspector(params: InspectorParams) extends BaseDataProcessor[PredecessorData, PredecessorData](params) {

  val defaultNum = 20

  def isDataFrame[T](df: T): Boolean = df.isInstanceOf[DataFrame]

  def isRDD[T](rdd: T): Boolean = rdd.isInstanceOf[RDD[_]]

  override protected def processBlock(input: PredecessorData): PredecessorData = {

    val num = params.num.getOrElse(defaultNum)

    predecessorData.asMap.foreach {
      case (key, df: DataFrame) =>
        logger.info(s"$key:DataFrame => ")
        df.show()//(num, false)
        logger.info(df.schema.treeString)
      case (key, df: Some[_]) if isDataFrame(df.get) =>
        df.get.asInstanceOf[DataFrame].show()//(num, false)
        logger.info(s"$key:DataFrame => ")
        df.asInstanceOf[DataFrame].show()//(num, false)
        logger.info(df.get.asInstanceOf[DataFrame].schema.treeString)
      case (key, rdd: RDD[_]) =>
        val samples = rdd.take(num)
        if (samples.length > 0)
          logger.info(s"$key: RDD[${samples.head.getClass.getName}] => ${samples.mkString("\n")}")
        else
          logger.info(rdd.toString() + " " + samples.mkString("\n"))
      case (key, rdd: Some[_]) if isRDD(rdd.get) =>
        val samples = rdd.get.asInstanceOf[RDD[_]].take(num)
        if (samples.length > 0)
          logger.info(s"$key: RDD[${samples.head.getClass.getName}] => ${samples.mkString("\n")}")
        else
          logger.info(rdd.get.toString + " " + samples.mkString("\n"))
      case (key, any) =>
        val t = any.getClass.getName
        logger.info(s"$key: $t => $any")
    }
    predecessorData
  }
}
