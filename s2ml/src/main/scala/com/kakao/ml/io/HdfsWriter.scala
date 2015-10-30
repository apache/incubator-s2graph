package com.kakao.ml.io

import com.kakao.ml.util.Json
import com.kakao.ml.{BaseDataProcessor, Params, PredecessorData}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable

case class HdfsParams(outputDir: Option[String]) extends Params

class HdfsWriter(params: HdfsParams)
    extends BaseDataProcessor[PredecessorData, PredecessorData](params) {

  def getOutputDir(key: String) = params.outputDir.getOrElse(batchDir) + s"/$key"

  def saveDataFrame(key: String, df: DataFrame): Unit = {
    val o = getOutputDir(key)
    df.write.mode(SaveMode.Ignore).parquet(o)
  }

  def saveRdd(key: String, rdd: RDD[_]): Unit = {
    val sc = rdd.sparkContext
    val o = getOutputDir(key)
    if(o.split('/').length > 3) {
      if(!FileSystem.get(sc.hadoopConfiguration).exists(new Path(o)))
        rdd.saveAsTextFile(o)
    }
  }

  def isDataFrame[T](df: T): Boolean = df.isInstanceOf[DataFrame]

  override protected def processBlock(sqlContext: SQLContext, input: PredecessorData): PredecessorData = {

    val sc = sqlContext.sparkContext

    val hashMap = new mutable.HashMap[String, Any]
    hashMap += ("jobId" -> jobId)
    hashMap += ("batchId" -> batchId)
    hashMap += ("batchId" -> lastBatchId)
    hashMap += ("env" -> env)

    input.asMap.foreach {
      case (key, df: DataFrame) => saveDataFrame(key, df)
      case (key, df: Some[_]) if isDataFrame(df.get) => saveDataFrame(key, df.get.asInstanceOf[DataFrame])
      case (key, value) =>
        hashMap += (key -> value.toString)
    }

    saveRdd("metadata", sc.parallelize(Seq(Json.toJsonString(hashMap)), 1))

    predecessorData
  }
}
