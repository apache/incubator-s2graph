package com.kakao.ml.io

import com.kakao.ml.recommendation._
import com.kakao.ml.{BaseDataProcessor, Params, PredecessorData}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class GraphBulkFormatWriterParams(
    keyToSave: String,
    label: String,
    scale: Option[Int]) extends Params

class GraphBulkFormatWriter(params: GraphBulkFormatWriterParams) extends BaseDataProcessor[PredecessorData, PredecessorData](params) {
  override protected def processBlock(sqlContext: SQLContext, input: PredecessorData): PredecessorData = {

    val keyToSave = params.keyToSave
    val label = params.label
    val scale = params.scale.getOrElse(100)

    if(!input.asMap.contains(keyToSave)) {
      logError(s"predecessor does not contain $keyToSave")
      return input
    }

    val dataToSaveAll = predecessorData.asMap(keyToSave) match {
      case df: DataFrame => df
      case Some(df: DataFrame) => df
      case _ =>
        logError(s"predecessorData($keyToSave) is not instance of DataFrame")
        return input
    }

    val dataToSave = try {
      dataToSaveAll.select(fromCol, toCol, scoreCol.cast(DoubleType))
    } catch {
      case _: Throwable =>
        logError(s"predecessorData($keyToSave) dose not contain columns $fromColString, $toColString, $scoreColString")
        return input
    }

    val ts = System.currentTimeMillis()
    val bulkFormattedRDD = dataToSave.map { case Row(from: String, to: String, score: Double) =>
      val prop = s"""{"weight" : ${(score * scale).toInt}}"""
      s"$ts\tinsertBulk\te\t$from\t$to\t$label\t$prop"
    }

    val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(bulkDir))) fs.delete(new Path(bulkDir), true)
    bulkFormattedRDD.saveAsTextFile(bulkDir, classOf[GzipCodec])

    predecessorData
  }
}
