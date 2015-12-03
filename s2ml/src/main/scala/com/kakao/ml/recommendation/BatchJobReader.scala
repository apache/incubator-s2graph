package com.kakao.ml.recommendation

import com.kakao.ml.{EmptyData, BaseDataProcessor, EmptyParams}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.SQLContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

class BatchJobReader(params: EmptyParams) extends BaseDataProcessor[EmptyData, LastBatchJobData](params){

  override protected def processBlock(sqlContext: SQLContext, input: EmptyData): LastBatchJobData = {

    val sc = sqlContext.sparkContext

    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(lastBatchDir + "/metadata").first())
    val tsFrom = (metadata \ "tsFrom").extract[Long]
    val tsTo = (metadata \ "tsTo").extract[Long]

    val similarItemPath = new Path(lastBatchDir + "/similarItemDF")
    val similarUserPath = new Path(lastBatchDir + "/similarUserDF")

    val similarItemDF = if(FileSystem.get(sc.hadoopConfiguration).exists(similarItemPath)) {
      Some(sqlContext.read.parquet(similarItemPath.toString))
    } else {
      None
    }

    val similarUserDF = if(FileSystem.get(sc.hadoopConfiguration).exists(similarUserPath)) {
      Some(sqlContext.read.parquet(similarUserPath.toString))
    } else {
      None
    }

    show(s"tsFrom: $tsFrom")
    show(s"tsTo: $tsTo")

    LastBatchJobData(tsFrom, tsTo, None, None, None, similarUserDF, similarItemDF)
  }
}
