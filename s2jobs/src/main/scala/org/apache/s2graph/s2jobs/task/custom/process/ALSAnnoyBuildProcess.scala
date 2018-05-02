package org.apache.s2graph.s2jobs.task.custom.process

import java.io.File

import annoy4s.{Angular, Annoy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ALSAnnoyBuildProcess {

  def buildAnnoyIndex(ss: SparkSession,
                      conf: TaskConf,
                      dataFrame: DataFrame): Unit = {
    // annoy tree params.
    val outputPath = conf.options("outputPath")
    val localInputPath = conf.options("localInputPath")
    val localIndexPath = conf.options("localIndexPath")
    val numDimensions = conf.options.getOrElse("dimensions", "10").toInt

    // als model params.
    val rank = conf.options.getOrElse("rank", numDimensions.toString).toInt
    val maxIter = conf.options.getOrElse("maxIter", "5").toInt
    val regParam = conf.options.getOrElse("regParam", "0.01").toDouble
    val userCol = conf.options.getOrElse("userCol", "userId")
    val itemCol = conf.options.getOrElse("itemCol", "movieId")
    val ratingCol = conf.options.getOrElse("ratingCol", "rating")

    assert(rank == numDimensions)

    val als = new ALS()
      .setRank(rank)
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setUserCol(userCol)
      .setItemCol(itemCol)
      .setRatingCol(ratingCol)

    val model = als.fit(dataFrame)

    saveFeatures(ss, model.itemFactors, outputPath)
    copyToLocal(ss.sparkContext.hadoopConfiguration, outputPath, localInputPath)

    FileUtil.fullyDelete(new File(localIndexPath))

    Annoy.create[Int](s"${localInputPath}", numDimensions, outputDir = s"$localIndexPath", Angular)
  }

  def saveFeatures(ss: SparkSession,
                   dataFrame: DataFrame,
                   outputPath: String,
                   idCol: String = "id",
                   featuresCol: String = "features"): Unit = {
    import ss.sqlContext.implicits._

    val result = dataFrame.map { row =>
      val id = row.getAs[Int](idCol)
      val vector = row.getAs[Seq[Float]](featuresCol)
      (Seq(id) ++ vector).mkString(" ")
    }

    result.write.mode(SaveMode.Overwrite).csv(outputPath)
  }

  def copyToLocal(configuration: Configuration,
                  remoteInputPath: String,
                  localOutputPath: String,
                  merge: Boolean = true): Unit  = {
    val fs = FileSystem.get(configuration)
    val localFs = FileSystem.getLocal(configuration)
    localFs.deleteOnExit(new Path(localOutputPath))

    if (merge)
      FileUtil.copyMerge(fs, new Path(remoteInputPath), localFs, new Path(localOutputPath), false, configuration, "")
    else
      fs.copyToLocalFile(new Path(remoteInputPath), new Path(localOutputPath))
  }
}
class ALSAnnoyBuildProcess(conf: TaskConf) extends org.apache.s2graph.s2jobs.task.Process(conf) {
  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = ???

  override def mandatoryOptions: Set[String] = Set("outputPath")


}
