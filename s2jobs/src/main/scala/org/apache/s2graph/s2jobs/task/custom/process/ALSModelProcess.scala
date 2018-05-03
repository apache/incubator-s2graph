package org.apache.s2graph.s2jobs.task.custom.process

import java.io.File

import annoy4s.{Angular, Annoy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.s2graph.s2jobs.task.{Sink, TaskConf}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ALSModelProcess {

  def runALS(ss: SparkSession,
                      conf: TaskConf,
                      dataFrame: DataFrame): DataFrame = {
    // als model params.
    val rank = conf.options.getOrElse("rank", "10").toInt
    val maxIter = conf.options.getOrElse("maxIter", "5").toInt
    val regParam = conf.options.getOrElse("regParam", "0.01").toDouble
    val userCol = conf.options.getOrElse("userCol", "userId")
    val itemCol = conf.options.getOrElse("itemCol", "movieId")
    val ratingCol = conf.options.getOrElse("ratingCol", "rating")

//    assert(rank == numDimensions)

    val als = new ALS()
      .setRank(rank)
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setUserCol(userCol)
      .setItemCol(itemCol)
      .setRatingCol(ratingCol)

    val model = als.fit(dataFrame)

    model.itemFactors
  }

  def buildAnnoyIndex(conf: TaskConf,
                      dataFrame: DataFrame): Unit = {
    // annoy tree params.
    val itemFactorsPath = conf.options("itemFactors")
    val tempPath = conf.options.getOrElse("tempPath", "/tmp")

    val tempInputPath = tempPath + "/_tmp"

    val annoyResultPath = conf.options("path")
    val numDimensions = conf.options.getOrElse("dimensions", "10").toInt

    FileUtil.fullyDelete(new File(tempInputPath))

    saveFeatures(dataFrame, itemFactorsPath)
    copyToLocal(dataFrame.sparkSession.sparkContext.hadoopConfiguration, itemFactorsPath, tempInputPath)

    FileUtil.fullyDelete(new File(annoyResultPath))

    Annoy.create[Int](s"${tempInputPath}", numDimensions, outputDir = s"$annoyResultPath", Angular)
  }

  def saveFeatures(dataFrame: DataFrame,
                   outputPath: String,
                   idCol: String = "id",
                   featuresCol: String = "features"): Unit = {

    import dataFrame.sparkSession.implicits._

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

class ALSModelProcess(conf: TaskConf) extends org.apache.s2graph.s2jobs.task.Process(conf) {
  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {
    ALSModelProcess.runALS(ss, conf, inputMap.head._2)
  }
  override def mandatoryOptions: Set[String] = Set.empty
}

class AnnoyIndexBuildSink(queryName: String, conf: TaskConf) extends Sink(queryName, conf) {
  override val FORMAT: String = "parquet"

  override def mandatoryOptions: Set[String] = Set("path", "itemFactors")

  override def write(inputDF: DataFrame): Unit = {
    val df = repartition(preprocess(inputDF), inputDF.sparkSession.sparkContext.defaultParallelism)

    if (inputDF.isStreaming) throw new IllegalStateException("AnnoyIndexBuildSink can not be run as streaming.")
    else {
      ALSModelProcess.buildAnnoyIndex(conf, inputDF)
    }
  }
}
