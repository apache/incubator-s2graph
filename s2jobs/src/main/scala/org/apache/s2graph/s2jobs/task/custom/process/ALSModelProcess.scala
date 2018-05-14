/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.s2jobs.task.custom.process

import java.io.File

import annoy4s._
import org.apache.spark.ml.evaluation.RegressionEvaluator
//import org.apache.spark.ml.nn.Annoy

//import annoy4s.{Angular, Annoy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.s2graph.s2jobs.task.{Sink, TaskConf}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ALSModelProcess {

  def runALS(ss: SparkSession,
             conf: TaskConf,
             dataFrame: DataFrame): DataFrame = {
    // split
    val Array(training, test) = dataFrame.randomSplit(Array(0.8, 0.2))

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

    val model = als.fit(training)
    model.setColdStartStrategy("drop")
    
    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol(ratingCol)
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"RMSE: ${rmse}")

    model.itemFactors
  }

//  def buildAnnoyIndex(conf: TaskConf,
//                      dataFrame: DataFrame): Unit = {
//    val ann = new Annoy()
//      .setNumTrees(2)
//      .setFraction(0.1)
//      .setIdCol("id")
//      .setFeaturesCol("features")
//
//    val itemAnnModel = ann.fit(dataFrame)
//    itemAnnModel.saveAsAnnoyBinary(conf.options("itemFactors"))
//  }
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
