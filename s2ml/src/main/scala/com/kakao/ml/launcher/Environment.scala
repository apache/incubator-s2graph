package com.kakao.ml.launcher

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

object Environment {

  private var jobId: String = null
  
  private var rootDir: String = null

  private var comment = ""

  private var env: Map[String, Any] = Map.empty[String, Any]

  private val batchId: String = System.currentTimeMillis().toString

  private var lastBatchId: String = null

  private var sparkContext: SparkContext = null

}

trait Environment {

  private val bulkDirSuffix = "bulk"

  def setJobId(jobId: String) {
    Environment.jobId = jobId
  }

  def setRootDir(rooDir: String) {
    Environment.rootDir = rooDir
  }

  def setSparkContext(sc: SparkContext) {
    Environment.sparkContext = sc
  }

  def setComment(comment: String) {
    Environment.comment = comment
  }

  def setEnv(env: Map[String, Any]) {
    Environment.env = env
  }

  lazy val jobId: String = {
    require(Environment.jobId != null)
    Environment.jobId
  }

  lazy val rootDir: String = {
    require(Environment.rootDir != null)
    Environment.rootDir
  }

  lazy val bulkDir: String = {
    require(Environment.rootDir != null)
    Environment.rootDir + '/' + bulkDirSuffix
  }

  lazy val sparkContext: SparkContext = {
    require(Environment.sparkContext != null)
    Environment.sparkContext
  }

  lazy val env: Map[String, Any] = Environment.env

  lazy val lastBatchId: String = {
    if (Environment.lastBatchId == null) {
      try {
        val fs = FileSystem.get(sparkContext.hadoopConfiguration)
        val status = fs.listStatus(new Path(rootDir)).map(_.getPath.getName)
        Environment.lastBatchId =
            status.filter(_ != bulkDirSuffix).filter(_ != batchId) match {
              case empty if empty.isEmpty => batchId
              case nonEmpty => nonEmpty.max
            }
      } catch {
        case _: Throwable => Environment.lastBatchId = batchId
      }
    }
    Environment.lastBatchId
  }

  lazy val batchId: String = Environment.batchId

  lazy val batchDir: String = s"$rootDir/$batchId"

  lazy val lastBatchDir: String = s"$rootDir/$lastBatchId"

  lazy val comment: String = Environment.comment

}
