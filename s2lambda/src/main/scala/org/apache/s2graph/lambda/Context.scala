package org.apache.s2graph.lambda

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import scala.util.Try

case class Context(jobId: String, rootDir: String, comment: String, sparkContext: SparkContext) {

  val sqlContext = Try {
    new HiveContext(sparkContext)
  } getOrElse {
    println("Fail to get HiveContext, returns SQLContext instead")
    new SQLContext(sparkContext)
  }

  val BulkDirSuffix = "bulk"

  val batchId: String = System.currentTimeMillis().toString

  val bulkDir = rootDir + "/" + BulkDirSuffix

  val batchDir: String = s"$rootDir/$batchId"

  val lastBatchId: String = {
    try {
      val fs = FileSystem.get(sparkContext.hadoopConfiguration)
      val status = fs.listStatus(new Path(rootDir)).map(_.getPath.getName)
      status.filter(_ != BulkDirSuffix).filter(_ != batchId) match {
        case empty if empty.isEmpty => batchId
        case nonEmpty => nonEmpty.max
      }
    } catch {
      case _: Throwable => batchId
    }
  }

  println("globalContext: " + JsonUtil.toPrettyJsonString(this))

}
