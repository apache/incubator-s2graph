package org.apache.s2graph.core.model

import java.io.File

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.s2graph.core.{Fetcher, S2GraphLike}
import org.apache.s2graph.core.utils.logger

import scala.concurrent.{ExecutionContext, Future}

object Importer {
  def toHDFSConfiguration(hdfsConfDir: String): Configuration = {
    val conf = new Configuration

    val hdfsConfDirectory = new File(hdfsConfDir)
    if (hdfsConfDirectory.exists()) {
      if (!hdfsConfDirectory.isDirectory || !hdfsConfDirectory.canRead) {
        throw new IllegalStateException(s"HDFS configuration directory ($hdfsConfDirectory) cannot be read.")
      }

      val path = hdfsConfDirectory.getAbsolutePath
      conf.addResource(new Path(s"file:///$path/core-site.xml"))
      conf.addResource(new Path(s"file:///$path/hdfs-site.xml"))
    } else {
      logger.warn("RocksDBImporter doesn't have valid hadoop configuration directory..")
    }
    conf
  }
}

trait Importer {
  @volatile var isFinished: Boolean = false
  def run(config: Config)(implicit ec: ExecutionContext): Future[Importer]

  def status: Boolean = isFinished

  def setStatus(otherStatus: Boolean): Boolean = {
    this.isFinished = otherStatus
    this.isFinished
  }
//  def status: ImportStatus

//  def getImportedStorage(graphExecutionContext: ExecutionContext): Storage[_, _]
  def close(): Unit
}
case class IdentityImporter(graph: S2GraphLike) extends Importer {
  override def run(config: Config)(implicit ec: ExecutionContext): Future[Importer] = {
    Future.successful(this)
  }

  override def close(): Unit = {}
}
object HDFSImporter {
  import scala.collection.JavaConverters._
  val PathsKey = "paths"
  val HDFSConfDirKey = "hdfsConfDir"

  def extractPaths(config: Config): Map[String, String] = {
    config.getConfigList(PathsKey).asScala.map { e =>
      val key = e.getString("src")
      val value = e.getString("tgt")

      key -> value
    }.toMap
  }
}
case class HDFSImporter(graph: S2GraphLike) extends Importer {

  import HDFSImporter._

  override def run(config: Config)(implicit ec: ExecutionContext): Future[Importer] = {
    Future {
      val paths = extractPaths(config)
      val hdfsConfiDir = config.getString(HDFSConfDirKey)

      val hadoopConfig = Importer.toHDFSConfiguration(hdfsConfiDir)
      val fs = FileSystem.get(hadoopConfig)

      def copyToLocal(remoteSrc: String, localSrc: String): Unit = {
        val remoteSrcPath = new Path(remoteSrc)
        val localSrcPath = new Path(localSrc)

        fs.copyToLocalFile(remoteSrcPath, localSrcPath)
      }

      paths.foreach { case (srcPath, tgtPath) =>
        copyToLocal(srcPath, tgtPath)
      }

      this
    }
  }

//  override def status: ImportStatus = ???

  override def close(): Unit = {}
}