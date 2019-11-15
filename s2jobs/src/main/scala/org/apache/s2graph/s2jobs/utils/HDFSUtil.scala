package org.apache.s2graph.s2jobs.utils

import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.SparkSession


case class RemoteIteratorWrapper[A](remoteIterator: RemoteIterator[A])
  extends scala.collection.AbstractIterator[A]
    with scala.collection.Iterator[A] {

  override def hasNext: Boolean = remoteIterator.hasNext

  def next: A = remoteIterator.next
}

object HDFSUtil {
  def getFilteredFiles(ss: SparkSession,
                       basePath: String,
                       recursive: Boolean)(filterFunc: FileStatus => Boolean): Iterator[FileStatus] = {
    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
    val iter = fs.listFiles(new Path(basePath), recursive)
    RemoteIteratorWrapper(iter).filter(filterFunc)
  }

  def getFileStatus(ss: SparkSession, path: String): FileStatus = {
    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
    fs.getFileStatus(new Path(path))
  }

  def getLatestFile(ss: SparkSession,
                    baseDir: String,
                    recursive: Boolean): Option[Path] = {
    var latestPath: LocatedFileStatus = null

    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)

    val iter = fs.listFiles(new Path(baseDir), recursive)
    while (iter.hasNext) {
      val current = iter.next()
      val skipThis = current.getPath.getName.startsWith("_")
      if (!skipThis) {
        val latestModifiedTime = if (latestPath == null) 0L else latestPath.getModificationTime
        if (latestModifiedTime < current.getModificationTime) {
          latestPath = current
        }
      }
    }

    Option(latestPath).map(_.getPath)
  }
}

