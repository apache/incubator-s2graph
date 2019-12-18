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

  def isEmptyDirectory(fs: FileSystem, path: Path): Boolean = {
    val ls = fs.listStatus(path)
    ls.isEmpty || (ls.size == 1 && isMetaFile(ls.head.getPath))
  }

  def isMetaFile(path: Path): Boolean = path.getName.startsWith("_")

  def reducePaths(fs: FileSystem, paths: Set[Path]): Seq[Path] = {
    val reduced = paths.groupBy(path => path.getParent).foldLeft(Seq[Path]()){ case (ls, (parentPath, childPaths)) =>
        val allPathsInParent = fs.listStatus(parentPath).map(_.getPath).filterNot(isMetaFile).toSet
        if (allPathsInParent equals childPaths) ls :+ parentPath
        else ls ++ childPaths
    }

    if (reduced.toSet equals paths) reduced
    else reducePaths(fs, reduced.toSet)
  }

  def reducePaths(ss: SparkSession, paths: Set[FileStatus]): Seq[FileStatus] = {
    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
    reducePaths(fs, paths.map(_.getPath)).map(fs.getFileStatus)
  }
}

