package org.apache.s2graph.s2jobs.utils

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.s2graph.core.Management
import org.apache.s2graph.core.schema.Schema
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.s2jobs._
import org.apache.s2graph.s2jobs.task.{EmptySource, Source, TaskConf}
import org.apache.spark.sql.SparkSession
import scalikejdbc.WrappedResultSet
import scalikejdbc.interpolation.SQLSyntax

object TxId {
  def apply(rs: WrappedResultSet): TxId = {
    TxId(
      rs.string("job_name"),
      rs.string("app_name"),
      rs.string("source_name"),
      rs.string("path")
    )
  }
}

case class TxId(jobName: String,
                appName: String,
                sourceName: String,
                path: String)

object TxLog {
  def apply(rs: WrappedResultSet): TxLog = {
    TxLog(
      rs.long("id"),
      TxId(rs),
      CommitType(rs.string("commit_type")),
      rs.string("commit_id"),
      rs.long("last_modified_at"),
      rs.booleanOpt("enabled")
    )
  }
}

case class TxLog(id: Long,
                 txId: TxId,
                 commitType: CommitType,
                 commitId: String,
                 lastModifiedAt: Long,
                 enabled: Option[Boolean])

object CommitType {
  def apply(typeName: String): CommitType = typeName match {
    case "time" => TimeType
    case "path" => PathType
    case "hive" => HiveType
  }
}

trait CanCommit[A] {
  def toCommitId(a: A): String
}

object CanCommit {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val failed: Long = 0L

  implicit val longCommitId: CanCommit[Long] = new CanCommit[Long] {
    override def toCommitId(time: Long): String =
      if (time == failed) "Failed to fetch modification time"
      else dateFormat.format(new Date(time))
  }

  implicit val fileStatusCommitId: CanCommit[FileStatus] = new CanCommit[FileStatus] {
    override def toCommitId(fileStatus: FileStatus): String = fileStatus.getPath.toUri.getPath
  }
}

trait CommitType {
  def commitId[A: CanCommit](value: A): String = implicitly[CanCommit[A]].toCommitId(value)

  def toColumnValue: String
}

/**
 * Compares numeric ordering of modification time of file sources.
 * commit_id is datetime of latest read directory / file's modification time.
 */
case object TimeType extends CommitType {
  override def toColumnValue: String = "time"
}

/**
 * Compares lexicographic ordering of paths
 * commit_id is path of latest read directory / file.
 */
case object PathType extends CommitType {
  override def toColumnValue: String = "path"
}

/**
 * Compares numeric ordering of modification time of hive sources.
 * commit_id is datetime of hive sources modification time.
 */
case object HiveType extends CommitType {
  override def toColumnValue: String = "hive"
}


/**
 * TransactionUtil supports incremental processing of spark batch jobs by recording
 * each latest path(or partition) processed of Filesource.
 *
 * Incremental processing of Hive tables are also supported by recording each tables modification time.
 * HiveSource in incremental job will be processed only when its modification time has been updated excluding first time read..
 *
 */
object TransactionUtil {
  val BaseTxTableKey = "baseTxTable"
  val typeKey = "type"

  def apply(config: Config, options: Map[String, String] = Map.empty): TransactionUtil = {
    DBTransactionUtil(config, options)
  }


  /** Return filter function according to CommitType
   *
   * @param fs
   * @param lastTx
   * @return
   */
  def toFileFilterFunc(fs: FileSystem, lastTx: Option[TxLog]): FileStatus => Boolean = (fileStatus: FileStatus) => {
    val valid = lastTx match {
      case None => true
      case Some(tx) =>
        tx.commitType match {
          case PathType => fileStatus.getPath.toUri.getPath > tx.commitId
          case TimeType => fileStatus.getModificationTime > tx.lastModifiedAt
        }
    }

    valid && (!HDFSUtil.isEmptyDirectory(fs, fileStatus.getPath))
  }


  /** list subdirectory within given partitions with was modified after last transaction log
   *
   * @param ss
   * @param basePath
   * @param partitionKeys
   * @param lastTx
   * @param readLeafFiles
   */
  def listAllPath(ss: SparkSession,
                  basePath: String,
                  partitionKeys: Seq[String],
                  lastTx: Option[TxLog] = None,
                  readLeafFiles: Boolean = false): Seq[FileStatus] = {
    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
    val filterFunc = toFileFilterFunc(fs, lastTx)

    if (readLeafFiles) HDFSUtil.getFilteredFiles(ss, basePath, recursive = true)(filterFunc).toSeq
    else {
      val path =
        if (partitionKeys.isEmpty) new Path(s"$basePath")
        else {
          val postfix: String = partitionKeys.map(k => s"$k=*").mkString("/")
          new Path(s"$basePath/$postfix")
        }

      val ls = fs.globStatus(path, new PathFilter {
        override def accept(path: Path): Boolean =
          !path.getName.startsWith("_")
      })

      ls.filter(filterFunc)
    }
  }

  def copySource(source: Source, paths: String): Source = {
    val newTaskConf = source.conf.copy(options = source.conf.options ++ Map("paths" -> paths))
    JobDescription.getSource(newTaskConf)
  }

  /**
   * Map each FileSource and its each path with corresponding
   * incremental sub-partitions(directories) after last TxLog
   *
   * parameters for FileSource in Incremental Jobs
   * every parameters are optional
   *
   * - partition_keys:         TxLog will record latest path within given partition.
   *                           if not specified, TxLog will record just the given path
   *                           only when the path has been modified after last TxLog
   *
   * - initial_offset:         file or partition count to read at initial read of source
   *
   * - incomplete_partitions:  if incomplete partition number K is specified,
   *                           incremental job will skip latest K partitions of given path
   *
   * - max_paths:              number of maximum paths count to be read in each interval
   *
   * - read_leaf_file:         given "true", Incremental Job will record leaf file as TxLog
   *                           [Warning] This option can cause too many tasks in spark job
   *                           trying to merge multiple paths in to one data frame
   *                           when reading the source.
   *
   * - skip_mapping:           given "true", Incremental Job will skip to map sources each paths.
   *
   *
   *
   * @param fileSources
   * @param ss
   * @param jobName
   * @param jobDesc
   * @param txUtil
   */
  def toTargetsPerFileSourcePath(fileSources: Seq[Source],
                                 ss: SparkSession,
                                 jobName: String,
                                 jobDesc: JobDescription,
                                 txUtil: TransactionUtil): Map[Source, Map[String, Seq[FileStatus]]] = {
    fileSources.map { source =>
      val sourceName = source.getName
      val paths = source.conf.options("paths").split(",")
      val partitionKeys = source.conf.options.get("partition_keys")
        .map { s => s.split(",").map(_.trim).filter(_.nonEmpty).toSeq }.getOrElse(Nil)

      val initialOffset = source.conf.options.get("initial_offset").map(_.toInt)
      val incompletePartitions = source.conf.options.get("incomplete_partitions").map(_.toInt)
      val maxPaths = source.conf.options.get("max_paths").map(_.toInt)

      val readLeafFile = source.conf.options.getOrElse("read_leaf_file", "false") == "true"
      val skipMapping = source.conf.options.getOrElse("skip_mapping", "false") == "true"

      val innerMap = paths.map { path =>
        if (skipMapping) path -> Seq(HDFSUtil.getFileStatus(ss, path))
        else {
          val txId = TxId(jobName, jobDesc.name, sourceName, path)
          val lastTx = txUtil.readTx(txId)

          val filteredTargets =
            listAllPath(ss, path, partitionKeys, lastTx, readLeafFile)
              .dropRight(incompletePartitions.getOrElse(0))
              .take(maxPaths.getOrElse(Int.MaxValue))

          val targets = lastTx match {
            case Some(_) => filteredTargets
            case None => initialOffset.map(k => filteredTargets.takeRight(k)).getOrElse(filteredTargets)
          }

          path -> targets
        }
      }.toMap

      source -> innerMap
    }.toMap
  }

  /**
   * Replace each FileSources to read paths specified at targetsPerFileSourcePath.
   *
   * @param targetsPerFileSourcePath
   * @param ss
   * @param jobName
   * @param jobDesc
   * @param txUtil
   */
  def toNewFileSources(targetsPerFileSourcePath: Map[Source, Map[String, Seq[FileStatus]]],
                       ss: SparkSession,
                       jobName: String,
                       jobDesc: JobDescription,
                       txUtil: TransactionUtil): Seq[Source] = {
    targetsPerFileSourcePath.toSeq.map { case (source, innerMap) =>
      val isEmptySource = innerMap.values.forall(_.isEmpty)
      val sourceName = source.getName

      if (isEmptySource) {
        val (path, _) = innerMap.head
        val txId = TxId(jobName, jobDesc.name, sourceName, path)
        val lastTx = txUtil.readTx(txId)

        val lastPath = lastTx.map(_.commitId)
          .orElse(HDFSUtil.getLatestFile(ss, path, recursive = true).map(_.toUri.getPath))
          .getOrElse(throw new IllegalStateException(s"last commitId or latest path on source's paths should return any file."))

        val tmpSource = copySource(source, lastPath)

        val schemaInJson = tmpSource.toDF(ss).schema.json
        val newTaskConf = source.conf.copy(
          `type` = "custom",
          options = source.conf.options ++ Map("schema" -> schemaInJson, "class" -> classOf[EmptySource].getName)
        )

        EmptySource(newTaskConf)
      } else {
        copySource(source, innerMap.values.flatten.map(_.getPath.toUri.getPath).mkString(","))
      }
    }
  }

  /**
   * Write TxLog of FileSources
   *
   * @param targetsPerSourcePath
   * @param jobName
   * @param jobDesc
   * @param txUtil
   */
  def writeFileTxLog(targetsPerSourcePath: Map[Source, Map[String, Seq[FileStatus]]],
                     jobName: String,
                     jobDesc: JobDescription,
                     txUtil: TransactionUtil): Unit = {
    import CanCommit._

    targetsPerSourcePath.foreach { case (source, innerMap) =>
      val sourceName = source.getName


      innerMap.foreach { case (path, targets) =>
        val txId = TxId(jobName, jobDesc.name, sourceName, path)

        targets.lastOption.foreach { target =>
          val lastModifiedAt = target.getModificationTime
          val commitType = CommitType(source.conf.options.getOrElse("commit_type", "path"))
          val commitId = commitType.commitId(target)

          val txLog = TxLog(0, txId, commitType = commitType, commitId = commitId,
            lastModifiedAt = lastModifiedAt, enabled = None)

          txUtil.writeTx(txLog)
        }
      }
    }
  }

  /**
   * Map each HiveSources with its modification time
   *
   * @param hiveSources
   * @param ss
   * @param jobName
   * @param jobDesc
   * @param txUtil
   */
  def toHiveSourcesModTimeMap(hiveSources: Seq[Source],
                              ss: SparkSession,
                              jobName: String,
                              jobDesc: JobDescription,
                              txUtil: TransactionUtil): Map[Source, Long] = {
    hiveSources.map { source =>
      val table = source.conf.options("table")
      val database = source.conf.options("database")
      val sourceName = source.getName

      val txId = TxId(jobName, jobDesc.name, sourceName, s"$database.$table")
      val lastTx = txUtil.readTx(txId)

      val tableSchemaJson = HiveUtil.getTableSchema(ss, table, database).json

      val newTaskConf = source.conf.copy(
        `type` = "custom",
        options = source.conf.options ++ Map("schema" -> tableSchemaJson, "class" -> classOf[EmptySource].getName)
      )

      val emptySource = EmptySource(newTaskConf)

      lastTx match {
        case Some(txLog: TxLog) =>
          val txLastModifiedAt = txLog.lastModifiedAt
          val tableLastModifiedAt = HiveUtil.getTableModificationTime(ss, table, database)

          tableLastModifiedAt.map { time =>
            if (time > txLastModifiedAt) source -> time
            else emptySource -> time
          }.getOrElse(source -> CanCommit.failed)

        case None => source -> HiveUtil.getTableModificationTime(ss, table, database).getOrElse(0L)
      }
    }.toMap
  }

  /**
   * Write TxLog of HiveSources
   *
   * @param hiveSourcesMap
   * @param jobName
   * @param jobDesc
   * @param txUtil
   */
  def writeHiveTxLog(hiveSourcesMap: Map[Source, Long],
                     jobName: String,
                     jobDesc: JobDescription,
                     txUtil: TransactionUtil): Unit = {
    import CanCommit._

    hiveSourcesMap.foreach { case (source, time) =>
      val table = source.conf.options("table")
      val database = source.conf.options("database")
      val sourceName = source.getName

      val txId = TxId(jobName, jobDesc.name, sourceName, s"$database.$table")
      val commitId = HiveType.commitId(time)

      val txLog = TxLog(0, txId, commitType = HiveType, commitId = commitId,
        lastModifiedAt = time, enabled = None)

      txUtil.writeTx(txLog)
    }
  }


  /**
   * Main function of TransactionUtil
   *
   * @param ss
   * @param jobName
   * @param jobDesc
   * @param txUtil
   * @param func
   */
  def withTx(ss: SparkSession,
             jobName: String,
             jobDesc: JobDescription,
             txUtil: TransactionUtil)(func: Job => Unit): Unit = {
    val (incrementalSources, skipSources) = jobDesc.sources.partition { source =>
      source.conf.options.getOrElse("skip_tx", "false") != "true"
    }

    val (fileSources, __otherSources) = incrementalSources.partition { source =>
      source.conf.options.contains("paths")
    }

    val (hiveSources, _otherSources) = __otherSources.partition { source =>
      source.conf.`type` == "hive"
    }

    val otherSources = _otherSources ++ skipSources

    val targetsPerSourcePath: Map[Source, Map[String, Seq[FileStatus]]] = toTargetsPerFileSourcePath(fileSources, ss, jobName, jobDesc, txUtil)
    val newFileSources: Seq[Source] = toNewFileSources(targetsPerSourcePath, ss, jobName, jobDesc, txUtil)
    val hiveSourceMap = toHiveSourcesModTimeMap(hiveSources, ss, jobName, jobDesc, txUtil)
    val newHiveSources = hiveSourceMap.keySet

    val newJobDesc = jobDesc.copy(sources = newFileSources ++ newHiveSources ++ otherSources)
    val newJob = new Job(ss, newJobDesc)

    func(newJob)

    writeFileTxLog(targetsPerSourcePath, jobName, jobDesc, txUtil)
    writeHiveTxLog(hiveSourceMap, jobName, jobDesc, txUtil)
  }
}

trait TransactionUtil {

  import TransactionUtil._

  val config: Config

  val baseTxTableRaw =
    if (config.hasPath(BaseTxTableKey)) config.getString(BaseTxTableKey)
    else "tx_logs"

  val baseTxTable = SQLSyntax.createUnsafely(baseTxTableRaw)

  /**
   * read last committed transaction using context provided from spark session.
   *
   * @param txId
   * @return
   */
  def readTx(txId: TxId): Option[TxLog]

  /**
   * write latest target as last committed transaction
   *
   * @param txLog
   */
  def writeTx(txLog: TxLog): Unit
}

/**
 * the underlying JDBC Driver is singleton, so be sure to initialize singleton instance of this.
 * currently only expect FileSource. need to add HiveSource support.
 *
 * @param config
 */
case class DBTransactionUtil(config: Config,
                             options: Map[String, String] = Map.empty) extends TransactionUtil {

  import scalikejdbc._

  var initialized = false

  init(config, options)

  private def init(config: Config, options: Map[String, String] = Map.empty): Boolean = {
    val optionConf = Management.toConfig(TaskConf.parseMetaStoreConfigs(options) ++ TaskConf.parseLocalCacheConfigs(options))
    val mergedConfig = optionConf.withFallback(config)

    synchronized {
      if (!initialized) {
        logger.info(s"[SCHEMA]: initializing...")
        logger.info(s"[SCHEMA]: $mergedConfig")

        logger.info(s"[SCHEMA]: ${config.root().render(ConfigRenderOptions.concise())}")

        Schema.apply(mergedConfig)

        initialized = true
      }

      initialized
    }
  }

  /**
   * read last committed transaction using context provided from spark session.
   *
   * @param txId
   * @return
   */
  override def readTx(txId: TxId): Option[TxLog] = {
    DB readOnly { implicit session =>
      readTxInner(txId)
    }
  }

  private def readTxInner(txId: TxId)(implicit session: DBSession): Option[TxLog] = {
    sql"""
          SELECT  *
          FROM    ${baseTxTable}
          WHERE   job_name = ${txId.jobName}
          AND     app_name = ${txId.appName}
          AND     source_name = ${txId.sourceName}
          AND     path = ${txId.path}
      """.map { rs =>
      TxLog.apply(rs)
    }.single().apply()
  }

  private def writeTxInner(txLog: TxLog)(implicit session: DBSession): Unit = {
    sql"""
          INSERT INTO
            ${baseTxTable}(
              job_name, app_name, source_name, path, commit_type, commit_id,
              last_modified_at, enabled, updated_at
            )
          VALUES
            (
              ${txLog.txId.jobName}, ${txLog.txId.appName}, ${txLog.txId.sourceName}, ${txLog.txId.path},
              ${txLog.commitType.toColumnValue}, ${txLog.commitId}, ${txLog.lastModifiedAt}, ${txLog.enabled}, now()
            )
          ON DUPLICATE KEY UPDATE
            commit_id = ${txLog.commitId},
            last_modified_at = ${txLog.lastModifiedAt},
            enabled = ${txLog.enabled},
            updated_at = now()
       """.execute().apply()
  }

  /**
   * write latest target as last committed transaction
   *
   * @param txLog
   */
  override def writeTx(txLog: TxLog): Unit = {
    DB localTx { implicit session =>
      try {
        writeTxInner(txLog)
      } catch {
        case ex: Exception =>
          logger.error(s"write tx failed. $txLog")
      }
    }
  }
}

