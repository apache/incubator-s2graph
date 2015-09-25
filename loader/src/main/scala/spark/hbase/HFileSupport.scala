package spark.hbase

import java.util.{TreeSet, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.apache.hadoop.util.ToolRunner
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

class HFileRDD extends Serializable {

  private class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) extends Partitioner {
    val fraction = 1 max numFilesPerRegion min conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)

    override def getPartition(key: Any): Int = {
      def bytes(n: Any) = n match {
        case s: String => Bytes.toBytes(s)
      }

      val h = (key.hashCode() & Int.MaxValue) % fraction
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(bytes(key), splits(i)) < 0) return (i - 1) * fraction + h

      (splits.length - 1) * fraction + h
    }

    override def numPartitions: Int = splits.length * fraction
  }

  implicit val bytesOrdering = new Ordering[ImmutableBytesWritable] {
    override def compare(a: ImmutableBytesWritable, b: ImmutableBytesWritable) = {
      Bytes.compareTo(a.get(), b.get())
    }
  }

  def distCp(conf: Configuration,
             srcPath: Path, tgtPath: Path,
             maxMaps: Int = 10, mapBandWidth: Int = 10) = {
    val distCpOption = new DistCpOptions(List(srcPath), tgtPath)
    distCpOption.setMaxMaps(maxMaps)
    distCpOption.setMapBandwidth(mapBandWidth)
    val distCpJob = new DistCp(conf, distCpOption)
    ToolRunner.run(conf, DistCp,  Array(srcPath.toString, tgtPath.toString, "-bandwidth", mapBandWidth, "-m", maxMaps).map(_.toString))
  }
  def loadHFile(fs: FileSystem,
                job: Job,
                hfilePath: Path,
                targetHFilePath: Path,
                hTable: HTable,
                conf: Configuration) = {
    try {
      val distCpOpt = new DistCpOptions(List(hfilePath), targetHFilePath)
      distCpOpt.setMapBandwidth(10)
      val distCpJob = new DistCp(conf, distCpOpt)
      distCpJob.run(Array[String]("-bandwidth"))
      val rwx = new FsPermission("777")
      def setRecursivePermission(path: Path): Unit = {
        val listFiles = fs.listStatus(path)
        listFiles foreach { f =>
          val p = f.getPath
          fs.setPermission(p, rwx)
          if (f.isDirectory && p.getName != "_tmp") {
            // create a "_tmp" folder that can be used for HFile splitting, so that we can
            // set permissions correctly. This is a workaround for unsecured HBase. It should not
            // be necessary for SecureBulkLoadEndpoint (see https://issues.apache.org/jira/browse/HBASE-8495
            // and http://comments.gmane.org/gmane.comp.java.hadoop.hbase.user/44273)
            FileSystem.mkdirs(fs, new Path(p, "_tmp"), rwx)
            setRecursivePermission(p)
          }
        }
      }
      setRecursivePermission(hfilePath)

      val lih = new LoadIncrementalHFiles(conf)
      lih.doBulkLoad(hfilePath, hTable)
    } finally {
      fs.deleteOnExit(hfilePath)
      fs.deleteOnExit(new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration)))
    }
  }

  def toHFile(rdd: RDD[KeyValue], tableName: String, cf: Array[Byte], numFilesPerRegion: Int, conf: Configuration) = {
    val hTable = new HTable(conf, TableName.valueOf(tableName))
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))

    HFileOutputFormat2.configureIncrementalLoad(job, hTable)

    val fs = FileSystem.get(conf)
    val hFilePath = new Path("/tmp", tableName + "_" + UUID.randomUUID())
    fs.makeQualified(hFilePath)

    rdd.groupBy { kv =>
      new ImmutableBytesWritable(kv.getRowArray, kv.getRowOffset, kv.getRowLength)
    }.repartitionAndSortWithinPartitions(new HFilePartitioner(conf, hTable.getStartKeys, numFilesPerRegion))
      .flatMap { case (hKey, kvs) =>
      val inner = new TreeSet[KeyValue](KeyValue.COMPARATOR)
      for {
        kv <- kvs
      } {
        inner.add(kv)
      }
      inner.toSeq map { kv => (hKey -> kv) }
    }.saveAsNewAPIHadoopFile(hFilePath.toString, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)


  }
}