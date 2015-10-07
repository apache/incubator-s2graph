package spark.hbase

import java.io.Serializable
import java.util
import java.util.{Comparator, TreeSet, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.hadoop.tools.{DistCpConstants, DistCp, DistCpOptions}
import org.apache.hadoop.util.ToolRunner
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

class HFileRDD(rdd: RDD[KeyValue]) extends Serializable {

  class KeyFamilyQualifier(val rowKey: Array[Byte], val family: Array[Byte], val qualifier: Array[Byte])
    extends Comparable[KeyFamilyQualifier] with Serializable {
    override def compareTo(o: KeyFamilyQualifier): Int = {
      var result = Bytes.compareTo(rowKey, o.rowKey)
      if (result == 0) {
        result = Bytes.compareTo(family, o.family)
        if (result == 0) result = Bytes.compareTo(qualifier, o.qualifier)
      }
      result
    }

    override def toString: String = {
      Bytes.toString(rowKey) + ":" + Bytes.toString(family) + ":" + Bytes.toString(qualifier)
    }
  }


  case class BulkLoadPartitioner(startKeys: Array[Array[Byte]]) extends Partitioner {

    override def numPartitions: Int = startKeys.length

    override def getPartition(key: Any): Int = {

      val rowKey: Array[Byte] =
        key match {
          case qualifier: KeyFamilyQualifier =>
            qualifier.rowKey
          case _ =>
            key.asInstanceOf[Array[Byte]]
        }

      val comparator: Comparator[Array[Byte]] = new Comparator[Array[Byte]] {
        override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
          Bytes.compareTo(o1, o2)
        }
      }
      val partition = util.Arrays.binarySearch(startKeys, rowKey, comparator)
      if (partition < 0) partition * -1 + -2
      else partition
    }
  }

  //  def toHBaseBulk(hbaseConf: Configuration,
  //                  tableName: String,
  //                  numFilesPerRegion: Int,
  //                  tmpPath: String,
  //                  outputPath: String,
  //                  maxMaps: Int,
  //                  maxBandWidth: Int) = {
  //    val hTable = new HTable(hbaseConf, TableName.valueOf(tableName))
  //    try {
  //
  //      val job = toHFile(hbaseConf, tableName, numFilesPerRegion, tmpPath)
  ////      loadHFile(job, new Path(tmpPath), new Path(outputPath), hTable, maxMaps, maxBandWidth)
  //    } finally {
  //      hTable.close()
  //    }
  //  }

  //  private def distCp(conf: Configuration,
  //                     srcPath: Path, tgtPath: Path,
  //                     maxMaps: Int, mapBandWidth: Int) = {
  //    val distCpOption = new DistCpOptions(List(srcPath), tgtPath)
  //    distCpOption.setMaxMaps(maxMaps)
  //    distCpOption.setMapBandwidth(mapBandWidth)
  //    distCpOption.setOverwrite(true)
  //
  //    conf.set(DistCpConstants.CONF_LABEL_BANDWIDTH_MB, s"$mapBandWidth")
  //    conf.set(DistCpConstants.CONF_LABEL_MAX_MAPS, s"$maxMaps")
  //    println(s"DistCp: $conf")
  //    val job = new DistCp(conf, distCpOption)
  ////    job.run(Array("-overwrite", "-m", maxMaps, "-bandwidth", mapBandWidth, srcPath, tgtPath).map(_.toString))
  ////    job.execute()
  //    ToolRunner.run(conf, job, Array("-overwrite", "-m", maxMaps, "-bandwidth", mapBandWidth, srcPath, tgtPath).map(_.toString))
  //  }

  //  def loadHFile(job: Job,
  //                srcPath: Path,
  //                tgtPath: Path,
  //                hTable: HTable,
  //                maxMaps: Int,
  //                maxBandWidth: Int) = {
  //    val conf = job.getConfiguration
  //    val fs = FileSystem.get(conf)
  //    val rwx = new FsPermission("777")
  //
  //    try {
  //      setRecursivePermission(srcPath)
  //      distCp(conf, srcPath, tgtPath, maxMaps, maxBandWidth)
  //
  //
  //      def setRecursivePermission(path: Path): Unit = {
  //        val listFiles = fs.listStatus(path)
  //        listFiles foreach { f =>
  //          val p = f.getPath
  //          fs.setPermission(p, rwx)
  //          if (f.isDirectory && p.getName != "_tmp") {
  //            // create a "_tmp" folder that can be used for HFile splitting, so that we can
  //            // set permissions correctly. This is a workaround for unsecured HBase. It should not
  //            // be necessary for SecureBulkLoadEndpoint (see https://issues.apache.org/jira/browse/HBASE-8495
  //            // and http://comments.gmane.org/gmane.comp.java.hadoop.hbase.user/44273)
  //            FileSystem.mkdirs(fs, new Path(p, "_tmp"), rwx)
  //            setRecursivePermission(p)
  //          }
  //        }
  //      }
  //
  //
  //      val lih = new LoadIncrementalHFiles(hTable.getConfiguration)
  //      lih.doBulkLoad(tgtPath, hTable)
  //    } finally {
  //      //      fs.deleteOnExit(tgtPath)
  //      fs.deleteOnExit(new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration)))
  //    }
  //  }

  def toHFile(hbaseConf: Configuration, tableName: String, numFilesPerRegion: Int, tmpPath: String): Job = {
    val hTable = new HTable(hbaseConf, TableName.valueOf(tableName))

    val job = Job.getInstance(hbaseConf, this.getClass.getName.split('$')(0))
    job.getConfiguration.set("hadoop.tmp.dir", s"/tmp/${tableName}_${UUID.randomUUID()}}")
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    HFileOutputFormat2.configureIncrementalLoad(job, hTable)

    //    val fs = FileSystem.get(hbaseConf)
    //    val hFilePath = new Path(tmpPath)
    //    fs.makeQualified(hFilePath)
    rdd.map { kv => new KeyFamilyQualifier(kv.getRow, kv.getFamily, kv.getQualifier) -> (kv.getValue, kv.getTimestamp)}
      .repartitionAndSortWithinPartitions(new BulkLoadPartitioner(hTable.getStartKeys))
      .map { case (key, (value, ts)) =>
      new ImmutableBytesWritable(key.rowKey) -> new KeyValue(key.rowKey, key.family, key.qualifier, ts, value)
    }.saveAsNewAPIHadoopFile(tmpPath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    job

  }
}