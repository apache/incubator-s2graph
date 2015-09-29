package subscriber

import java.util.TreeSet

import com.daumkakao.s2graph.core.{Edge, Graph}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hbase.client.{HTable, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.hbase.async.{PutRequest}
import s2.spark.{HashMapParam, SparkApp}
import spark.hbase.HFileRDD

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.collection.JavaConversions.asScalaSet
import scala.util.Random

object TransferToHFile extends SparkApp {

  val usages =
    s"""
       |create HFiles for hbase table on zkQuorum specified.
       |note that hbase table is created already and pre-splitted properly.
       |
       |params:
       |0. input: hdfs path for tsv file(bulk format).
       |1. output: hdfs path for storing HFiles.
       |2. zkQuorum: running hbase cluster zkQuorum.
       |3. tableName: table name for this bulk upload.
       |4. dbUrl: db url for parsing to graph element.
     """.stripMargin

  //
  private class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) extends Partitioner {
    val fraction = 1 max numFilesPerRegion min conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)

    override def getPartition(key: Any): Int = {
      def bytes(n: Any) = n match {
        case s: String => Bytes.toBytes(s)
        case b: ImmutableBytesWritable => b.get()
      }

      val h = (key.hashCode() & Int.MaxValue) % fraction
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(bytes(key), splits(i)) < 0) return (i - 1) * fraction + h

      (splits.length - 1) * fraction + h
    }

    override def numPartitions: Int = splits.length * fraction
  }

  /*
      1270054885000	insertBulk	e	85418	97556	talk_friend	{"created_at": 1269799161000}
  */
  //TODO: Process AtomicIncrementRequest too.
  /** build key values */

  def toKeyValues(strs: Seq[String]): Iterator[KeyValue] = {
    val kvs = for {
      s <- strs
      edge <- Graph.toEdge(s).toSeq
      rpc <- edge.insert() if rpc.isInstanceOf[PutRequest]
      put = rpc.asInstanceOf[PutRequest]
    } yield {
        val p = put
        val kv = new KeyValue(p.key(), p.family(), p.qualifier, p.timestamp, p.value)

//        println(s"[Edge]: $edge\n[Put]: $p\n[KeyValue]: ${kv.getRow.toList}, ${kv.getQualifier.toList}, ${kv.getValue.toList}, ${kv.getTimestamp}")

        kv
      }
    kvs.toIterator
  }

  def buildCells(rdd: RDD[String], dbUrl: String, conf: Configuration, hTable: HTable, numFilesPerRegion: Int) = {

    val kvs = rdd.mapPartitions { iter =>

      val phase = System.getProperty("phase")
      GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")

      toKeyValues(iter.toSeq)
    }
    implicit val bytesOrdering = new Ordering[ImmutableBytesWritable] {
      override def compare(a: ImmutableBytesWritable, b: ImmutableBytesWritable) = {
        Bytes.compareTo(a.get(), b.get())
      }
    }

    val grouped = kvs.groupBy { kv =>
      val hKey = new ImmutableBytesWritable(kv.getRow())
      hKey
    }
    val sorted = grouped.repartitionAndSortWithinPartitions(new HFilePartitioner(conf, hTable.getStartKeys, numFilesPerRegion))

    val ret = sorted.flatMap { case (hKey, kvs) =>
        val inner = new TreeSet[KeyValue](KeyValue.COMPARATOR)
        for {
          kv <- kvs
        } {
          inner.add(kv)
        }
        inner.toSeq map { kv =>
//          println(s"[Key]: ${hKey.get.toList}")
//          println(s"[KeyValue]: ${kv}")
          (hKey -> kv)
        }

    }
    ret
    //
    //    implicit val ordering = new Ordering[KeyValue] {
    //      override def compare(a: KeyValue, b: KeyValue) = {
    //        KeyValue.COMPARATOR.compare(a, b)
    //      }
    //    }
    //
    //    kvs.repartition(numOfHFiles).sortBy { kv =>
    //      kv
    //    }.map { kv =>
    //      val hKey = new ImmutableBytesWritable(kv.getRow())
    ////      println(s"[Key]: ${hKey.get().toList}")
    ////      println(s"[Value]: ${kv}")
    //      (hKey -> kv)
    //    }

  }


  override def run() = {
    val input = args(0)
    val tmpPath = args(1)
    val outputPath = args(2)
    val zkQuorum = args(3)
    val tableName = args(4)
    val dbUrl = args(5)
    val maxHFilePerResionServer = if (args.length >= 7) args(6).toInt else 1

    val conf = sparkConf(s"$input: TransferToHFile")

    val sc = new SparkContext(conf)


    /** set up hbase init */
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hbaseConf.set("hadoop.tmp.dir", s"/tmp/$tableName")

//    val table = new HTable(hbaseConf, tableName)

    try {
//      val rdd = sc.textFile(input)
      val rdd = sc.textFile(input)
      val kvs = rdd.mapPartitions { iter =>

        val phase = System.getProperty("phase")
        GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")

        toKeyValues(iter.toSeq)
      }
      val newRDD = new HFileRDD(kvs)
      newRDD.toHBaseBulk(hbaseConf, tableName, maxHFilePerResionServer, tmpPath, outputPath)
//      val cells = buildCells(rdd, dbUrl, hbaseConf, table, maxHFilePerResionServer)
//
//      val job = Job.getInstance(hbaseConf)
//      job.getConfiguration().setClassLoader(Thread.currentThread().getContextClassLoader())
//      job.getConfiguration.set("hadoop.tmp.dir", s"/tmp/$tableName")
//
//
//      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
//      job.setMapOutputValueClass(classOf[KeyValue])
//      HFileOutputFormat2.configureIncrementalLoad(job, table)
//
//      cells.saveAsNewAPIHadoopFile(output, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration())

    } finally {

//      fs.deleteOnExit(o
    }
  }

}