package subscriber

import java.util.TreeSet

import com.daumkakao.s2graph.core.{Edge, Graph}
import org.apache.hadoop.conf.Configuration
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

import scala.collection.mutable.{HashMap => MutableHashMap}
import unicredit.spark.hbase._

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
  //  private class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) extends Partitioner {
  //    val fraction = 1 max numFilesPerRegion min conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)
  //
  //    override def getPartition(key: Any): Int = {
  //      def bytes(n: Any) = n match {
  //        case s: String => Bytes.toBytes(s)
  //      }
  //
  //      val h = (key.hashCode() & Int.MaxValue) % fraction
  //      for (i <- 1 until splits.length)
  //        if (Bytes.compareTo(bytes(key), splits(i)) < 0) return (i - 1) * fraction + h
  //
  //      (splits.length - 1) * fraction + h
  //    }
  //
  //    override def numPartitions: Int = splits.length * fraction
  //  }

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

  def buildCells(rdd: RDD[String], dbUrl: String) = {
    val kvs = rdd.mapPartitions { iter =>

      val phase = System.getProperty("phase")
      GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")
      toKeyValues(iter.toSeq)

    }

    implicit val ordering = new Ordering[KeyValue] {
      override def compare(a: KeyValue, b: KeyValue) = {
        KeyValue.COMPARATOR.compare(a, b)
      }
    }
    kvs.sortBy { kv =>
      kv
    }.map { kv =>
      val hKey = new ImmutableBytesWritable(kv.getRow())
//      println(s"[Key]: ${hKey.get().toList}")
//      println(s"[Value]: ${kv}")
      (hKey -> kv)
    }

  }


  override def run() = {
    val input = args(0)
    val output = args(1)
    val zkQuorum = args(2)
    val tableName = args(3)
    val dbUrl = args(4)
    val maxHFilePerResionServer = if (args.length >= 6) args(5).toInt else 1

    val conf = sparkConf(s"$input: TransferToHFile")
    val sc = new SparkContext(conf)
    println(args.toList)

    val mapAcc = sc.accumulable(MutableHashMap.empty[String, Long], "counter")(HashMapParam[String, Long](_ + _))

    /** set up hbase init */
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hbaseConf.set("hadoop.tmp.dir", s"/tmp/$tableName")

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val numOfRegionServers = conn.getAdmin.getClusterStatus.getServersSize
    val table = new HTable(hbaseConf, tableName)


    try {

      val rdd = sc.textFile(input)
      val cells = buildCells(rdd, dbUrl)
      //      def toKeyValue(row: Array[Byte], cf: Array[Byte], qualifier: Array[Byte], data: Array[Byte]): KeyValue = {
      //        val kv = new KeyValue(row, cf, qualifier, data)
      //        println(s"[row]: ${row.toList}")
      //        println(s"[cf]: ${cf.toList}")
      //        println(s"[qualifier]: ${qualifier.toList}")
      //        println(s"[data]: ${data.toList}")
      //        println(s"[keyValue]: ${kv}")
      //        kv
      //      }
      //      new HFileMapRDD[Array[Byte]](cells, toKeyValue)
      //      .toHBaseBulk(tableName, Bytes.toString(Graph.edgeCf), maxHFilePerResionServer)(HBaseConfig(hbaseConf))

      //      println("buildCellsFinished")
      //
      val job = Job.getInstance(hbaseConf)
      job.getConfiguration().setClassLoader(Thread.currentThread().getContextClassLoader())
      job.getConfiguration.set("hadoop.tmp.dir", s"/tmp/$tableName")


      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      HFileOutputFormat2.configureIncrementalLoad(job, table)

      cells.saveAsNewAPIHadoopFile(output, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration())


    } finally {
      table.close()
      conn.close()
    }
  }

}