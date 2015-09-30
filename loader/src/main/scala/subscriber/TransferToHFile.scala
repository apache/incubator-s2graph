package subscriber

import com.kakao.s2graph.core.Graph
import org.apache.hadoop.hbase.client.{HTable, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase._
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.hbase.async.{PutRequest}
import s2.spark.{HashMapParam, SparkApp}

import scala.collection.mutable.{HashMap => MutableHashMap}

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

  //TODO: Process AtomicIncrementRequest too.
  /** build key values */
  private def buildCells(rdd: RDD[String], dbUrl: String, numOfRegionServers: Int): RDD[(ImmutableBytesWritable, Cell)] = {
    val kvs = rdd.mapPartitions { iter =>

      val phase = System.getProperty("phase")
      GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")

      iter.flatMap { s =>
        Graph.toGraphElement(s) match {
          case None => List.empty[Cell]
          case Some(e) =>
            for {
              rpc <- e.buildPutsAll() // what about increments?
              if rpc.isInstanceOf[PutRequest]
              put = rpc.asInstanceOf[PutRequest]
            } yield {
              val p = put
              CellUtil.createCell(p.key(), p.family(), p.qualifier(), p.timestamp(), KeyValue.Type.Put.getCode, p.value())
            }
        }
      }
    }
    val sorted =
      kvs.map(kv => (kv, kv)).sortByKey(ascending = true, numOfRegionServers).map { kv =>
        new ImmutableBytesWritable(kv._1.getRowArray, kv._1.getRowOffset, kv._1.getRowLength) -> kv._2
      }
    sorted
  }

  implicit val myComparator = new Ordering[Cell] {
    override def compare(left: Cell, right: Cell) = {
      KeyValue.COMPARATOR.compare(left, right)
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
    println(args)

    val mapAcc = sc.accumulable(MutableHashMap.empty[String, Long], "counter")(HashMapParam[String, Long](_ + _))

    /** set up hbase init */
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//    hbaseConf.set("hadoop.tmp.dir", s"/tmp/$tableName")

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val numOfRegionServers = conn.getAdmin.getClusterStatus.getServersSize
    val table = new HTable(hbaseConf, tableName)


    try {

      val rdd = sc.textFile(input)
      val cells = buildCells(rdd, dbUrl, numOfRegionServers * maxHFilePerResionServer)

      println("buildCellsFinished")

      val job = Job.getInstance(hbaseConf)
      job.getConfiguration().setClassLoader(Thread.currentThread().getContextClassLoader())
      job.getConfiguration.set("hadoop.tmp.dir", s"/tmp/$tableName")


      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[Cell])
      HFileOutputFormat2.configureIncrementalLoad(job, table)

      cells.saveAsNewAPIHadoopFile(output, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration())
    } finally {
      table.close()
      conn.close()
    }
  }

}
