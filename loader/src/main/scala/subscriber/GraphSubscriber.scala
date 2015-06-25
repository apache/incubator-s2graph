package subscriber


import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.Graph
import com.typesafe.config.{Config, ConfigFactory}
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulable, SparkContext}
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext

object GraphConfig {
  var database = ""
  var zkQuorum = ""
  var kafkaBrokers = ""

  def apply(phase: String, dbUrl: Option[String], zkAddr: Option[String], kafkaBrokerList: Option[String]) = {
    database = dbUrl.getOrElse("jdbc:mysql://localhost:3306/graph")
    zkQuorum = zkAddr.getOrElse("localhost")
    kafkaBrokers = kafkaBrokerList.getOrElse("localhost:9092")
    val s = s"""
               |# hbase
               |hbase.zookeeper.quorum="$zkQuorum"
               |# APP PHASE
               |phase=dev
               | """.stripMargin

    println(s)
    ConfigFactory.parseString(s)
  }
}

object GraphSubscriberHelper extends WithKafka {


  type HashMapAccumulable = Accumulable[HashMap[String, Long], (String, Long)]


  lazy val producer = new Producer[String, String](kafkaConf(GraphConfig.kafkaBrokers))
  var config: Config = _
  private val writeBufferSize = 1024 * 1024 * 8
  private val sleepPeriod = 10000
  private val maxTryNum = 10

  def toOption(s: String) = {
    s match {
      case "" | "none" => None
      case _ => Some(s)
    }
  }

  def apply(phase: String, dbUrl: String, zkQuorum: String, kafkaBrokerList: String): Unit = {
    config = GraphConfig(phase, toOption(dbUrl), toOption(zkQuorum), toOption(kafkaBrokerList))
    Graph.apply(config)(ExecutionContext.Implicits.global)
  }

  //  def apply(phase: String, dbUrl: Option[String], zkQuorum: Option[String], kafkaBrokerList: Option[String]): Unit  = {
  //    Graph.apply(GraphConfig(phase, dbUrl, zkQuorum, kafkaBrokerList))(ExecutionContext.Implicits.global)
  //  }
  def report(key: String, value: Option[String], topic: String = "report") = {
    val msg = Seq(Some(key), value).flatten.mkString("\t")
    val kafkaMsg = new KeyedMessage[String, String](topic, msg)
    producer.send(kafkaMsg)
  }

  def toGraphElements(msgs: Seq[String], labelToReplace: Option[String])
                     (statFunc: (String, Int) => Unit): Iterable[GraphElement] = {
    (for (msg <- msgs) yield {
      statFunc("total", 1)
      Graph.toGraphElement(msg, labelToReplace) match {
        case Some(e) if e.isInstanceOf[Edge] =>
          statFunc("EdgeParseOk", 1)
          e.asInstanceOf[Edge]
        case Some(v) if v.isInstanceOf[Vertex] =>
          statFunc("VertexParseOk", 1)
          v.asInstanceOf[Vertex]
        case None =>
          throw new RuntimeException(s"GraphSubscriber.toGraphElements: parsing failed. $msg")
      }

    }).toList
  }

  /**
   * bulkMutates read connection and table info from database.
   */
  //  def store(conn: HConnection, tableName: String)(msgs: Seq[String], labelToReplace: Option[String] = None)
  //           (mapAccOpt: Option[HashMapAccumulable]): Iterable[(String, Long)] = {
  //    val counts = HashMap[String, Long]()
  //    val statFunc = storeStat(counts)(mapAccOpt) _
  //
  //    /**
  //     * fetch cluster, table from database.
  //     */
  //    val elements = toGraphElements(msgs, labelToReplace)(statFunc)
  //
  //    val puts = for {
  //      element <- elements
  //    } yield {
  //      element match {
  //        case e: Edge => e.insertBulk()
  //        case v: Vertex => v.buildPuts()
  //      }
  //    }
  //
  //    counts
  //  }

  def storeBulk(conn: Connection, tableName: String)(msgs: Seq[String], labelToReplace: Option[String] = None)
               (mapAccOpt: Option[HashMapAccumulable]): Iterable[(String, Long)] = {
    val counts = HashMap[String, Long]()
    val statFunc = storeStat(counts)(mapAccOpt) _
    val elements = toGraphElements(msgs, labelToReplace)(statFunc)
    val puts = elements.flatMap { element =>
      element match {
        case v: Vertex if v.op == GraphUtil.operations("insert") || v.op == GraphUtil.operations("insertBulk") =>
          v.buildPuts()
        case e: Edge if e.op == GraphUtil.operations("insert") || e.op == GraphUtil.operations("insertBulk") =>
          val snapshotEdgePuts =
            if (e.labelWithDir.dir == GraphUtil.directions("out")) List(e.toInvertedEdgeHashLike().buildPut())
            else Nil
          snapshotEdgePuts ++ e.edgesWithIndex.flatMap { edgeWithIndex =>
            edgeWithIndex.buildPuts()
          } ++ e.buildVertexPuts()
        case _ => Nil
      }
    } toList
    /**
     * don't use database for connection and table.
     */

    def storeRec(puts: List[Put], tryNum: Int): Unit = {
      if (tryNum <= 0) {
        statFunc("errorStore", elements.size)
        throw new RuntimeException(s"retry failed after $maxTryNum")
      }

      val mutator = conn.getBufferedMutator(TableName.valueOf(tableName))


      try {
        mutator.mutate(puts)
        statFunc("storeOk", msgs.size)
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          Thread.sleep(sleepPeriod)
          storeRec(puts, tryNum - 1)
      } finally {
        mutator.close()
      }
    }



    storeRec(puts, maxTryNum)
    counts
  }

  def storeStat(counts: HashMap[String, Long])(mapAccOpt: Option[HashMapAccumulable])(key: String, value: Int) = {
    counts.put(key, counts.getOrElse(key, 0L) + value)
    mapAccOpt match {
      case None =>
      case Some(mapAcc) => mapAcc += (key -> value)
    }
  }

}

object GraphSubscriber extends SparkApp with WithKafka {
  val sleepPeriod = 5000
  val usages =
    s"""
       |/**
       |* this job read edge format(TSV) from HDFS file system then bulk load edges into s2graph. assumes that newLabelName is already created by API.
       |* params:
       |*  1. hdfsPath: where is your data in hdfs. require full path with hdfs:// predix
       |*  2. dbUrl: jdbc database connection string to specify database for meta.
       |*  3. originalLabelName: label to copy with.
       |*  4. newLabelName: label field will be replaced to this value.
       |*  5. zkQuorum: target hbase zkQuorum where this job will publish data to.
       |*  6. hTableName: target hbase physical table name where this job will publish data to.
       |*  7. batchSize: how many edges will be batched for Put request to target hbase.
       |*  8. kafkaBrokerList: using kafka as fallback queue. when something goes wrong during batch, data needs to be replay will be stored in kafka.
       |*  9. kafkaTopic: fallback queue topic.
       |* after this job finished, s2graph will have data with sequence corresponding newLabelName.
       |* change this newLabelName to ogirinalName if you want to online replace of label.
       |*
       |*/
     """.stripMargin

  override def run() = {
    /**
     * Main function
     */
    println(args.toList)
    if (args.length != 9) {
      System.err.println(usages)
      System.exit(1)
    }
    val hdfsPath = args(0)
    val dbUrl = args(1)
    val oldLabelName = args(2)
    val newLabelName = args(3)
    val zkQuorum = args(4)
    val hTableName = args(5)
    val batchSize = args(6).toInt
    val kafkaBrokerList = args(7)
    val kafkaTopic = args(8)

    val conf = sparkConf(s"$hdfsPath: GraphSubscriber")
    val sc = new SparkContext(conf)
    val mapAcc = sc.accumulable(HashMap.empty[String, Long], "counter")(HashMapParam[String, Long](_ + _))



    try {

      import GraphSubscriberHelper._
      // set local driver setting.
      val phase = System.getProperty("phase")
      GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, kafkaBrokerList)

      /** copy when oldLabel exist and newLabel done exist. otherwise ignore. */
      Management.copyLabel(oldLabelName, newLabelName, toOption(hTableName))

      val msgs = sc.textFile(hdfsPath)

      /** change assumption here. this job only take care of one label data */
      val degreeStart: RDD[((String, String, String), Int)] = msgs.map { msg =>
        val tokens = GraphUtil.split(msg)
        val direction = if (tokens.length == 7) "out" else tokens(7)
        val vertexWithLabel = if (direction == "out") {
          (tokens(3), newLabelName, direction)
        } else {
          (tokens(4), newLabelName, direction)
        }
        (vertexWithLabel, 1)
      }
      val vertexDegrees = degreeStart.reduceByKey(_ + _)

      vertexDegrees.saveAsTextFile(s"/user/work/s2graph/${newLabelName}_degree")

      vertexDegrees.foreachPartition { partition  =>

        // init Graph
        val phase = System.getProperty("phase")
        GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, kafkaBrokerList)

        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        val conn = ConnectionFactory.createConnection(conf)
        val table = conn.getTable(TableName.valueOf(hTableName))
        //        val mutator = conn.getBufferedMutator(TableName.valueOf(hTableName))

        try {
          for {
            ((vertexId, labelName, direction), degreeVal) <- partition
            incrementRequests <- Edge.buildIncrementDegreeBulk(vertexId, labelName, direction, degreeVal)
            incrementRequest <- incrementRequests
          } {
            println(s"${vertexId}\t${incrementRequest.getRow.toList}")

            //            mutator.mutate(incrementRequest)
            table.increment(incrementRequest)
          }
        } finally {
          table.close()
          //          mutator.close()
        }

      }


      msgs.foreachPartition(partition => {
        // set executor setting.
        val phase = System.getProperty("phase")
        GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, kafkaBrokerList)

        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        val conn = HConnectionManager.createConnection(conf)

        partition.grouped(batchSize).foreach { msgs =>
          try {
            val start = System.currentTimeMillis()
            //            val counts =
            //              GraphSubscriberHelper.store(msgs, GraphSubscriberHelper.toOption(newLabelName))(Some(mapAcc))
            val counts =
              GraphSubscriberHelper.storeBulk(conn, hTableName)(msgs, GraphSubscriberHelper.toOption(newLabelName))(Some(mapAcc))

            for ((k, v) <- counts) {
              mapAcc +=(k, v)
            }
            val duration = System.currentTimeMillis() - start
            println(s"[Success]: store, $mapAcc, $duration, $zkQuorum, $hTableName")
          } catch {
            case e: Throwable =>
              println(s"[Failed]: store $e")

              msgs.foreach { msg =>
                GraphSubscriberHelper.report(msg, Some(e.getMessage()), topic = kafkaTopic)
              }
          }
        }
        conn.close()
      })

      logInfo(s"counter: $mapAcc")
      println(s"Stats: ${mapAcc}")

    } catch {
      case e: Throwable =>
        println(s"job failed with exception: $e")
        throw e
    }

  }
}
