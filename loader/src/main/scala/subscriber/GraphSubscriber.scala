package subscriber

import java.io.IOException

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.Graph
import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager}
import org.apache.spark.{Accumulable, SparkContext}
import s2.spark.{HashMapParam, SparkApp, WithKafka}
import subscriber.parser.{GraphParser, GraphParsers}

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
db.default.driver=com.mysql.jdbc.Driver
db.default.url="$database"
db.default.user=graph
db.default.password=graph

is.query.server=true
is.analyzer=false
is.test.query.server=false
test.sample.prob=0.1
    
cache.ttl.seconds=60000
cache.max.size=100000

hbase.connection.pool.size=1
hbase.table.pool.size=10
hbase.client.ipc.pool.size=1
zookeeper.recovery.retry=10
zookeeper.session.timeout=180000
hbase.zookeeper.quorum="$zkQuorum"
hbase.table.name="s2graph-alpha"
hbase.client.operation.timeout=10000
hbase.client.retries.number=10
hbase.client.write.operation.timeout=10000
hbase.client.write.retries.number=10
    
kafka.metadata.broker.list="$kafkaBrokers"
kafka.request.required.acks=1
kafka.producer.type="sync"
kafka.producer.buffer.flush.time=1000
kafka.producer.buffer.size=1000
kafka.producer.pool.size=1
kafka.aggregate.flush.timeout=1000
    
# Aggregator
client.aggregate.buffer.size=100
client.aggregate.buffer.flush.time=10000
client.aggregate.pool.size=1


# blocking execution context
contexts {
	query {
		fork-join-executor {
  		parallelism-min = 1
    	parallelism-max = 1
		}
	}
	blocking {
		fork-join-executor {
  		parallelism-min = 1
    	parallelism-max = 1
		}
	}
	scheduler {
	 	fork-join-executor {
  		parallelism-min = 1
    	parallelism-max = 1
		}
	}
}
  """
    println(s)
    ConfigFactory.parseString(s)
  }
}
object GraphSubscriberHelper extends WithKafka {
  

  type HashMapAccumulable = Accumulable[HashMap[String, Long], (String, Long)]


  lazy val producer = new Producer[String, String](kafkaConf(GraphConfig.kafkaBrokers))

  private val writeBufferSize = 1024 * 1024 * 8
  private val sleepPeriod = 10000
  private val maxTryNum = 10
//  lazy val graph = {
//    println(System.getProperty("phase"))
//    Graph.apply(GraphConfig.apply(System.getProperty("phase"), None, None))(ExecutionContext.Implicits.global)
//    println(Graph.config)
//    println(Graph.hbaseConfig)
//    Graph
//  }
  def toOption(s: String) = {
    s match {
      case "" | "none" => None
      case _ => Some(s)
    }
  }
  def apply(phase: String, dbUrl: String, zkQuorum: String, kafkaBrokerList: String) : Unit = {
    apply(phase, toOption(dbUrl), toOption(zkQuorum), toOption(kafkaBrokerList))
  }
  def apply(phase: String, dbUrl: Option[String], zkQuorum: Option[String], kafkaBrokerList: Option[String]): Unit  = {
    Graph.apply(GraphConfig(phase, dbUrl, zkQuorum, kafkaBrokerList))(ExecutionContext.Implicits.global)
  }
  def report(key: String, value: Option[String], topic: String = "report") = {
    //    val ts = System.currentTimeMillis().toString
    val msg = Seq(Some(key), value).flatten.mkString("\t")

    //    val producer = new Producer[String, String](kafkaConf(Config.KAFKA_METADATA_BROKER_LIST))
    val kafkaMsg = new KeyedMessage[String, String](topic, msg)
    producer.send(kafkaMsg)
  }

  /**
   * bulkMutates read connection and table info from database.
   */
  def store(msgs: Seq[String])(mapAccOpt: Option[HashMapAccumulable]): Iterable[(String, Long)] = {
    //    assert(msgs.size >= maxSize)
    val counts = HashMap[String, Long]()
    val statFunc = storeStat(counts)(mapAccOpt)_

    val elements = (for (msg <- msgs) yield {
      statFunc("total", 1)

      val element = Graph.toGraphElement(msg)
      element match {
        case Some(e) =>
          statFunc("parseOk", 1)
          element
        case None =>
          statFunc("errorParsing", 1)
          None
      }
    }).flatten.toList

    /**
     * fetch cluster, table from database.
     */

    try {
      Graph.bulkMutates(elements, mutateInPlace = true)
      statFunc("store", elements.size)
    } catch {
      case e: Throwable =>
        statFunc("storeFailed", elements.size)
        println(s"[Exception]: $e")
        throw e
    }
    counts
  }
  def storeStat(counts: HashMap[String, Long])(mapAccOpt: Option[HashMapAccumulable])(key: String, value: Int) = {
    counts.put(key, counts.getOrElse(key, 0L) + value)
    mapAccOpt match {
      case None =>
      case Some(mapAcc) => mapAcc += (key -> value)
    }
  }
//
//  /**
//   * caller of this method should split msgs into reasonable size.
//   */
//  def storeBulk(conn: HConnection, msgs: Seq[String])(mapAccOpt: Option[HashMapAccumulable], zkQuorum: String, tableName: String): Iterable[(String, Long)] = {
//    val counts = HashMap[String, Long]()
//    val statFunc = storeStat(counts)(mapAccOpt)_
//    val edges = (for (msg <- msgs) yield {
//      statFunc("total", 1)
//
//      val edge = Graph.toEdge(msg)
//      edge match {
//        case Some(e) =>
//          statFunc("parseOk", 1)
//          edge
//        case None =>
//          statFunc("errorParsing", 1)
//          None
//      }
//    }).flatten.toList
//
//    storeRec(edges)
//    /**
//     * don't use database for connection and table.
//     */
//    // throw all exception to caller.
//
//    def storeRec(edges: List[Edge], tryNum: Int = maxTryNum): Unit = {
//      if (tryNum <= 0) {
//        statFunc("errorStore", edges.size)
//        throw new RuntimeException(s"retry failed after $maxTryNum")
//      }
//      try {
//
//      }
//      try {
//
//        Graph.bulkMutates(edges)
//        // on bulk mode, we don`t actually care about WAL log on hbase. so skip this process to make things faster.
//        val puts = edges.flatMap(e => e.buildPutsAll ++ e.buildVertexPuts).map { p =>
//          //        p.setDurability(Durability.SKIP_WAL)
//          p
//        }
//        table.put(puts)
//        statFunc("storeOk", msgs.size)
//      } catch {
//        case e: Throwable =>
//          e.printStackTrace()
//          Thread.sleep(sleepPeriod)
//          storeRec(edges, tryNum - 1)
//        //          statFunc("errorStore", msgs.size)
//        //          throw e
//      } finally {
//        table.close()
//      }
//    }
//
//    counts
//  }
}
/**
 * do not use Graph.bulkMutates since it automatically read zkQuorum and hbase TableName from database.
 * batch insert should reference database only for parsing not getting connection and table!
 */
object GraphSubscriber extends SparkApp with WithKafka {
  val sleepPeriod = 5000

  override def run() = {
    /**
     * Main function
     */
    if (args.length < 3) {
      System.err.println("Usage: GraphSubscriber <hdfsPath> <batchSize> <dbUrl> <zkQuorum> <tableName> <kafkaBrokerList> <kafkaTopic> <preSplitSize>")
      System.exit(1)
    }
    val hdfsPath = args(0)
    val batchSize = args(1).toInt
    val dbUrl = args(2)
    val zkQuorum = args(3)
    val tableName = args(4)
    val kafkaBrokerList = args(5)
    val kafkaTopic = args(6)
    val preSplitSize = if (args.length > 7) args(7).toInt else 20

    val conf = sparkConf(s"$hdfsPath: GraphSubscriber")
    val sc = new SparkContext(conf)

    val mapAcc = sc.accumulable(HashMap.empty[String, Long], "counter")(HashMapParam[String, Long](_ + _))
    val fallbackTopic = s"${tableName}_batch_failed"

    try {
      tableName match {
        case "s2graph" | "s2graph-alpha" | "s2graph-sandbox" => System.err.println("try to create master table!")
        case _ => Management.createTable(zkQuorum, tableName, List("e", "v"), preSplitSize, None)
      }

      // not sure how fast htable get table can recognize new table so sleep.
      Thread.sleep(sleepPeriod)

      // set local driver setting.
      val phase = System.getProperty("phase")
      GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, kafkaBrokerList)

      val msgs = sc.textFile(hdfsPath)
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
            val counts =
//              if (isBulk) GraphSubscriberHelper.storeBulk(conn, msgs)(Some(mapAcc), zkQuorum, tableName)
              GraphSubscriberHelper.store(msgs)(Some(mapAcc))

            for ((k, v) <- counts) {
              mapAcc += (k, v)
            }
            val duration = System.currentTimeMillis() - start
            println(s"[Success]: store, $mapAcc, $duration, $zkQuorum, $tableName")
          } catch {
            case e: Throwable =>
              println(s"[Failed]: store $e")

              msgs.foreach { msg =>
                GraphSubscriberHelper.report(msg, Some(e.getMessage()), topic = fallbackTopic)
              }
          }
        }
        conn.close()
      })

      logInfo(s"counter: $mapAcc")
      println(s"Stats: ${mapAcc}")

      //      if (shouldUpdate) Label.prependHBaseTableName(labelName, tableName)
    } catch {
      case e: IOException =>
        println(s"job failed with exception: $e")
        throw e
    }

  }
}
