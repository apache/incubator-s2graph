package subscriber


import java.util

import com.daumkakao.s2graph.core.{Graph, _}
import com.typesafe.config.{Config, ConfigFactory}
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.spark.{Accumulable, SparkContext}
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext

object GraphConfig {
  var database = ""
  var zkQuorum = ""
  var kafkaBrokers = ""

  def apply(phase: String, dbUrl: Option[String], zkAddr: Option[String], kafkaBrokerList: Option[String]): Config = {
    database = dbUrl.getOrElse("jdbc:mysql://localhost:3306/graph")
    zkQuorum = zkAddr.getOrElse("localhost")


//    val newConf = new util.HashMap[String, Object]()
//    newConf.put("hbase.zookeeper.quorum", zkQuorum)
//    newConf.put("db.default.url", database)
//    newConf.put("kafka.metadata.broker.list", kafkaBrokers)
    val newConf =
      if (kafkaBrokerList.isEmpty) Map("hbase.zookeeper.quorum" -> zkQuorum, "db.default.url" -> database)
      else Map("hbase.zookeeper.quorum" -> zkQuorum, "db.default.url" -> database, "kafka.metadata.broker.list" -> kafkaBrokers)

    ConfigFactory.parseMap(newConf).withFallback(Graph.config)
  }
}

object GraphSubscriberHelper extends WithKafka {


  type HashMapAccumulable = Accumulable[HashMap[String, Long], (String, Long)]


  lazy val producer = new Producer[String, String](kafkaConf(GraphConfig.kafkaBrokers))
  var config: Config = _
  private val writeBufferSize = 1024 * 1024 * 8
  private val sleepPeriod = 10000
  private val maxTryNum = 10

  var g: Graph.type = null

  def toOption(s: String) = {
    s match {
      case "" | "none" => None
      case _ => Some(s)
    }
  }

  def apply(phase: String, dbUrl: String, zkQuorum: String, kafkaBrokerList: String): Unit = {
    config = GraphConfig(phase, toOption(dbUrl), toOption(zkQuorum), toOption(kafkaBrokerList))

    if (g == null) {
      Graph.apply(config)(ExecutionContext.Implicits.global)
      g = Graph
    }
  }

  def getConn(zkQuorum: String): Connection = {
    g.getConn(zkQuorum)
  }
  //  def apply(phase: String, dbUrl: Option[String], zkQuorum: Option[String], kafkaBrokerList: Option[String]): Unit  = {
  //    Graph.apply(GraphConfig(phase, dbUrl, zkQuorum, kafkaBrokerList))(ExecutionContext.Implicits.global)
  //  }
  def report(key: String, value: Option[String], topic: String = "report") = {
    val msg = Seq(Some(key), value).flatten.mkString("\t")
    val kafkaMsg = new KeyedMessage[String, String](topic, msg)
    producer.send(kafkaMsg)
  }

  def toGraphElements(msgs: Seq[String], labelMapping: Map[String, String] = Map.empty)
                     (statFunc: (String, Int) => Unit): Iterable[GraphElement] = {
    (for (msg <- msgs) yield {
      statFunc("total", 1)
      Graph.toGraphElement(msg, labelMapping) match {
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

  private def storeRec(zkQuorum: String, tableName: String, puts: List[Put], elementsSize: Int, tryNum: Int)
                      (statFunc: (String, Int) => Unit, statPrefix: String = "edge"): Unit = {
    if (tryNum <= 0) {
      statFunc("errorStore", elementsSize)
      throw new RuntimeException(s"retry failed after $maxTryNum")
    }
    val conn = getConn(zkQuorum)
    val mutator = conn.getBufferedMutator(TableName.valueOf(tableName))
    //      val table = conn.getTable(TableName.valueOf(tableName))
    //      table.setAutoFlush(false, false)

    try {
      puts.foreach { put => put.setDurability(Durability.ASYNC_WAL) }
      mutator.mutate(puts)
      //        table.put(puts)
      statFunc(s"$statPrefix:storeOk", elementsSize)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        Thread.sleep(sleepPeriod)
        storeRec(zkQuorum, tableName, puts, elementsSize, tryNum - 1)(statFunc)
    } finally {
      mutator.close()
      //        table.close()
    }
  }

  def storeDegreeBulk(zkQuorum: String, tableName: String)
                     (degrees: Iterable[(String, String, String, Int)], labelMapping: Map[String, String] = Map.empty)
                     (mapAccOpt: Option[HashMapAccumulable]): Iterable[(String, Long)] = {
    val counts = HashMap[String, Long]()
    val statFunc = storeStat(counts)(mapAccOpt) _

    for {
      (vertexId, labelName, direction, degreeVal) <- degrees
      incrementRequests <- Edge.buildIncrementDegreeBulk(vertexId, labelName, direction, degreeVal)
    } {
      storeRec(zkQuorum, tableName, incrementRequests, degrees.size, maxTryNum)(statFunc, "degree")
    }
    counts
  }
  def storeBulk(zkQuorum: String, tableName: String)
               (msgs: Seq[String], labelMapping: Map[String, String] = Map.empty, autoCreateEdge: Boolean = false)
               (mapAccOpt: Option[HashMapAccumulable]): Iterable[(String, Long)] = {

    val counts = HashMap[String, Long]()
    val statFunc = storeStat(counts)(mapAccOpt) _
    val elements = toGraphElements(msgs, labelMapping)(statFunc)

    val puts = elements.flatMap { element =>
      element match {
        case v: Vertex if v.op == GraphUtil.operations("insert") || v.op == GraphUtil.operations("insertBulk") =>
          v.buildPuts()
        case e: Edge if e.op == GraphUtil.operations("insert") || e.op == GraphUtil.operations("insertBulk") =>
          e.insertBulk(autoCreateEdge)
        case _ => Nil
      }
    } toList

    storeRec(zkQuorum, tableName, puts, msgs.size, maxTryNum)(statFunc)
    counts
  }

  def storeStat(counts: HashMap[String, Long])(mapAccOpt: Option[HashMapAccumulable])(key: String, value: Int) = {
    counts.put(key, counts.getOrElse(key, 0L) + value)
    mapAccOpt match {
      case None =>
      case Some(mapAcc) => mapAcc += (key -> value)
    }
  }

  def toLabelMapping(lableMapping: String): Map[String, String] = {
    (for {
      token <- lableMapping.split(",")
      inner = token.split(":") if inner.length == 2
    } yield {
        (inner.head, inner.last)
      }).toMap
  }

  def isValidQuorum(quorum: String) = {
    quorum.split(",").size > 1
  }
}

object GraphSubscriber extends SparkApp with WithKafka {
  val sleepPeriod = 5000
  val usages =
    s"""
       |/**
       |* this job read edge format(TSV) from HDFS file system then bulk load edges into s2graph. assumes that newLabelName is already created by API.
       |* params:
       |*  0. hdfsPath: where is your data in hdfs. require full path with hdfs:// predix
       |*  1. dbUrl: jdbc database connection string to specify database for meta.
       |*  2. labelMapping: oldLabel:newLabel delimited by ,
       |*  3. zkQuorum: target hbase zkQuorum where this job will publish data to.
       |*  4. hTableName: target hbase physical table name where this job will publish data to.
       |*  5. batchSize: how many edges will be batched for Put request to target hbase.
       |*  6. kafkaBrokerList: using kafka as fallback queue. when something goes wrong during batch, data needs to be replay will be stored in kafka.
       |*  7. kafkaTopic: fallback queue topic.
       |*  8. edgeAutoCreate: true if need to create reversed edge automatically.
       |*
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
//    if (args.length != 10) {
//      System.err.println(usages)
//      System.exit(1)
//    }
    val hdfsPath = args(0)
    val dbUrl = args(1)
    val labelMapping = GraphSubscriberHelper.toLabelMapping(args(2))

    val zkQuorum = args(3)
    val hTableName = args(4)
    val batchSize = args(5).toInt
    val kafkaBrokerList = args(6)
    val kafkaTopic = args(7)
    val edgeAutoCreate = args(8).toBoolean
    val vertexDegreePathOpt = if (args.length >= 10) GraphSubscriberHelper.toOption(args(9)) else None

    val conf = sparkConf(s"$hdfsPath: GraphSubscriber")
    val sc = new SparkContext(conf)
    val mapAcc = sc.accumulable(HashMap.empty[String, Long], "counter")(HashMapParam[String, Long](_ + _))


    if (!GraphSubscriberHelper.isValidQuorum(zkQuorum)) throw new RuntimeException(s"$zkQuorum is not valid.")

    /** this job expect only one hTableName. all labels in this job will be stored in same physical hbase table */
    try {

      import GraphSubscriberHelper._
      // set local driver setting.
      val phase = System.getProperty("phase")
      GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, kafkaBrokerList)

      /** copy when oldLabel exist and newLabel done exist. otherwise ignore. */

      if (labelMapping.isEmpty) {
        // pass
      } else {
        for {
          (oldLabelName, newLabelName) <- labelMapping
        } {
          Management.copyLabel(oldLabelName, newLabelName, toOption(hTableName))
        }
      }

      vertexDegreePathOpt.foreach { vertexDegreePath =>
        val vertexDegrees = sc.textFile(vertexDegreePath).filter(line => line.split("\t").length == 4).map { line =>
          val tokens = line.split("\t")
          (tokens(0), tokens(1), tokens(2), tokens(3).toInt)
        }
        vertexDegrees.foreachPartition { partition =>

          // init Graph
          val phase = System.getProperty("phase")
          GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, kafkaBrokerList)

          partition.grouped(batchSize).foreach { msgs =>
            try {
              val start = System.currentTimeMillis()
              val counts = GraphSubscriberHelper.storeDegreeBulk(zkQuorum, hTableName)(msgs, labelMapping)(Some(mapAcc))
              for ((k, v) <- counts) {
                mapAcc +=(k, v)
              }
              val duration = System.currentTimeMillis() - start
              println(s"[Success]: store, $mapAcc, $duration, $zkQuorum, $hTableName")
            } catch {
              case e: Throwable =>
                println(s"[Failed]: store $e")

                msgs.foreach { msg =>
                  GraphSubscriberHelper.report(msg.toString(), Some(e.getMessage()), topic = kafkaTopic)
                }
            }
          }
        }
      }


      val msgs = sc.textFile(hdfsPath)
      msgs.foreachPartition(partition => {
        // set executor setting.
        val phase = System.getProperty("phase")
        GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, kafkaBrokerList)

        partition.grouped(batchSize).foreach { msgs =>
          try {
            val start = System.currentTimeMillis()
            //            val counts =
            //              GraphSubscriberHelper.store(msgs, GraphSubscriberHelper.toOption(newLabelName))(Some(mapAcc))
            val counts =
              GraphSubscriberHelper.storeBulk(zkQuorum, hTableName)(msgs, labelMapping, edgeAutoCreate)(Some(mapAcc))

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
