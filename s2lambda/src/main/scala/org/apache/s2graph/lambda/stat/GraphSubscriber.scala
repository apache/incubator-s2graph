package org.apache.s2graph.lambda.stat

import com.kakao.s2graph.core.{Graph, _}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.spark.Accumulable

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext

object GraphConfig {
  var database = ""
  var zkQuorum = ""
  var kafkaBrokers = ""
  var cacheTTL = s"${60 * 60 * 1}"
  def apply(phase: String, dbUrl: Option[String], zkAddr: Option[String], kafkaBrokerList: Option[String]): Config = {
    database = dbUrl.getOrElse("jdbc:mysql://localhost:3306/graph_dev")
    zkQuorum = zkAddr.getOrElse("localhost")

    val newConf =
      if (kafkaBrokerList.isEmpty) Map("hbase.zookeeper.quorum" -> zkQuorum, "db.default.url" -> database, "cache.ttl.seconds" -> cacheTTL)
      else Map("hbase.zookeeper.quorum" -> zkQuorum, "db.default.url" -> database, "kafka.metadata.broker.list" -> kafkaBrokers, "cache.ttl.seconds" -> cacheTTL)

    ConfigFactory.parseMap(newConf).withFallback(Graph.DefaultConfig)
  }
}

object GraphSubscriberHelper {


  type HashMapAccumulable = Accumulable[HashMap[String, Long], (String, Long)]

  var config: Config = _
  private val writeBufferSize = 1024 * 1024 * 8
  private val sleepPeriod = 10000
  private val maxTryNum = 10

  var g: Graph = null
  val conns = new scala.collection.mutable.HashMap[String, Connection]()

  def toOption(s: String) = {
    s match {
      case "" | "none" => None
      case _ => Some(s)
    }
  }

  def apply(phase: String, dbUrl: String, zkQuorum: String, kafkaBrokerList: String): Unit = {
    config = GraphConfig(phase, toOption(dbUrl), toOption(zkQuorum), toOption(kafkaBrokerList))

    println(s"helper config: $config")

    if (g == null) {
      g = new Graph(config)(ExecutionContext.Implicits.global)
    }
  }

  def getConn(zkQuorum: String): Connection = {
    conns.getOrElseUpdate(zkQuorum, {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
      ConnectionFactory.createConnection(hbaseConf)
    })
    conns(zkQuorum)
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
        case Some(x) =>
          throw new RuntimeException(s">>>>> GraphSubscriber.toGraphElements: parsing failed. ${x.serviceName}")
        case None =>
          throw new RuntimeException(s"GraphSubscriber.toGraphElements: parsing failed. $msg")
      }

    }).toList
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
