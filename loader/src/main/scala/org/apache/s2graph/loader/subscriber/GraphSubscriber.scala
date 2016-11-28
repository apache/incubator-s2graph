/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.loader.subscriber

import com.typesafe.config.{Config, ConfigFactory}
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.s2graph.core._
import org.apache.s2graph.spark.spark.WithKafka
import org.apache.spark.{Accumulable, SparkContext}
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

//    val newConf = new util.HashMap[String, Object]()
//    newConf.put("hbase.zookeeper.quorum", zkQuorum)
//    newConf.put("db.default.url", database)
//    newConf.put("kafka.metadata.broker.list", kafkaBrokers)
    val newConf =
      if (kafkaBrokerList.isEmpty) Map("hbase.zookeeper.quorum" -> zkQuorum, "db.default.url" -> database, "cache.ttl.seconds" -> cacheTTL)
      else Map("hbase.zookeeper.quorum" -> zkQuorum, "db.default.url" -> database, "kafka.metadata.broker.list" -> kafkaBrokers, "cache.ttl.seconds" -> cacheTTL)

    ConfigFactory.parseMap(newConf).withFallback(S2Graph.DefaultConfig)
  }
}

object GraphSubscriberHelper extends WithKafka {


  type HashMapAccumulable = Accumulable[HashMap[String, Long], (String, Long)]


  lazy val producer = new Producer[String, String](kafkaConf(GraphConfig.kafkaBrokers))
  var config: Config = _
  private val writeBufferSize = 1024 * 1024 * 8
  private val sleepPeriod = 10000
  private val maxTryNum = 10

  var g: S2Graph = null
  var management: Management = null
  val conns = new scala.collection.mutable.HashMap[String, Connection]()

  def toOption(s: String) = {
    s match {
      case "" | "none" => None
      case _ => Some(s)
    }
  }

  def apply(phase: String, dbUrl: String, zkQuorum: String, kafkaBrokerList: String): Unit = {
    config = GraphConfig(phase, toOption(dbUrl), toOption(zkQuorum), toOption(kafkaBrokerList))

    if (g == null) {
      val ec = ExecutionContext.Implicits.global
      g = new S2Graph(config)(ec)
      management = new Management(g)
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
      g.toGraphElement(msg, labelMapping) match {
        case Some(e) if e.isInstanceOf[S2Edge] =>
          statFunc("EdgeParseOk", 1)
          e.asInstanceOf[S2Edge]
        case Some(v) if v.isInstanceOf[S2Vertex] =>
          statFunc("VertexParseOk", 1)
          v.asInstanceOf[S2Vertex]
        case Some(x) =>
          throw new RuntimeException(s">>>>> GraphSubscriber.toGraphElements: parsing failed. ${x.serviceName}")
        case None =>
          throw new RuntimeException(s"GraphSubscriber.toGraphElements: parsing failed. $msg")
      }

    }).toList
  }

//  private def storeRec(zkQuorum: String, tableName: String, puts: List[Put], elementsSize: Int, tryNum: Int)
//                      (statFunc: (String, Int) => Unit, statPrefix: String = "edge"): Unit = {
//    if (tryNum <= 0) {
//      statFunc("errorStore", elementsSize)
//      throw new RuntimeException(s"retry failed after $maxTryNum")
//    }
//    val conn = getConn(zkQuorum)
//    val mutator = conn.getBufferedMutator(TableName.valueOf(tableName))
//    //      val table = conn.getTable(TableName.valueOf(tableName))
//    //      table.setAutoFlush(false, false)
//
//    try {
//      puts.foreach { put => put.setDurability(Durability.ASYNC_WAL) }
//      mutator.mutate(puts)
//      //        table.put(puts)
//      statFunc(s"$statPrefix:storeOk", elementsSize)
//    } catch {
//      case e: Throwable =>
//        e.printStackTrace()
//        Thread.sleep(sleepPeriod)
//        storeRec(zkQuorum, tableName, puts, elementsSize, tryNum - 1)(statFunc)
//    } finally {
//      mutator.close()
//      //        table.close()
//    }
//  }
//
//  def storeDegreeBulk(zkQuorum: String, tableName: String)
//                     (degrees: Iterable[(String, String, String, Int)], labelMapping: Map[String, String] = Map.empty)
//                     (mapAccOpt: Option[HashMapAccumulable]): Iterable[(String, Long)] = {
//    val counts = HashMap[String, Long]()
//    val statFunc = storeStat(counts)(mapAccOpt) _
//
//    for {
//      (vertexId, labelName, direction, degreeVal) <- degrees
//      incrementRequests <- TransferToHFile.buildDegreePutRequests(vertexId, labelName, direction, degreeVal)
//    } {
//      storeRec(zkQuorum, tableName, incrementRequests, degrees.size, maxTryNum)(statFunc, "degree")
//    }
//    counts
//  }
//  def storeBulk(zkQuorum: String, tableName: String)
//               (msgs: Seq[String], labelMapping: Map[String, String] = Map.empty, autoCreateEdge: Boolean = false)
//               (mapAccOpt: Option[HashMapAccumulable]): Iterable[(String, Long)] = {
//
//    val counts = HashMap[String, Long]()
//    val statFunc = storeStat(counts)(mapAccOpt) _
//    val elements = toGraphElements(msgs, labelMapping)(statFunc)
//
//    val puts = elements.flatMap { element =>
//      element match {
//        case v: Vertex if v.op == GraphUtil.operations("insert") || v.op == GraphUtil.operations("insertBulk") =>
//          v.buildPuts()
//        case e: Edge if e.op == GraphUtil.operations("insert") || e.op == GraphUtil.operations("insertBulk") =>
//          EdgeWriter(e).insertBulkForLoader(autoCreateEdge)
//        case _ => Nil
//      }
//    } toList
//
//    storeRec(zkQuorum, tableName, puts, msgs.size, maxTryNum)(statFunc)
//    counts
//  }

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

