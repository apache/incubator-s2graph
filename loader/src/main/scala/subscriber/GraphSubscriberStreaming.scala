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

package subscriber

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory}
import org.apache.spark.streaming.Durations._
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.language.postfixOps

//object GraphSubscriberStreaming extends SparkApp with WithKafka {
//  val usages =
//    s"""
//       |/**
//       |this job consume edges/vertices from kafka topic then load them into s2graph.
//       |params:
//       |  1. kafkaZkQuorum: kafka zk address to consume events
//       |  2. brokerList: kafka cluster`s broker list.
//       |  3. topics: , delimited list of topics to consume
//       |  4. intervalInSec: batch  interval for this job.
//       |  5. batchSize: how many edges/vertices will be grouped for bulk mutations.
//       |  6. hbaseZkQuorum: s2graph zookeeper address.
//       |  7. hTableName: physical hbase table name.
//       |  8. labelMapping: oldLabel:newLabel delimited by ,
//       |*/
//   """.stripMargin
//  override def run() = {
//    if (args.length != 9) {
//      System.err.println(usages)
//      System.exit(1)
//    }
//    val kafkaZkQuorum = args(0)
//    val brokerList = args(1)
//    val topics = args(2)
//    val intervalInSec = seconds(args(3).toLong)
//    val dbUrl = args(4)
//    val batchSize = args(5).toInt
//    val hbaseZkQuorum = args(6)
//    val hTableName = args(7)
//    val labelMapping = GraphSubscriberHelper.toLabelMapping(args(8))
//
//
//    if (!GraphSubscriberHelper.isValidQuorum(hbaseZkQuorum))
//      throw new RuntimeException(s"$hbaseZkQuorum is not valid.")
//
//    val conf = sparkConf(s"$topics: GraphSubscriberStreaming")
//    val ssc = streamingContext(conf, intervalInSec)
//    val sc = ssc.sparkContext
//
//    val groupId = topics.replaceAll(",", "_") + "_stream"
//    val fallbackTopic = topics.replaceAll(",", "_") + "_stream_failed"
//
//    val kafkaParams = Map(
//      "zookeeper.connect" -> kafkaZkQuorum,
//      "group.id" -> groupId,
//      "zookeeper.connection.timeout.ms" -> "10000",
//      "metadata.broker.list" -> brokerList,
//      "auto.offset.reset" -> "largest")
//
//    val stream = createKafkaValueStreamMulti(ssc, kafkaParams, topics, 8, None).flatMap(s => s.split("\n"))
//
//    val mapAcc = sc.accumulable(new MutableHashMap[String, Long](), "Throughput")(HashMapParam[String, Long](_ + _))
//
//
//    stream.foreachRDD(rdd => {
//
//      rdd.foreachPartition(partition => {
//        // set executor setting.
//        val phase = System.getProperty("phase")
//        GraphSubscriberHelper.apply(phase, dbUrl, hbaseZkQuorum, brokerList)
//
//        partition.grouped(batchSize).foreach { msgs =>
//          try {
//            val start = System.currentTimeMillis()
//            //            val counts =
//            //              GraphSubscriberHelper.store(msgs, GraphSubscriberHelper.toOption(newLabelName))(Some(mapAcc))
//            val counts =
//              GraphSubscriberHelper.storeBulk(hbaseZkQuorum, hTableName)(msgs, labelMapping)(Some(mapAcc))
//
//            for ((k, v) <- counts) {
//              mapAcc +=(k, v)
//            }
//            val duration = System.currentTimeMillis() - start
//            println(s"[Success]: store, $mapAcc, $duration, $hbaseZkQuorum, $hTableName")
//          } catch {
//            case e: Throwable =>
//              println(s"[Failed]: store $e")
//
//              msgs.foreach { msg =>
//                GraphSubscriberHelper.report(msg, Some(e.getMessage()), topic = fallbackTopic)
//              }
//          }
//        }
//      })
//    })
//
//
//    logInfo(s"counter: ${mapAcc.value}")
//    println(s"counter: ${mapAcc.value}")
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
