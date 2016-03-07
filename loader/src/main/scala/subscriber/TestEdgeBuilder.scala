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

import org.apache.spark.SparkContext
import s2.spark.{SparkApp, WithKafka}

import scala.util.Random

/**
 * Created by shon on 7/27/15.
 */
//object TestEdgeBuilder  extends SparkApp with WithKafka {
//  val sleepPeriod = 5000
//  val usages =
//    s"""
//       |/**
//       |0: numOfRows =
//       |*/
//     """.stripMargin
//
//  override def run() = {
//    /**
//     * label definition can be found on migrate/s2graph/bmt.schema
//     * Main function
//     * numOfRows: number of rows
//     * numOfCols: number of cols
//     * numOfMetas: number of metas
//     *
//     */
//    println(args.toList)
//    val conf = sparkConf(s"TestEdgeBuilder")
//    val sc = new SparkContext(conf)
//    val phase = args(0)
//    val dbUrl = args(1)
//    val zkQuorum = args(2)
//    val hTableName = args(3)
//    val labelName = args(4)
//    val metaName = args(5)
//
//    val numOfRows = if (args.length >= 7) args(6).toInt else 100000
//    val numOfCols = if (args.length >= 8) args(7).toInt else 10000
//    val dimOfCols = if (args.length >= 9) args(8).toInt else 10000
//    val numOfSlice = if (args.length >= 10) args(9).toInt else 10
//    val batchSize = if (args.length >= 11) args(10).toInt else 100
//
//    sc.parallelize((0 until numOfRows), numOfSlice).foreachPartition { partition =>
//
//      GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, "none")
//
//      partition.grouped(batchSize).foreach { rows =>
//        for {
//          rowId <- rows
//        } {
//          val ts = System.currentTimeMillis()
//          val msgs = for {
//            colId <- (0 until numOfCols)
//            metaId = Random.nextInt(dimOfCols)
//          } yield {
//              List(ts, "insertBulk", "edge", rowId, colId, labelName, s"""{"$metaName": $metaId}""").mkString("\t")
//            }
//          GraphSubscriberHelper.storeBulk(zkQuorum, hTableName)(msgs)(None)
//        }
//      }
//    }
//  }
//}

