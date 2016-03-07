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

//package com.kakao.s2graph.core.storage.hbase
//
//import com.kakao.s2graph.core.Graph
//import com.typesafe.config.ConfigFactory
//
//import org.apache.hadoop.hbase.util.Bytes
//import org.hbase.async.GetRequest
//import org.scalatest.{FunSuite, Matchers}
//
//import scala.concurrent.ExecutionContext
//
//class AsynchbaseQueryBuilderTest extends FunSuite with Matchers {
//
//  val dummyRequests = {
//    for {
//      id <- 0 until 1000
//    } yield {
//      new GetRequest("a", Bytes.toBytes(id))
//    }
//  }
//
//  implicit val ec = ExecutionContext.Implicits.global
//  val config = ConfigFactory.load()
//  val graph = new Graph(config)
//
//  val qb = new AsynchbaseQueryBuilder(graph.storage.asInstanceOf[AsynchbaseStorage])
//
//  test("test toCacheKeyBytes") {
//    val startedAt = System.nanoTime()
//
//    for {
//      i <- dummyRequests.indices
//      x = qb.toCacheKeyBytes(dummyRequests(i))
//    } {
//      for {
//        j <- dummyRequests.indices if i != j
//        y = qb.toCacheKeyBytes(dummyRequests(j))
//      } {
//        x should not equal y
//      }
//    }
//
//    dummyRequests.zip(dummyRequests).foreach { case (x, y) =>
//      val xHash = qb.toCacheKeyBytes(x)
//      val yHash = qb.toCacheKeyBytes(y)
//      //      println(xHash, yHash)
//      xHash should be(yHash)
//    }
//    val duration = System.nanoTime() - startedAt
//
//    println(s">> bytes: $duration")
//  }
//}
