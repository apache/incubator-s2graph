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

package org.apache.s2graph.core

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.types.LabelWithDirection
import org.scalatest.{FunSuite, Matchers}

class QueryParamTest extends FunSuite with Matchers with TestCommon {
//  val version = HBaseType.VERSION2
//  val testEdge = Management.toEdge(ts, "insert", "1", "10", labelNameV2, "out", Json.obj("is_blocked" -> true, "phone_number" -> "xxxx", "age" -> 20).toString)
//  test("EdgeTransformer toInnerValOpt") {
//
//    /** only labelNameV2 has string type output */
//    val jsVal = Json.arr(Json.arr("_to"), Json.arr("phone_number.$", "phone_number"), Json.arr("age.$", "age"))
//    val transformer = EdgeTransformer(queryParamV2, jsVal)
//    val convertedLs = transformer.transform(testEdge, None)
//
//    convertedLs(0).tgtVertex.innerId.toString == "10" shouldBe true
//    convertedLs(1).tgtVertex.innerId.toString == "phone_number.xxxx" shouldBe true
//    convertedLs(2).tgtVertex.innerId.toString == "age.20" shouldBe true
//    true
//  }

  val dummyRequests = {
    for {
      id <- 0 until 1000
    } yield {
      Bytes.toBytes(id)
    }
  }

  test("QueryParam toCacheKey bytes") {
    val startedAt = System.nanoTime()
    val queryParam = QueryParam(LabelWithDirection(1, 0))

    for {
      i <- dummyRequests.indices
      x = queryParam.toCacheKey(dummyRequests(i))
    } {
      for {
        j <- dummyRequests.indices if i != j
        y = queryParam.toCacheKey(dummyRequests(j))
      } {
        x should not equal y
      }
    }

    dummyRequests.zip(dummyRequests).foreach { case (x, y) =>
      val xHash = queryParam.toCacheKey(x)
      val yHash = queryParam.toCacheKey(y)
//      println(xHash, yHash)
      xHash should be(yHash)
    }
    val duration = System.nanoTime() - startedAt

    println(s">> bytes: $duration")
  }

  test("QueryParam toCacheKey with variable params") {
    val startedAt = System.nanoTime()
    val queryParam = QueryParam(LabelWithDirection(1, 0))

    dummyRequests.zip(dummyRequests).foreach { case (x, y) =>
      x shouldBe y
      queryParam.limit(0, 10)
      var xHash = queryParam.toCacheKey(x)
      xHash shouldBe queryParam.toCacheKey(y)
      queryParam.limit(1, 10)
      var yHash = queryParam.toCacheKey(y)
      queryParam.toCacheKey(x) shouldBe yHash
//      println(xHash, yHash)
      xHash should not be yHash

      queryParam.limit(0, 10)
      xHash = queryParam.toCacheKey(x)
      queryParam.limit(0, 11)
      yHash = queryParam.toCacheKey(y)

      xHash should not be yHash
    }

    val duration = System.nanoTime() - startedAt

    println(s">> diff: $duration")
  }

}
