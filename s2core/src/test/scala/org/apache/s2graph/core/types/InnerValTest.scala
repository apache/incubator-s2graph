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

package org.apache.s2graph.core.types

import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{FunSuite, Matchers}

import org.apache.s2graph.core.TestCommonWithModels
import org.apache.s2graph.core.utils.Logger

class InnerValTest extends FunSuite with Matchers with TestCommonWithModels {
  initTests()

  import HBaseType.VERSION2

  val decimals = List(BigDecimal(Long.MinValue),
    BigDecimal(Int.MinValue),
    BigDecimal(Double.MinValue),
    BigDecimal(Float.MinValue),
    BigDecimal(Short.MinValue),
    BigDecimal(Byte.MinValue),
    BigDecimal(-1),
    BigDecimal(0),
    BigDecimal(1),
    BigDecimal(Long.MaxValue),
    BigDecimal(Int.MaxValue),
    BigDecimal(Double.MaxValue),
    BigDecimal(Float.MaxValue),
    BigDecimal(Short.MaxValue),
    BigDecimal(Byte.MaxValue))
  val booleans = List(false, true)
  val strings = List("abc", "abd", "ac", "aca")
  val texts = List((0 until 1000).map(x => "a").mkString)
  val blobs = List((0 until 1000).map(x => Byte.MaxValue).toArray)

  def testEncodeDecode(ranges: List[InnerValLike], version: String): Unit =
    for {
      innerVal <- ranges
    } {
      val bytes = innerVal.bytes
      val (decoded, numOfBytesUsed) = InnerVal.fromBytes(bytes, 0, bytes.length, version)
      innerVal == decoded shouldBe true
      bytes.length == numOfBytesUsed shouldBe true
    }

  test("korean") {
    val small = InnerVal.withStr("가", VERSION2)
    val large = InnerVal.withStr("나", VERSION2)
    val smallBytes = small.bytes
    val largeBytes = large.bytes

    Logger.info(Bytes.compareTo(smallBytes, largeBytes))
    true
  }
}
