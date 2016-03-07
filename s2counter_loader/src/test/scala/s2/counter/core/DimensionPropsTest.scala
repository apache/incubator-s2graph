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

package s2.counter.core

import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ListBuffer

/**
 * Created by hsleep(honeysleep@gmail.com) on 2015. 10. 7..
 */
class DimensionPropsTest extends FunSuite with Matchers {
  test("makeRequestBody with Seq") {
    val requestBody =
      """
        |{
        |  "_from" => [[_from]]
        |}
      """.stripMargin
    val requestBodyExpected =
      """
        |{
        |  "_from" => 1
        |}
      """.stripMargin
    val requestBodyResult = DimensionProps.makeRequestBody(requestBody, Seq(("[[_from]]", "1")).toList)

    requestBodyResult shouldEqual requestBodyExpected
  }

  test("makeRequestBody with ListBuffer") {
    val requestBody =
      """
        |{
        |  "_from" => [[_from]]
        |}
      """.stripMargin
    val requestBodyExpected =
      """
        |{
        |  "_from" => 1
        |}
      """.stripMargin
    val requestBodyResult = DimensionProps.makeRequestBody(requestBody, ListBuffer(("[[_from]]", "1")).toList)

    requestBodyResult shouldEqual requestBodyExpected
  }
}
